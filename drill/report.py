"""Run the matrix, force bulk rejections, print the table.

  --all                 seed + every config + table
  --probe-rejections    saturate the write pool
  (no args)             re-render from results/
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import json
import random
import threading
import time

from .common import (
    BulkStats, ROW_COUNT, RESULTS_DIR, bulk_send, es, index_body, machine_specs,
    random_body, random_title, save_result,
)
from .workload import live_writer_count, stop_all_live

# (recovery, batch_size, replicas, refresh)
#
# The last four are a 2x2 on replicas x refresh. Changing both at once only
# tells you what "production settings" cost in total; separating them tells
# you which of the two you are paying for.
RUN_MATRIX = [
    ("naive", 2000, 0, "-1"),
    ("corrected", 1000, 0, "-1"),
    ("corrected", 2000, 0, "-1"),
    ("corrected", 5000, 0, "-1"),
    ("corrected", 2000, 0, "1s"),
    ("corrected", 2000, 1, "-1"),
    ("corrected", 2000, 1, "1s"),
]
REPEATS = 5


def probe_rejections(concurrency: int = 120, batch: int = 64, rounds: int = 10) -> dict:
    """Saturate the write pool until ES rejects for real.

    Getting the 200-with-errors shape (rather than a 429) needs all four of:
    connections_per_node above the client default of 10; a barrier so the
    requests land together; small requests, since a 3k-doc bulk drains as fast
    as it fills; and a multi-shard index, so some sub-requests can succeed
    while others are refused inside one response.
    """
    client = es(connections_per_node=max(64, concurrency))
    index = "reject_probe"
    if client.indices.exists(index=index):
        client.indices.delete(index=index)
    body = index_body(0, "-1")
    body["settings"]["number_of_shards"] = 8
    client.indices.create(index=index, settings=body["settings"], mappings=body["mappings"])
    rng = random.Random(7)
    docs = [
        {
            "title": random_title(rng),
            "body": random_body(rng),
            "status": "PUBLISHED",
            "version": 1,
            "updated_at": "2026-01-01T00:00:00+00:00",
        }
        for _ in range(batch)
    ]

    def one_request(n: int, barrier: threading.Barrier) -> dict:
        actions = [
            {"op": "index", "id": n * batch + i, "doc": {**docs[i], "id": n * batch + i}}
            for i in range(batch)
        ]
        s = BulkStats()
        try:
            barrier.wait(timeout=30)
        except threading.BrokenBarrierError:
            pass
        # max_attempts=1: raw rejections, not what retries paper over.
        bulk_send(client, index, actions, s, max_attempts=1)
        return vars(s)

    total = BulkStats()
    for r in range(rounds):
        barrier = threading.Barrier(concurrency)
        with cf.ThreadPoolExecutor(max_workers=concurrency) as pool:
            futures = [
                pool.submit(one_request, r * concurrency + i, barrier)
                for i in range(concurrency)
            ]
            for fut in cf.as_completed(futures):
                total.merge(BulkStats(**fut.result()))
        print(f"  round {r + 1}: rejected={total.items_rejected} "
              f"200-with-errors={total.http_200_with_errors} "
              f"429s={total.http_429_whole_request} types={total.error_types}")
        if total.http_200_with_errors > 0:
            break

    # items_rejected mixes two shapes: whole-request 429s add len(pending),
    # per-item rejections add 1 each. Split them before writing the artifact.
    # Every RETRYABLE type is counted per item, not just the rejection one -
    # otherwise a circuit-breaker rejection is silently reclassified as an
    # HTTP 429 whole-request refusal.
    from .common import RETRYABLE
    per_item = sum(total.error_types.get(k, 0) for k in RETRYABLE)
    whole_req = total.items_rejected - per_item

    try:
        pools = client.nodes.info(metric="thread_pool")["nodes"]
        queue_sizes = sorted({n["thread_pool"]["write"]["queue_size"] for n in pools.values()})
    except Exception:  # noqa: BLE001 - informational
        queue_sizes = None

    out = {
        "concurrency": concurrency,
        "batch": batch,
        "write_queue_size": queue_sizes,
        "rounds_run": r + 1,
        "items_rejected": total.items_rejected,
        "items_rejected_per_item": per_item,
        "items_rejected_whole_request": whole_req,
        "http_200_with_errors": total.http_200_with_errors,
        "http_429_whole_request": total.http_429_whole_request,
        "shards": 8,
        "error_types": total.error_types,
        "requests": total.requests,
        "items_sent": total.items_sent,
        "real_rejections_observed": total.items_rejected > 0,
        "machine": machine_specs(),
    }
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    (RESULTS_DIR / "rejection_probe.json").write_text(json.dumps(out, indent=2))

    print()
    if out["real_rejections_observed"]:
        print(f"  OBSERVED {total.items_rejected} rejected items, in two shapes:")
        print(f"    {whole_req} in {total.http_429_whole_request} whole requests "
              f"refused with HTTP 429")
        print(f"    {per_item} inside {total.http_200_with_errors} responses that "
              f"were HTTP 200 with errors=true")
    else:
        print("  No rejections. Restart ES with --small-queue:")
        print("    ./podman-setup.sh --small-queue")
    return out


def _clear_drill_indices(client) -> None:
    """Drop every index this drill created.

    Without this, configurations run against a progressively fuller cluster
    and later ones look slower for reasons unrelated to their settings. That
    confound is what made the first replica/refresh comparison unusable.
    """
    for pattern in ("naive_*", "corrected_*", "reject_probe"):
        try:
            client.indices.delete(index=pattern, ignore_unavailable=True)
        except Exception:  # noqa: BLE001 - best-effort cleanup
            pass


def run_all(window: float, rows: int, repeats: int = REPEATS) -> None:
    from . import corrected as corrected_mod
    from . import naive as naive_mod
    from . import seed as seed_mod

    client = es()
    # Interleave configurations instead of running all repeats of one
    # back-to-back, so any drift over the session does not line up with a
    # single configuration. Fixed seed so the order is reproducible.
    plan = [(cfg, rep) for rep in range(1, repeats + 1) for cfg in RUN_MATRIX]
    random.Random(4242).shuffle(plan)
    failures: list[dict] = []

    for i, ((recovery, batch, replicas, refresh), rep) in enumerate(plan, 1):
        print()
        print("#" * 72)
        print(f"# [{i}/{len(plan)}] {recovery} batch={batch} replicas={replicas} "
              f"refresh={refresh}  run {rep}/{repeats}")
        print("#" * 72)
        _clear_drill_indices(client)
        # Full reseed: each run deletes 250 rows and inserts 120, so without
        # this the runs stop being comparable.
        seed_mod.seed(rows)
        mod = naive_mod if recovery == "naive" else corrected_mod
        try:
            result = mod.run(batch, replicas, refresh, window)
        except Exception as exc:  # noqa: BLE001
            # Recorded, not swallowed: a missing repeat shows up in the
            # per-cell run count.
            failures.append({"config": (recovery, batch, replicas, refresh),
                             "repeat": rep, "error": repr(exc)})
            print(f"!!! run failed: {exc!r}")
            # Stop this run's writers before reseeding for the next one.
            orphaned = stop_all_live()
            if orphaned:
                print(f"    stopped {orphaned} orphaned writer(s)")
            if live_writer_count():
                raise RuntimeError(
                    "writers survived cleanup; aborting rather than reseeding "
                    "under a live writer"
                ) from exc
            continue
        result["repeat"] = rep
        result["run_order"] = i
        print(f"saved -> {save_result(result)}")

    if failures:
        print(f"\n!!! {len(failures)} run(s) failed and are absent from results/:")
        for f in failures:
            print(f"    {f['config']} run {f['repeat']}: {f['error']}")

    print()
    render_table()


def _load_results() -> list[dict]:
    out = []
    for p in sorted(RESULTS_DIR.glob("*.json")):
        if p.name == "rejection_probe.json":
            continue
        try:
            out.append(json.loads(p.read_text()))
        except json.JSONDecodeError:
            print(f"  (skipping unreadable {p.name})")
    order = {(r, b, rp, rf): i for i, (r, b, rp, rf) in enumerate(RUN_MATRIX)}
    return sorted(
        out,
        key=lambda d: order.get(
            (d["recovery"], d["batch_size"], d["replicas"], d["refresh"]), 99
        ),
    )


def render_table() -> None:
    results = _load_results()
    if not results:
        print("No results in results/. Run:  python3 -m drill.report --all")
        return

    groups: dict[tuple, list[dict]] = {}
    for d in results:
        groups.setdefault(
            (d["recovery"], d["batch_size"], d["replicas"], d["refresh"]), []
        ).append(d)

    def med(vals):
        # docs_per_sec is None when a load somehow measured zero seconds;
        # sorting a list with None in it raises rather than skewing quietly.
        s = sorted(v for v in vals if v is not None)
        if not s:
            return 0
        n = len(s)
        return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2

    print("| Recovery | Batch | Replicas/refresh | Runs | Docs/sec (median) | "
          "Range | Rebuild s | Cutover-ready s | Missing | Resurrected | "
          "Predicate-excluded | Stale (sampled) | Stale (exact) |")
    print("|---|---:|---|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|")
    order = {k: i for i, k in enumerate(RUN_MATRIX)}
    for key in sorted(groups, key=lambda k: order.get(k, 99)):
        runs = groups[key]
        # Filtered here too: med() drops None but bare min/max would raise.
        dps = [d["docs_per_sec"] for d in runs if d.get("docs_per_sec") is not None] or [0]
        cs = [d["checks"] for d in runs]
        extras = [c.get("extra", {}) for c in cs]
        cutover = [d.get("cutover_ready_seconds") for d in runs
                   if d.get("cutover_ready_seconds") is not None]
        rec, batch, repl, refr = key
        # naive has no cutover gate, so it has no cutover-ready time.
        cutover_cell = f"{med(cutover):.1f}" if cutover else "-"
        print(
            f"| {rec.capitalize()} | {batch} | {repl} / {refr} | {len(runs)} | "
            f"{med(dps):,.0f} | {min(dps):,.0f}-{max(dps):,.0f} | "
            f"{med([d['rebuild_seconds'] for d in runs]):.1f} | "
            f"{cutover_cell} | ",
            end="")
        print(
            f"{med([c['missing']['count'] for c in cs]):.0f} | "
            f"{med([c['resurrected']['count'] for c in cs]):.0f} | "
            f"{med([e.get('predicate_excluded', 0) for e in extras]):.0f} | "
            f"{med([c['stale']['count'] for c in cs]):.0f}/"
            f"{med([c['stale']['sampled'] for c in cs]):.0f} | "
            f"{med([c['stale_exact']['count'] for c in cs]):.0f} |"
        )

    print()
    print("Defect counts across repeats (all runs, not median)")
    print("---------------------------------------------------")
    for key in sorted(groups, key=lambda k: order.get(k, 99)):
        runs = sorted(groups[key], key=lambda d: d.get("repeat", 0))
        rec, batch, repl, refr = key
        cells = []
        for d in runs:
            c = d["checks"]
            cells.append(f"{c['missing']['count']}/{c['resurrected']['count']}"
                         f"/{c['stale_exact']['count']}")
        print(f"  {rec:>9} b{batch:<5} {repl}/{refr:<3} "
              f"missing/resurrected/stale: {'  '.join(cells)}")

    print()
    print("Timing breakdown (median seconds)")
    print("---------------------------------")
    for key in sorted(groups, key=lambda k: order.get(k, 99)):
        runs = groups[key]
        rec, batch, repl, refr = key
        parts = []
        for f in ("load_seconds", "barrier_seconds", "replay_seconds",
                  "settle_seconds", "verify_seconds"):
            vals = [d[f] for d in runs if d.get(f) is not None]
            if vals:
                parts.append(f"{f.replace('_seconds','')}={med(vals):.1f}")
        swaps = [d["alias_swap_ms"] for d in runs if d.get("alias_swap_ms")]
        if swaps:
            parts.append(f"alias_swap={med(swaps):.1f}ms")
        lags = [d.get("lag_before_barrier") for d in runs
                if d.get("lag_before_barrier") is not None]
        if lags:
            parts.append(f"lag_at_barrier={med(lags):.0f}")
        print(f"  {rec:>9} b{batch:<5} {repl}/{refr:<3} {'  '.join(parts)}")

    m = results[0].get("machine", {})
    print()
    print("Machine")
    print("-------")
    print(f"  cpu      {m.get('cpu', 'unknown')} ({m.get('logical_cpus')} logical)")
    ram = m.get("ram_bytes")
    print(f"  ram      {ram / 1e9:.1f} GB" if ram else "  ram      unknown")
    print(f"  platform {m.get('platform')}")
    topo = results[0].get("es_topology", {})
    if topo:
        print(f"  es       {topo.get('version')}, {topo.get('nodes')} nodes, "
              f"heap {topo.get('heap_max_mb_per_node')} MB")

    probe = RESULTS_DIR / "rejection_probe.json"
    if probe.exists():
        pr = json.loads(probe.read_text())
        print()
        print("Bulk rejection probe")
        print("--------------------")
        print(f"  concurrency {pr['concurrency']} x {pr['batch']} items, {pr['requests']} requests")
        print(f"  items rejected            {pr['items_rejected']}")
        print(f"  HTTP 200 with errors=true {pr['http_200_with_errors']}")
        print(f"  error types               {pr['error_types']}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--all", action="store_true", help="seed then run the full matrix")
    ap.add_argument("--probe-rejections", action="store_true")
    ap.add_argument("--window", type=float, default=60.0, help="workload window seconds")
    ap.add_argument("--rows", type=int, default=ROW_COUNT)
    a = ap.parse_args()

    if a.probe_rejections:
        probe_rejections()
    elif a.all:
        t0 = time.perf_counter()
        run_all(a.window, a.rows)
        print(f"\ntotal elapsed {(time.perf_counter() - t0) / 60:.1f} min")
    else:
        render_table()
