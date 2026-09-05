"""Build a new index, drain the outbox, swap the alias.

The watermark is pg_snapshot_xmin(pg_current_snapshot()). Sequence values
are allocated at statement time and are not consumed in commit order, so a
row can become visible with a seq below one already observed; xmin is the
oldest still-running xid and covers everything in flight.
"""
from __future__ import annotations

import argparse
import time

from .common import (
    ALIAS, PROJECTION_PREDICATE, BulkStats, bulk_send, collect_pg_stats, es,
    current_alias_indices, full_scan_load, machine_specs, pg, point_alias,
    recreate_index,
    reset_pg_stats, row_to_doc, save_result, es_topology, index_shard_state,
)
from .reconcile import reconcile
from .workload import Workload


def outbox_lag(conn, xmin: int, rebuild_id: str) -> int:
    """Outbox entries in the replay window not yet applied."""
    return conn.execute(
        """
        SELECT count(*) FROM reindex_outbox o
          LEFT JOIN reindex_progress p
            ON p.seq = o.seq AND p.rebuild_id = %s
         WHERE o.xact_id >= %s AND p.seq IS NULL
        """,
        (rebuild_id, xmin),
    ).fetchone()[0]


def drain_outbox(conn, client, index, xmin, batch_size, stats, rebuild_id) -> dict:
    """Replay unapplied outbox entries from transactions at or after xmin.

    Anti-joins reindex_progress rather than paginating on seq: a high-water
    cursor can be overtaken by a transaction that commits out of sequence
    order. There is nothing here to outrun, and the applied set is durable.

    Idempotent - it re-reads current row state, so replaying twice converges.
    """
    rows = conn.execute(
        """
        SELECT o.seq, o.content_id FROM reindex_outbox o
          LEFT JOIN reindex_progress p
            ON p.seq = o.seq AND p.rebuild_id = %s
         WHERE o.xact_id >= %s AND p.seq IS NULL
         ORDER BY o.seq
        """,
        (rebuild_id, xmin),
    ).fetchall()
    if not rows:
        return {"ids": 0, "upserts": 0, "deletes": 0, "seqs": 0, "unresolved": 0}

    ids = list(dict.fromkeys(r[1] for r in rows))

    upserts = deletes = 0
    unresolved: set = set()
    for i in range(0, len(ids), batch_size):
        chunk = ids[i:i + batch_size]
        live = {
            r[0]: r
            for r in conn.execute(
                f"SELECT id, title, body, status, version, updated_at "
                f"FROM searchable_content "
                f"WHERE id = ANY(%s) AND {PROJECTION_PREDICATE}",
                (chunk,),
            ).fetchall()
        }
        actions = []
        for cid in chunk:
            row = live.get(cid)
            if row is None:
                # Deleted outright, or no longer matches the predicate.
                actions.append({"op": "delete", "id": cid, "doc": None})
                deletes += 1
            else:
                actions.append({"op": "index", "id": cid, "doc": row_to_doc(row)})
                upserts += 1
        unresolved |= bulk_send(client, index, actions, stats)

    # Only seqs whose document landed: the anti-join will not return them
    # again, so an unresolved seq must stay unmarked.
    applied = [s for s, cid in rows if cid not in unresolved]
    if applied:
        conn.execute(
            "INSERT INTO reindex_progress (rebuild_id, seq) "
            "SELECT %s, unnest(%s::bigint[]) ON CONFLICT DO NOTHING",
            (rebuild_id, applied),
        )
    return {"ids": len(ids), "upserts": upserts, "deletes": deletes,
            "seqs": len(applied), "unresolved": len(unresolved),
            "unresolved_ids": sorted(unresolved)}


def run(batch_size: int, replicas: int, refresh: str, window: float) -> dict:
    client = es()
    stats = BulkStats()
    index = f"corrected_b{batch_size}_r{replicas}_{'neg1' if refresh == '-1' else refresh}"

    rebuild_id = f"{index}-{int(time.time())}"

    with pg() as conn:
        max_id = conn.execute("SELECT max(id) FROM searchable_content").fetchone()[0]
        reset_pg_stats(conn)
        recreate_index(client, index, replicas, refresh)

        # Same ordering as the naive run, so the two face identical conditions.
        workload = Workload(run_label=index, max_id=max_id, window_seconds=window)
        workload.start()
        held = workload.wait_for_held_transaction(timeout=30)
        if held < workload.workers:
            # The commit-skew demonstration depends on transactions being open
            # when the watermark is read. A silent shortfall would change the
            # experiment without changing how the run reports.
            print(f"    warning: only {held}/{workload.workers} writers were "
                  f"mid-transaction at watermark time")

        xmin = int(
            conn.execute(
                "SELECT pg_snapshot_xmin(pg_current_snapshot())::text::bigint"
            ).fetchone()[0]
        )
        print(f"==> corrected watermark (snapshot xmin): {xmin} "
              f"({held} writes mid-transaction)")

        t0 = time.perf_counter()
        loaded, load_unresolved = full_scan_load(client, index, batch_size, stats)
        load_seconds = time.perf_counter() - t0
        # Before the settings restore below, which raises replicas to 1.
        load_shards = index_shard_state(client, index, replicas)
        print(f"==> scan loaded {loaded:,} docs in {load_seconds:.1f}s "
              f"(shards during load: {load_shards})")

        # Drain once while writes are still arriving, to measure the lag a
        # continuously-running drain would leave.
        t0 = time.perf_counter()
        live_pass = drain_outbox(conn, client, index, xmin, batch_size, stats, rebuild_id)
        live_drain_seconds = time.perf_counter() - t0
        lag_before_barrier = outbox_lag(conn, xmin, rebuild_id)
        print(f"==> live drain applied {live_pass['seqs']:,} entries; "
              f"lag still {lag_before_barrier:,}")

        # Write barrier. A live rebuild converges to a bounded lag, never to
        # zero - new commits keep arriving. Cutover needs either this or a
        # dual-write window; the drill uses the barrier and reports its cost.
        # Let the workload issue its planned writes first, so every config
        # commits the same number, then time the barrier itself.
        workload.wait_for_plan()
        barrier_seconds = workload.barrier()
        workload.stats["peak_concurrent_holds"] = workload.peak_concurrent_holds
        workload.stats["plan_completed"] = workload.plan_completed()
        print(f"==> write barrier: in-flight cleared in {barrier_seconds * 1000:.0f}ms")
        print(f"==> workload: {workload.stats}")

        t0 = time.perf_counter()
        passes = [live_pass] if live_pass["ids"] else []
        stalled = False
        for _ in range(20):
            p = drain_outbox(conn, client, index, xmin, batch_size, stats, rebuild_id)
            if p["ids"] == 0:
                break
            passes.append(p)
            shown = {k: v for k, v in p.items() if k != "unresolved_ids"}
            print(f"    drain pass: {shown}")
            if p["seqs"] == 0:
                # Nothing applied: every row in this pass failed permanently.
                # The anti-join returns them again forever, so retrying is a
                # spin, not progress.
                stalled = True
                print(f"    drain stalled - {p['unresolved']} entries will not apply")
                break
        replay_seconds = time.perf_counter() - t0
        total_ids = sum(p["ids"] for p in passes)
        lag_after_drain = outbox_lag(conn, xmin, rebuild_id)
        print(f"==> drained to lag {lag_after_drain}")
        print(f"==> drained {total_ids:,} ids in {len(passes)} pass(es), {replay_seconds:.1f}s")

        client.indices.put_settings(
            index=index, settings={"number_of_replicas": 1, "refresh_interval": "1s"}
        )
        # Raising replicas allocates asynchronously. Going live before the
        # shards are actually assigned makes the new index serve from a
        # partially-allocated state.
        t0 = time.perf_counter()
        health = client.cluster.health(index=index, wait_for_status="green", timeout="120s")
        client.indices.refresh(index=index)
        settle_seconds = time.perf_counter() - t0
        ready_shards = index_shard_state(client, index, 1)
        # wait_for_status returns normally on timeout with timed_out: true.
        # Not checking it means cutting over to a half-allocated index.
        shards_ready = not health.get("timed_out", False) and health["status"] == "green"
        print(f"==> settings restored, shards {ready_shards['status']} in "
              f"{settle_seconds:.1f}s (ready={shards_ready})")

        # Verification is the cutover gate, so it runs BEFORE the alias moves.
        t0 = time.perf_counter()
        checks = reconcile(conn, client, index)
        verify_seconds = time.perf_counter() - t0

        unresolved_ids = set(load_unresolved)
        for p in passes:
            unresolved_ids |= set(p.get("unresolved_ids", ()))
        unresolved_total = len(unresolved_ids)
        remaining_lag = lag_after_drain
        gate_ok = (checks["clean"] and shards_ready and not stalled
                   and unresolved_total == 0 and remaining_lag == 0)
        print(f"==> verified in {verify_seconds:.1f}s: clean={checks['clean']} "
              f"shards_ready={shards_ready} unresolved={unresolved_total} "
              f"lag={remaining_lag} stalled={stalled} -> gate={gate_ok}")

        if not gate_ok:
            print("==> GATE FAILED - not swapping the alias")

        old = [i for i in current_alias_indices(client, ALIAS) if i != index]
        swap_seconds = None
        if gate_ok:
            # Dual-write a canary to both indices, then time the swap.
            canary = {"op": "index", "id": max_id, "doc": None}
            row = conn.execute(
                f"SELECT id, title, body, status, version, updated_at FROM searchable_content "
                f"WHERE id = %s AND {PROJECTION_PREDICATE}", (max_id,)
            ).fetchone()
            canary_failed = False
            if row:
                canary["doc"] = row_to_doc(row)
                for target in old + [index]:
                    if bulk_send(client, target, [canary], stats):
                        print(f"    canary write to {target} did not land")
                        # Only the incoming index is disqualifying; the
                        # outgoing one is about to be retired.
                        canary_failed = canary_failed or target == index
            if canary_failed:
                gate_ok = False
                print("==> CANARY FAILED on the target index - not swapping")
            else:
                swap_seconds = point_alias(client, ALIAS, index)
                print(f"==> alias swap took {swap_seconds * 1000:.1f}ms")

        # live_drain_seconds does the majority of the replay work and used to
        # sit in no bucket at all, which understated both totals.
        rebuild_seconds = load_seconds + live_drain_seconds + replay_seconds
        cutover_ready_seconds = (
            load_seconds + live_drain_seconds + barrier_seconds + replay_seconds
            + settle_seconds + verify_seconds
        )
        pg_stats = collect_pg_stats(conn)

    result = {
        "recovery": "corrected",
        "batch_size": batch_size,
        "replicas": replicas,
        "refresh": refresh,
        "refresh_label": "neg1" if refresh == "-1" else refresh,
        "index": index,
        "watermark_xmin": xmin,
        "docs_loaded": loaded,
        "replay_rows": total_ids,
        "drain_passes": passes,
        "load_seconds": round(load_seconds, 2),
        "live_drain_seconds": round(live_drain_seconds, 2),
        "barrier_seconds": round(barrier_seconds, 4),
        "replay_seconds": round(replay_seconds, 2),
        "settle_seconds": round(settle_seconds, 2),
        "verify_seconds": round(verify_seconds, 2),
        # rebuild = scan + replay, comparable with the naive run.
        # cutover_ready = everything needed before the alias can safely move.
        "rebuild_seconds": round(rebuild_seconds, 2),
        "cutover_ready_seconds": round(cutover_ready_seconds, 2),
        "wall_clock_seconds": round(rebuild_seconds, 2),
        "docs_per_sec": round(loaded / load_seconds, 1) if load_seconds else None,
        "alias_swap_ms": (round(swap_seconds * 1000, 2)
                          if swap_seconds is not None else None),
        "rebuild_id": rebuild_id,
        "lag_before_barrier": lag_before_barrier,
        "lag_after_drain": lag_after_drain,
        "unresolved_writes": unresolved_total,
        "drain_stalled": stalled,
        "shards_ready_at_cutover": shards_ready,
        "gate_passed": gate_ok,
        "shards_at_cutover": ready_shards,
        "bulk": vars(stats),
        "workload": workload.stats,
        "held_at_watermark": held,
        "workers": workload.workers,
        "pg_stat_statements": pg_stats,
        "checks": checks,
        "machine": machine_specs(),
        "es_topology": es_topology(client),
        "index_shards_during_load": load_shards,
    }
    # After the dict is built, since es_topology() above needs the client.
    client.close()
    return result


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch-size", type=int, default=2000)
    ap.add_argument("--replicas", type=int, default=0)
    ap.add_argument("--refresh", default="-1")
    ap.add_argument("--window", type=float, default=60.0)
    a = ap.parse_args()
    result = run(a.batch_size, a.replicas, a.refresh, a.window)
    print(f"\nsaved -> {save_result(result)}")
    c = result["checks"]
    print(f"missing={c['missing']['count']} resurrected={c['resurrected']['count']} "
          f"stale={c['stale']['count']}")
