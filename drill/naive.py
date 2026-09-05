"""Truncate-and-reload, watermarked on a clock reading.

Loses rows three ways: commit-time skew (updated_at is stamped before
the commit is visible), deletes leave nothing to scan, and status
changes out of the predicate are never removed.
"""
from __future__ import annotations

import argparse
import time

from .common import (
    ALIAS, PROJECTION_PREDICATE, BulkStats, bulk_send, collect_pg_stats, es,
    full_scan_load, machine_specs, pg, point_alias, recreate_index,
    reset_pg_stats, row_to_doc, save_result, es_topology, index_shard_state,
)
from .reconcile import reconcile
from .workload import Workload


def run(batch_size: int, replicas: int, refresh: str, window: float) -> dict:
    client = es()
    stats = BulkStats()
    # Includes replicas/refresh so each configuration gets its own index.
    index = f"naive_b{batch_size}_r{replicas}_{'neg1' if refresh == '-1' else refresh}"

    with pg() as conn:
        max_id = conn.execute("SELECT max(id) FROM searchable_content").fetchone()[0]
        reset_pg_stats(conn)

        recreate_index(client, index, replicas, refresh)
        point_alias(client, ALIAS, index)

        # Writers first - traffic is already in flight when a reindex starts.
        workload = Workload(run_label=index, max_id=max_id, window_seconds=window)
        workload.start()
        held = workload.wait_for_held_transaction(timeout=30)
        if held < workload.workers:
            # The commit-skew demonstration depends on transactions being open
            # when the watermark is read. A silent shortfall would change the
            # experiment without changing how the run reports.
            print(f"    warning: only {held}/{workload.workers} writers were "
                  f"mid-transaction at watermark time")

        # The bug, in one line. A timestamp orders statements, not commits.
        watermark = conn.execute("SELECT clock_timestamp()").fetchone()[0]
        print(f"==> naive watermark (clock): {watermark.isoformat()} "
              f"({held} writes mid-transaction)")

        t0 = time.perf_counter()
        loaded, load_unresolved = full_scan_load(client, index, batch_size, stats)
        load_seconds = time.perf_counter() - t0
        # Before the settings restore below, which raises replicas to 1.
        load_shards = index_shard_state(client, index, replicas)
        print(f"==> scan loaded {loaded:,} docs in {load_seconds:.1f}s "
              f"(shards during load: {load_shards})")

        workload.wait_for_plan()
        workload.barrier()
        workload.stats["peak_concurrent_holds"] = workload.peak_concurrent_holds
        workload.stats["plan_completed"] = workload.plan_completed()
        print(f"==> workload: {workload.stats}")

        # Restore before reconciling; counts read stale until an explicit
        # refresh.
        client.indices.put_settings(
            index=index, settings={"number_of_replicas": 1, "refresh_interval": "1s"}
        )

        t0 = time.perf_counter()
        rows = conn.execute(
            f"SELECT id, title, body, status, version, updated_at "
            f"FROM searchable_content "
            f"WHERE updated_at > %s AND {PROJECTION_PREDICATE} ORDER BY id",
            (watermark,),
        ).fetchall()
        batch = [{"op": "index", "id": r[0], "doc": row_to_doc(r)} for r in rows]
        for i in range(0, len(batch), batch_size):
            load_unresolved |= bulk_send(client, index, batch[i:i + batch_size], stats)
        replay_seconds = time.perf_counter() - t0
        print(f"==> replayed {len(rows):,} rows by timestamp in {replay_seconds:.1f}s")

        client.indices.refresh(index=index)
        t0 = time.perf_counter()
        checks = reconcile(conn, client, index)
        verify_seconds = time.perf_counter() - t0
        # After reconcile, matching corrected.py: collecting before would omit
        # the verification queries and make the two arms incomparable.
        pg_stats = collect_pg_stats(conn)
        wall_clock = load_seconds + replay_seconds

    result = {
        "recovery": "naive",
        "batch_size": batch_size,
        "replicas": replicas,
        "refresh": refresh,
        "refresh_label": "neg1" if refresh == "-1" else refresh,
        "index": index,
        "watermark": watermark.isoformat(),
        "docs_loaded": loaded,
        "unresolved_writes": len(load_unresolved),
        "replay_rows": len(rows),
        "load_seconds": round(load_seconds, 2),
        "replay_seconds": round(replay_seconds, 2),
        "verify_seconds": round(verify_seconds, 2),
        "rebuild_seconds": round(wall_clock, 2),
        "wall_clock_seconds": round(wall_clock, 2),
        "docs_per_sec": round(loaded / load_seconds, 1) if load_seconds else None,
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
