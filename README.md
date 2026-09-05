# reindex-recovery-drill

Two ways of rebuilding an Elasticsearch index from Postgres while writes are
still arriving:

- **naive** — wipe the index, copy everything, replay `updated_at > watermark`
- **corrected** — build a new index, drain an outbox watermarked on
  `pg_snapshot_xmin()`, barrier writes, verify exactly, swap the alias

Under the synthetic workload below, the timestamp replay left 366 measurable
inconsistencies per run. The transaction-aware path passed every implemented
reconciliation gate, five runs out of five.

The corrected path is not a continuous-write rebuild. Reads and writes continue
through the expensive bulk phase, then writes pause while the drain finishes and
the index is verified. A drain running against live writes converges to a lag,
not to zero, so cutover needs either that barrier or a dual-write window.

## Run

```
./podman-setup.sh
python3 -m venv .venv
./.venv/bin/pip install 'elasticsearch>=8,<9' 'psycopg[binary]'
./.venv/bin/python -m drill.report --all
```

`./run-drill.sh` does the whole thing from cold, including the constrained
write queue needed to force real bulk rejections.

Tests for the failure paths — rejections, retries, and the cutover gate:

```
./.venv/bin/python -m drill.selftest          # needs a live cluster
./.venv/bin/python -m drill.selftest --fast   # mocked only
```

Watermark proofs, two sessions each:

```
./.venv/bin/python -m drill.commit_time_demo
```

No containers, no dependencies:

```
python3 reindex_drill.py
```

## Results

500k rows (449,917 matching the predicate), 2,000 updates / 250 deletes / 120
inserts committed concurrently across 8 connections. Postgres 16.15, two-node
Elasticsearch 8.19.3 at 1 GB heap, Apple M4 Max in a 7-CPU / 8 GiB Podman
machine.

Seven configurations, five runs each, one JSON per run. Medians:

| Recovery | Batch | Replicas/refresh | Docs/sec | Rebuild | Cutover-ready | Missing | Deleted, still indexed | Predicate-excluded | Stale (sampled) | Stale (exact) |
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Naive | 2000 | 0 / -1 | 34,304 | 13.2s | — | 3 | 228 | 131 | 0/5000 | 4 |
| Corrected | 1000 | 0 / -1 | 34,122 | 13.3s | 26.7s | 0 | 0 | 0 | 0/5000 | 0 |
| Corrected | 2000 | 0 / -1 | 34,106 | 13.3s | 27.0s | 0 | 0 | 0 | 0/5000 | 0 |
| Corrected | 5000 | 0 / -1 | 36,405 | 12.5s | 25.0s | 0 | 0 | 0 | 0/5000 | 0 |
| Corrected | 2000 | 0 / 1s | 36,978 | 12.3s | 25.7s | 0 | 0 | 0 | 0/5000 | 0 |
| Corrected | 2000 | 1 / -1 | 23,670 | 19.4s | 25.8s | 0 | 0 | 0 | 0/5000 | 0 |
| Corrected | 2000 | 1 / 1s | 25,961 | 17.5s | 24.1s | 0 | 0 | 0 | 0/5000 | 0 |

Every index is dropped and the data reseeded before each run, and the 35 runs
are interleaved rather than grouped by configuration, so session drift cannot
line up with one setting.

`Rebuild` is scan + live drain + replay. `Cutover-ready` adds the barrier,
final drain, settings restore, shard allocation and exact verification —
everything needed before the alias can safely move. It roughly doubles the
total.

Raw JSON per run is in `results/`.

Worth noting from the runs:

- The 5,000-ID sampled check found none of the 4 stale documents, on any of the
  five runs. At that defect density its detection probability is 4.4%; 95%
  detection needs 53% of the corpus. Segmented counts (6/6 mismatched) and the
  deleted-ID check (228/250 found) did fire.
- Replicas and refresh are separated into a 2×2, each variable held constant
  across a pair. Holding refresh constant, the replica costs 30–31% of
  throughput and yet is cheaper to cutover-ready both ways (25.7s→24.1s at
  refresh 1s, 27.0s→25.8s at refresh −1): with replicas 0 the scan is faster
  and then hands the time back as a ~7s shard copy. Holding replicas constant,
  disabling refresh was slower on both axes. Non-overlapping run ranges
  throughout.
- The finalization window is 6.4–13.7s, and it is shard allocation plus
  verification rather than the barrier. The barrier itself measures 0 ms, but
  the synthetic writer had finished its plan by then, so that is the cost of
  quiescing an idle writer rather than latency for blocked writes.
- Bulk rejections need real concurrency. A sequential loader never fills the
  write queue, even at `queue_size=10` — `--probe-rejections` uses 120
  concurrent bulks against an 8-shard index to get them.

Implementation notes:

- The drain anti-joins a `reindex_progress` table rather than carrying a
  `seq > last_applied` cursor, which a transaction committing out of sequence
  order can overtake. The same table is the durable applied position that
  makes lag monitoring meaningful.
- `bulk_send` returns the ids that never landed, and the drain marks only
  resolved seqs applied.
- Verification runs before the alias swap, and after `wait_for_status=green`
  since raising replicas allocates asynchronously.
- The exact check compares a length-prefixed digest over `id`, `title`,
  `body`, `status`, `version` (not `updated_at`), computed in SQL on the
  source and recomputed from the `_source` Elasticsearch returns.
- The cutover gate requires exact comparison clean, shards green, zero
  unresolved writes, zero remaining lag, and a drain that did not stall. If
  any fails, the alias does not move.

## Notes

- ES 8.15.0 dies with `SIGILL` in `System.registerNatives` on aarch64 under
  applehv (bundled JDK 22, crashes before any JVM flag applies). 8.19.3 works.
  Override with `DRILL_ES_IMAGE`.
- Two ES nodes because `replicas: 1` on a single node leaves the replica
  `UNASSIGNED`, which makes replication look free. Each result records
  `index_shards_during_load` so that's checkable.
- The workload plan is seeded, so operations and target IDs are identical run to
  run. Defect counts came out identical across repeats; throughput did not, and
  the range is in the table. Every run has its own JSON in `results/`.
- `figures/` holds the diagrams and the script that generates them.

- `results/` holds the 35-run matrix, produced by the code at tag
  `dzone-recovery-drill-v1`. All 30 corrected runs passed the cutover gate;
  the 5 naive runs are deliberately defective comparisons and do not use it.
