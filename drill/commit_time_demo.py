"""Two sessions showing that neither a timestamp nor MAX(seq) is a
valid watermark. Demo A: updated_at lands behind a watermark taken
while the transaction was open. Demo B: a seq commits below an
already-observed MAX(seq).
"""
from __future__ import annotations

import threading
import time

import psycopg

from .common import PG_DSN, pg


def _fresh_outbox(conn) -> None:
    conn.execute("TRUNCATE reindex_outbox RESTART IDENTITY")


def demo_a() -> bool:
    print("=" * 72)
    print("DEMO A  updated_at behind the watermark")
    print("=" * 72)

    with pg() as ctl:
        _fresh_outbox(ctl)
        rid = ctl.execute(
            "SELECT id FROM searchable_content WHERE status='PUBLISHED' ORDER BY id LIMIT 1"
        ).fetchone()[0]

        held = psycopg.connect(PG_DSN, autocommit=False)
        with held.cursor() as cur:
            cur.execute("BEGIN")
            cur.execute(
                "UPDATE searchable_content SET title = 'held-write', "
                "version = version + 1, updated_at = clock_timestamp() WHERE id = %s",
                (rid,),
            )
            row_updated_at = cur.execute(
                "SELECT updated_at FROM searchable_content WHERE id = %s", (rid,)
            ).fetchone()[0]
        print(f"  session A: UPDATE id={rid} ran, updated_at = {row_updated_at.isoformat()}")
        print("  session A: holding the transaction open (NOT committed)")

        time.sleep(0.2)
        watermark = ctl.execute("SELECT clock_timestamp()").fetchone()[0]
        xmin = int(ctl.execute("SELECT pg_snapshot_xmin(pg_current_snapshot())::text::bigint").fetchone()[0])
        visible_title = ctl.execute(
            "SELECT title FROM searchable_content WHERE id = %s", (rid,)
        ).fetchone()[0]
        print(f"  session B: naive watermark   = {watermark.isoformat()}")
        print(f"  session B: snapshot xmin     = {xmin}")
        print(f"  session B: sees title        = {visible_title!r}  <- old value, A hasn't committed")

        held.commit()
        held.close()  # this connection holds a write lock; release it promptly
        print("  session A: COMMIT")

        after = ctl.execute(
            "SELECT title, updated_at FROM searchable_content WHERE id = %s", (rid,)
        ).fetchone()
        ob = ctl.execute(
            "SELECT seq, xact_id FROM reindex_outbox WHERE content_id = %s "
            "ORDER BY seq DESC LIMIT 1", (rid,)
        ).fetchone()

        if ob is None:
            # Guarded because ob[0]/ob[1] are dereferenced below: an empty
            # outbox means the trigger is disabled, not that the demo passed.
            raise SystemExit("no outbox row for the held write - trigger disabled?")
        behind = after[1] <= watermark
        caught = ob[1] >= xmin

        print()
        print(f"  row updated_at   = {after[1].isoformat()}")
        print(f"  watermark        = {watermark.isoformat()}")
        print(f"  updated_at <= watermark ?           {behind}   "
              f"{'<-- NAIVE REPLAY MISSES THIS ROW' if behind else '(no skew this run)'}")
        print(f"  outbox seq={ob[0]} xact_id={ob[1]}")
        print(f"  outbox xact_id >= snapshot xmin ?   {caught}   "
              f"{'<-- CORRECTED REPLAY CATCHES IT' if caught else '<-- UNEXPECTED'}")
        print()
        print("  A rebuild replaying `updated_at > watermark` skips a row that")
        print("  committed after the watermark was taken. The row is also absent")
        print("  from the scan cursor's snapshot. It is simply lost.")
        return behind and caught


def demo_b() -> bool:
    print()
    print("=" * 72)
    print("DEMO B  MAX(seq) misses an out-of-order commit; xmin does not")
    print("=" * 72)

    with pg() as ctl:
        _fresh_outbox(ctl)
        ids = [r[0] for r in ctl.execute(
            "SELECT id FROM searchable_content WHERE status='PUBLISHED' ORDER BY id LIMIT 2"
        ).fetchall()]
        id_slow, id_fast = ids

        started = threading.Event()
        release = threading.Event()
        captured: dict = {}

        def slow_writer() -> None:
            with psycopg.connect(PG_DSN, autocommit=False) as c:
                with c.cursor() as cur:
                    cur.execute(
                        "UPDATE searchable_content SET title='slow-txn', version=version+1, "
                        "updated_at=clock_timestamp() WHERE id=%s", (id_slow,)
                    )
                    captured["slow_seq"] = cur.execute(
                        "SELECT max(seq) FROM reindex_outbox WHERE content_id=%s", (id_slow,)
                    ).fetchone()[0]
                started.set()
                release.wait(10)
                c.commit()

        t = threading.Thread(target=slow_writer, daemon=True)
        t.start()
        started.wait(10)
        print(f"  T1: UPDATE id={id_slow} ran, outbox seq={captured['slow_seq']} allocated, uncommitted")

        with pg() as fast:
            fast.execute(
                "UPDATE searchable_content SET title='fast-txn', version=version+1, "
                "updated_at=clock_timestamp() WHERE id=%s", (id_fast,)
            )
        fast_seq = ctl.execute(
            "SELECT max(seq) FROM reindex_outbox WHERE content_id=%s", (id_fast,)
        ).fetchone()[0]
        print(f"  T2: UPDATE id={id_fast} ran and COMMITTED, outbox seq={fast_seq}")

        max_seq = ctl.execute("SELECT COALESCE(max(seq),0) FROM reindex_outbox").fetchone()[0]
        xmin = int(ctl.execute("SELECT pg_snapshot_xmin(pg_current_snapshot())::text::bigint").fetchone()[0])
        print(f"  watermark: MAX(seq) = {max_seq}   snapshot xmin = {xmin}")

        release.set()
        t.join(10)
        print(f"  T1: COMMIT -> seq {captured['slow_seq']} is now visible")

        slow_xact = ctl.execute(
            "SELECT xact_id FROM reindex_outbox WHERE seq=%s", (captured["slow_seq"],)
        ).fetchone()[0]

        missed_by_seq = captured["slow_seq"] <= max_seq
        caught_by_xmin = slow_xact >= xmin

        print()
        print(f"  slow row: seq={captured['slow_seq']} xact_id={slow_xact}")
        print(f"  seq > MAX(seq) watermark ({max_seq}) ?   {captured['slow_seq'] > max_seq}   "
              f"{'<-- MAX(seq) REPLAY MISSES IT' if missed_by_seq else '(ordered luckily this run)'}")
        print(f"  xact_id >= xmin ({xmin}) ?              {caught_by_xmin}   "
              f"{'<-- XMIN REPLAY CATCHES IT' if caught_by_xmin else '<-- UNEXPECTED'}")
        print()
        print("  Sequences order statements, not commits. A snapshot xmin is the")
        print("  only one of the three that knows what was still in flight.")
        return caught_by_xmin


if __name__ == "__main__":
    a = demo_a()
    b = demo_b()
    print()
    print("=" * 72)
    print(f"demo A (timestamp skew shown): {a}")
    print(f"demo B (xmin covers out-of-order commit): {b}")
    print("=" * 72)
