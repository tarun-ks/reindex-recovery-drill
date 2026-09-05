"""Seed searchable_content with ROW_COUNT rows via COPY."""
from __future__ import annotations

import argparse
import datetime as dt
import random
import time
from pathlib import Path

from .common import PG_DSN, ROW_COUNT, pg, random_body, random_title


def _copy_rows(conn, rows: int, rng: random.Random, base: dt.datetime) -> float:
    t0 = time.perf_counter()
    with conn.cursor() as cur:
        with cur.copy(
            "COPY searchable_content (id, title, body, status, version, updated_at) FROM STDIN"
        ) as copy:
            for i in range(1, rows + 1):
                status = "DRAFT" if rng.random() < 0.10 else "PUBLISHED"
                copy.write_row((
                    i,
                    random_title(rng),
                    random_body(rng),
                    status,
                    1,
                    base + dt.timedelta(seconds=i),
                ))
                if i % 100_000 == 0:
                    print(f"    {i:,}")
    return time.perf_counter() - t0


def seed(rows: int = ROW_COUNT, seed_value: int = 1337) -> None:
    schema = (Path(__file__).resolve().parent / "schema.sql").read_text()
    rng = random.Random(seed_value)
    base = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)

    with pg() as conn:
        print("==> applying schema")
        conn.execute(schema)

        # Disabled so the trigger does not fire once per seeded row, and
        # re-enabled in a finally block so it is never left off.
        conn.execute("ALTER TABLE searchable_content DISABLE TRIGGER searchable_content_outbox")
        try:
            print(f"==> loading {rows:,} rows via COPY")
            elapsed = _copy_rows(conn, rows, rng, base)
        finally:
            conn.execute(
                "ALTER TABLE searchable_content ENABLE TRIGGER searchable_content_outbox"
            )

        conn.execute("TRUNCATE reindex_outbox RESTART IDENTITY")
        conn.execute("TRUNCATE workload_deletes")

        rate = f"{rows / elapsed:,.0f} rows/sec" if elapsed else "instant"
        print(f"==> ANALYZE ({elapsed:.1f}s to copy, {rate})")
        conn.execute("ANALYZE searchable_content")

        total, published, draft = conn.execute(
            "SELECT count(*), count(*) FILTER (WHERE status='PUBLISHED'),"
            "       count(*) FILTER (WHERE status='DRAFT') FROM searchable_content"
        ).fetchone()
        avg_body = conn.execute(
            "SELECT round(avg(length(body))) FROM searchable_content"
        ).fetchone()[0]

    pct = f"{draft / total:.1%}" if total else "n/a"
    print(f"    total={total:,} published={published:,} draft={draft:,} "
          f"({pct}) avg_body={avg_body} chars")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=ROW_COUNT)
    args = ap.parse_args()
    print(f"dsn: {PG_DSN}")
    seed(args.rows)
