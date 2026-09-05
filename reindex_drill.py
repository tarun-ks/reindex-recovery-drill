"""Same comparison with sqlite and a dict. No containers, no deps.

Shows the correctness argument only. SQLite serialises writers, so
there is no MVCC commit-time skew to stage here - the missing rows
below come from second-granularity timestamps instead. Real bulk
rejection, refresh visibility and throughput need the drill/ version.
"""
from __future__ import annotations

import random
import sqlite3
import threading
import time

ROWS = 20_000
UPDATES = 800
DELETES = 120
WORDS = "quantum ledger harbor cascade meridian tundra lattice cobalt ember verdant".split()


def now_seconds() -> int:
    """Second granularity, as plenty of schemas actually store it."""
    return int(time.time())


def build_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:", check_same_thread=False, isolation_level=None)
    conn.executescript(
        """
        CREATE TABLE searchable_content (
            id INTEGER PRIMARY KEY, title TEXT, body TEXT,
            status TEXT NOT NULL DEFAULT 'PUBLISHED',
            version INTEGER NOT NULL DEFAULT 1,
            updated_at INTEGER NOT NULL
        );
        CREATE TABLE outbox (
            seq INTEGER PRIMARY KEY AUTOINCREMENT,
            content_id INTEGER NOT NULL, op TEXT NOT NULL
        );
        CREATE TABLE deleted_log (id INTEGER PRIMARY KEY);
        CREATE TRIGGER ob_i AFTER INSERT ON searchable_content
          BEGIN INSERT INTO outbox (content_id, op) VALUES (NEW.id, 'upsert'); END;
        CREATE TRIGGER ob_u AFTER UPDATE ON searchable_content
          BEGIN INSERT INTO outbox (content_id, op) VALUES (NEW.id, 'upsert'); END;
        CREATE TRIGGER ob_d AFTER DELETE ON searchable_content
          BEGIN INSERT INTO outbox (content_id, op) VALUES (OLD.id, 'delete'); END;
        """
    )
    rng = random.Random(1337)
    base = now_seconds() - ROWS
    conn.execute("DROP TRIGGER ob_i")
    conn.executemany(
        "INSERT INTO searchable_content (id, title, body, status, version, updated_at)"
        " VALUES (?,?,?,?,1,?)",
        [
            (
                i,
                " ".join(rng.choice(WORDS) for _ in range(4)),
                " ".join(rng.choice(WORDS) for _ in range(40))[:300],
                "DRAFT" if rng.random() < 0.10 else "PUBLISHED",
                base + i,
            )
            for i in range(1, ROWS + 1)
        ],
    )
    conn.execute("DELETE FROM outbox")
    return conn


class Writer(threading.Thread):
    """Concurrent updates and deletes spread across the rebuild window."""

    def __init__(self, conn: sqlite3.Connection, window: float) -> None:
        super().__init__(daemon=True)
        self.conn, self.window = conn, window
        self.lock = threading.Lock()
        self.stop_flag = threading.Event()
        self.drafted = 0

    def quiesce(self) -> None:
        """Stop writes and wait for the last to land. Runs between the scan
        and the replay, which is the real cutover order."""
        self.stop_flag.set()
        self.join(30)

    def run(self) -> None:
        rng = random.Random(99)
        ops = ["u"] * UPDATES + ["d"] * DELETES
        rng.shuffle(ops)
        gap = self.window / len(ops)
        pool = rng.sample(range(1, ROWS + 1), DELETES)
        for op in ops:
            if self.stop_flag.is_set():
                return
            with self.lock:
                if op == "d":
                    rid = pool.pop()
                    self.conn.execute("DELETE FROM searchable_content WHERE id=?", (rid,))
                    self.conn.execute("INSERT OR IGNORE INTO deleted_log VALUES (?)", (rid,))
                else:
                    rid = rng.randint(1, ROWS)
                    to_draft = rng.random() < 0.08
                    self.conn.execute(
                        "UPDATE searchable_content SET title=?, version=version+1,"
                        " status=?, updated_at=? WHERE id=?",
                        (
                            " ".join(rng.choice(WORDS) for _ in range(4)),
                            "DRAFT" if to_draft else "PUBLISHED",
                            now_seconds(),
                            rid,
                        ),
                    )
                    if to_draft:
                        self.drafted += 1
            time.sleep(gap)


def scan_load(conn, writer, index: dict, batch: int = 500) -> int:
    """Stream rows in, yielding to the writer between batches."""
    loaded = 0
    last_id = 0
    while True:
        with writer.lock:
            rows = conn.execute(
                "SELECT id, title, body, status, version, updated_at"
                " FROM searchable_content WHERE status='PUBLISHED' AND id > ?"
                " ORDER BY id LIMIT ?",
                (last_id, batch),
            ).fetchall()
        if not rows:
            return loaded
        for r in rows:
            index[r[0]] = r
            loaded += 1
        last_id = rows[-1][0]
        # Paced: an unpaced in-memory scan finishes before the writer lands
        # more than a handful of commits.
        time.sleep(0.06)


def naive_rebuild(conn, writer) -> dict:
    index: dict = {}
    watermark = now_seconds()
    loaded = scan_load(conn, writer, index)
    writer.quiesce()
    with writer.lock:
        replay = conn.execute(
            "SELECT id, title, body, status, version, updated_at FROM searchable_content"
            " WHERE updated_at > ? AND status='PUBLISHED'",
            (watermark,),
        ).fetchall()
    for r in replay:
        index[r[0]] = r
    return {"index": index, "loaded": loaded, "replayed": len(replay), "watermark": watermark}


def corrected_rebuild(conn, writer) -> dict:
    index: dict = {}
    with writer.lock:
        watermark = conn.execute("SELECT COALESCE(MAX(seq),0) FROM outbox").fetchone()[0]
    loaded = scan_load(conn, writer, index)
    writer.quiesce()

    replayed = 0
    seen = watermark
    # Loop until a pass finds nothing new: entries can land mid-pass.
    for _ in range(20):
        with writer.lock:
            rows = conn.execute(
                "SELECT DISTINCT content_id FROM outbox WHERE seq > ?", (seen,)
            ).fetchall()
            seen = conn.execute("SELECT COALESCE(MAX(seq),?) FROM outbox", (seen,)).fetchone()[0]
        if not rows:
            break
        ids = [r[0] for r in rows]
        with writer.lock:
            live = {
                r[0]: r
                for r in conn.execute(
                    "SELECT id, title, body, status, version, updated_at"
                    f" FROM searchable_content WHERE status='PUBLISHED'"
                    f" AND id IN ({','.join('?' * len(ids))})",
                    ids,
                ).fetchall()
            }
        for cid in ids:
            # Re-apply the predicate: gone or non-PUBLISHED becomes a delete.
            if cid in live:
                index[cid] = live[cid]
            else:
                index.pop(cid, None)
            replayed += 1
    return {"index": index, "loaded": loaded, "replayed": replayed, "watermark": watermark}


def reconcile(conn, index: dict) -> dict:
    truth = {
        r[0]: r
        for r in conn.execute(
            "SELECT id, title, body, status, version, updated_at"
            " FROM searchable_content WHERE status='PUBLISHED'"
        ).fetchall()
    }
    deleted = {r[0] for r in conn.execute("SELECT id FROM deleted_log").fetchall()}

    missing = set(truth) - set(index)
    extra = set(index) - set(truth)
    stale = sum(1 for k in set(truth) & set(index) if truth[k] != index[k])

    segments: list[tuple[int, int, int]] = []
    for seg in sorted({k // 5000 for k in set(truth) | set(index)}):
        d = sum(1 for k in truth if k // 5000 == seg)
        e = sum(1 for k in index if k // 5000 == seg)
        segments.append((seg, d, e))

    return {
        "missing": len(missing),
        "resurrected": len(extra),
        "resurrected_confirmed_deleted": len(extra & deleted),
        "stale": stale,
        "mismatched_segments": sum(1 for _, d, e in segments if d != e),
        "segments": segments,
        "db_total": len(truth),
        "index_total": len(index),
        "clean": not missing and not extra and not stale,
    }


def one_run(label: str, rebuild, window: float = 2.6) -> dict:
    conn = build_db()
    writer = Writer(conn, window)
    writer.start()
    t0 = time.perf_counter()
    out = rebuild(conn, writer)  # quiesces the writer before its replay phase
    elapsed = time.perf_counter() - t0

    checks = reconcile(conn, out["index"])
    print(f"\n{label}")
    print("-" * len(label))
    print(f"  watermark          {out['watermark']}")
    print(f"  docs loaded        {out['loaded']:,}  replayed {out['replayed']:,}")
    print(f"  wall clock         {elapsed:.1f}s")
    print(f"  db published       {checks['db_total']:,}   index {checks['index_total']:,}")
    print(f"  segments mismatch  {checks['mismatched_segments']}")
    print(f"  MISSING            {checks['missing']}")
    print(f"  RESURRECTED        {checks['resurrected']}"
          f"  (confirmed deleted: {checks['resurrected_confirmed_deleted']})")
    print(f"  STALE              {checks['stale']}")
    print(f"  CLEAN              {checks['clean']}")
    conn.close()
    return checks


if __name__ == "__main__":
    print("=" * 68)
    print("reindex recovery drill -- stdlib demo (sqlite + dict)")
    print(f"{ROWS:,} rows, {UPDATES} concurrent updates, {DELETES} concurrent deletes")
    print("=" * 68)

    naive = one_run("NAIVE  truncate-and-reload, timestamp watermark", naive_rebuild)
    corrected = one_run("CORRECTED  build-aside, outbox drain, predicate re-applied", corrected_rebuild)

    print()
    print("=" * 68)
    print(f"{'':<14}{'missing':>10}{'resurrected':>14}{'stale':>8}{'clean':>8}")
    print(f"{'naive':<14}{naive['missing']:>10}{naive['resurrected']:>14}"
          f"{naive['stale']:>8}{str(naive['clean']):>8}")
    print(f"{'corrected':<14}{corrected['missing']:>10}{corrected['resurrected']:>14}"
          f"{corrected['stale']:>8}{str(corrected['clean']):>8}")
    print("=" * 68)
    print("\nFor throughput and real Elasticsearch/Postgres semantics:")
    print("  ./podman-setup.sh && python3 -m drill.report --all")
