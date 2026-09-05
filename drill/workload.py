"""Concurrent writer for the duration of a rebuild.

~2000 updates, 250 deletes, 120 inserts over 8 connections. Some
transactions hold open before committing, which is where commit-time
skew comes from. Writers start before the watermark is read, since
otherwise every write trivially lands after it.
"""
from __future__ import annotations

import random
import threading
import time

import psycopg

from .common import PG_DSN, random_body, random_title

# Writers are daemon threads, so a caller that aborts mid-run needs a way to
# find and stop them before reseeding the source underneath them.
_LIVE: set = set()


def stop_all_live(timeout: float = 60.0) -> int:
    """Stop every writer that is still running. Returns how many were found."""
    found = 0
    for w in list(_LIVE):
        found += 1
        w._stop.set()
        deadline = time.monotonic() + timeout
        for t in w._threads:
            t.join(max(0.0, deadline - time.monotonic()))
        _LIVE.discard(w)
    return found


def live_writer_count() -> int:
    return sum(1 for w in _LIVE for t in w._threads if t.is_alive())


class Workload:
    def __init__(
        self,
        run_label: str,
        max_id: int,
        updates: int = 2000,
        deletes: int = 250,
        inserts: int = 120,
        window_seconds: float = 12.0,
        workers: int = 8,
        hold_fraction: float = 0.12,
        hold_seconds: float = 0.30,
        first_hold_seconds: float = 1.5,
        seed: int = 99,
    ) -> None:
        self.run_label = run_label
        self.max_id = max_id
        self.updates = updates
        self.deletes = deletes
        self.inserts = inserts
        self.window_seconds = window_seconds
        self.workers = workers
        self.hold_fraction = hold_fraction
        self.hold_seconds = hold_seconds
        # The first hold per worker is longer so the driver reliably reads its
        # watermark inside the window where all workers are mid-transaction.
        self.first_hold_seconds = first_hold_seconds

        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []
        self._holding = 0
        self._hold_cv = threading.Condition()
        self.peak_concurrent_holds = 0

        self.stats = {
            "ops_issued": 0,
            "updates_committed": 0,
            "deletes_committed": 0,
            "inserts_committed": 0,
            "held_transactions": 0,
            "drafted": 0,
            "errors": 0,
        }
        self.deleted_ids: list[int] = []
        self.inserted_ids: list[int] = []

        rng = random.Random(seed)
        ops = ["update"] * updates + ["delete"] * deletes + ["insert"] * inserts
        rng.shuffle(ops)
        delete_pool = rng.sample(range(1, max_id + 1), deletes)
        # Partition the work up front so the totals are exact regardless of
        # how the threads interleave.
        self._plans: list[list[tuple[str, int | None]]] = [[] for _ in range(workers)]
        di = 0
        next_insert_id = max_id + 1
        for i, op in enumerate(ops):
            w = i % workers
            if op == "delete":
                self._plans[w].append(("delete", delete_pool[di]))
                di += 1
            elif op == "insert":
                self._plans[w].append(("insert", next_insert_id))
                next_insert_id += 1
            else:
                self._plans[w].append(("update", None))

        self._seeds = [seed + 1 + i for i in range(workers)]

    def start(self) -> None:
        _LIVE.add(self)
        for i in range(self.workers):
            t = threading.Thread(target=self._run, args=(i,), name=f"wl{i}", daemon=True)
            t.start()
            self._threads.append(t)

    def wait_for_held_transaction(self, count: int = 0, timeout: float = 30.0) -> int:
        """Block until `count` workers are holding at once (default: all of
        them). Returns how many were holding when it gave up."""
        want = count or self.workers
        deadline = time.monotonic() + timeout
        with self._hold_cv:
            while self._holding < want:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                self._hold_cv.wait(remaining)
            return self._holding

    def barrier(self, timeout: float = 120.0) -> float:
        """Write barrier: stop accepting new writes, wait for in-flight ones.

        Returns how long in-flight transactions took to clear. Workers check
        the flag between operations, so setting it blocks new writes while
        letting open transactions commit.

        Note for readers of the results: corrected.run() calls wait_for_plan()
        first, so by the time this runs the workload has already finished and
        there is nothing in flight. The returned value is therefore ~0 by
        construction - it is the cost of quiescing an idle writer, not latency
        for writes blocked by an active barrier. Measuring the latter needs a
        writer that keeps submitting after the flag is set.
        """
        t0 = time.perf_counter()
        self._stop.set()
        deadline = time.monotonic() + timeout
        for t in self._threads:
            t.join(max(0.0, deadline - time.monotonic()))
        alive = [t.name for t in self._threads if t.is_alive()]
        if alive:
            # Left in _LIVE so the caller can still reach these via
            # stop_all_live().
            raise RuntimeError(
                f"write barrier did not hold: {alive} still running. "
                "Draining now would replay against a moving source."
            )
        _LIVE.discard(self)
        return time.perf_counter() - t0

    def plan_completed(self) -> bool:
        """True if the writer issued every planned operation before the barrier.

        Counts operations issued, not rows changed: an UPDATE against a row a
        previous op deleted legitimately affects zero rows, so a
        commit-based check reports False when nothing is wrong.
        """
        return self.stats["ops_issued"] >= self.updates + self.deletes + self.inserts

    def wait_for_plan(self, timeout: float = 600.0) -> None:
        """Let the workload finish issuing its planned writes."""
        deadline = time.monotonic() + timeout
        for t in self._threads:
            t.join(max(0.0, deadline - time.monotonic()))
        # Not fatal here - barrier() is the one that must hold - but a silent
        # timeout would change the experiment without saying so.
        if any(t.is_alive() for t in self._threads):
            print("    warning: workload did not finish its plan within timeout")

    def _enter_hold(self) -> None:
        with self._hold_cv:
            self._holding += 1
            self.peak_concurrent_holds = max(self.peak_concurrent_holds, self._holding)
            self._hold_cv.notify_all()

    def _exit_hold(self) -> None:
        with self._hold_cv:
            self._holding -= 1
            self._hold_cv.notify_all()
        with self._lock:
            self.stats["held_transactions"] += 1

    def _run(self, worker: int) -> None:
        plan = self._plans[worker]
        rng = random.Random(self._seeds[worker])
        gap = self.window_seconds / max(1, len(plan))

        with psycopg.connect(PG_DSN, autocommit=False) as conn:
            for n, (op, arg) in enumerate(plan):
                if self._stop.is_set():
                    break
                started = time.perf_counter()
                # First write per worker holds, so the watermark is read with
                # all 8 open. Makes the race reproducible, not artificial.
                force_hold = n == 0
                with self._lock:
                    self.stats["ops_issued"] += 1
                try:
                    if op == "delete":
                        self._do_delete(conn, arg, force_hold)
                    elif op == "insert":
                        self._do_insert(conn, rng, arg, force_hold)
                    else:
                        self._do_update(conn, rng, force_hold)
                except psycopg.Error:
                    try:
                        conn.rollback()
                    except psycopg.Error:
                        pass
                    with self._lock:
                        self.stats["errors"] += 1
                slack = gap - (time.perf_counter() - started)
                if slack > 0:
                    time.sleep(slack)

    def _do_delete(self, conn: psycopg.Connection, rid: int,
                   force_hold: bool = False) -> None:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM searchable_content WHERE id = %s", (rid,))
            deleted = cur.rowcount
            if deleted:
                cur.execute(
                    "INSERT INTO workload_deletes (id, run_label) VALUES (%s, %s) "
                    "ON CONFLICT (id) DO NOTHING",
                    (rid, self.run_label),
                )
        # Deletes take the forced first hold too, so every worker joins the
        # quorum regardless of which operation its plan starts with.
        if force_hold:
            self._enter_hold()
            time.sleep(self.first_hold_seconds)
            self._exit_hold()
        conn.commit()
        if deleted:
            with self._lock:
                self.deleted_ids.append(rid)
                self.stats["deletes_committed"] += 1

    def _do_insert(
        self, conn: psycopg.Connection, rng: random.Random, rid: int, force_hold: bool
    ) -> None:
        """New content mid-rebuild. Ids sort above anything the cursor reaches
        and the snapshot predates the commit, so a held insert is the case that
        produces a genuinely missing document."""
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO searchable_content (id, title, body, status, version, updated_at)"
                " VALUES (%s, %s, %s, 'PUBLISHED', 1, clock_timestamp())"
                " ON CONFLICT (id) DO NOTHING",
                (rid, random_title(rng), random_body(rng)),
            )
            inserted = cur.rowcount

        if force_hold or rng.random() < self.hold_fraction:
            self._enter_hold()
            time.sleep(self.first_hold_seconds if force_hold else self.hold_seconds)
            self._exit_hold()

        conn.commit()
        if inserted:
            with self._lock:
                self.inserted_ids.append(rid)
                self.stats["inserts_committed"] += 1

    def _do_update(
        self, conn: psycopg.Connection, rng: random.Random, force_hold: bool = False
    ) -> None:
        rid = rng.randint(1, self.max_id)
        # ~8% of updates flip PUBLISHED -> DRAFT. These must leave the index,
        # which only happens if the replay re-applies the projection predicate.
        to_draft = rng.random() < 0.08
        hold = force_hold or rng.random() < self.hold_fraction

        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE searchable_content
                   SET title = %s,
                       body = %s,
                       status = CASE WHEN %s THEN 'DRAFT' ELSE 'PUBLISHED' END,
                       version = version + 1,
                       updated_at = clock_timestamp()
                 WHERE id = %s
                """,
                (random_title(rng), random_body(rng), to_draft, rid),
            )
            changed = cur.rowcount

        if hold:
            # updated_at is already stamped; the commit has not happened yet.
            self._enter_hold()
            time.sleep(self.first_hold_seconds if force_hold else self.hold_seconds)
            self._exit_hold()

        conn.commit()
        if changed:
            with self._lock:
                self.stats["updates_committed"] += 1
                if to_draft:
                    self.stats["drafted"] += 1
