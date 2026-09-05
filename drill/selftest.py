"""Tests for the failure paths: rejections, retries, and the cutover gate.

    python3 -m drill.selftest          # needs a live cluster
    python3 -m drill.selftest --fast   # only the tests that mock the client
"""
from __future__ import annotations

import argparse
import pathlib
import sys
from unittest.mock import patch

from .common import (
    ALIAS, BulkStats, bulk_send, current_alias_indices, doc_digest, es,
    recreate_index,
)

PASS, FAIL = "PASS", "FAIL"
_results: list[tuple[str, str, str]] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    _results.append((PASS if ok else FAIL, name, detail))
    print(f"  [{PASS if ok else FAIL}] {name}" + (f"  {detail}" if detail else ""))


# --------------------------------------------------------------- mocked
class _Meta:
    status = 200


class _Resp(dict):
    meta = _Meta()


class _FakeES:
    def __init__(self, responses):
        self._responses, self._n = responses, 0

    def bulk(self, operations):
        r = self._responses[min(self._n, len(self._responses) - 1)]
        self._n += 1
        return r


_DOC = {"id": 1, "title": "t", "body": "b", "status": "PUBLISHED",
        "version": 1, "updated_at": "2026-01-01T00:00:00+00:00"}


def test_hard_failure_survives_successful_retry() -> None:
    """A hard failure on one attempt survives a clean later attempt."""
    actions = [{"op": "index", "id": 1, "doc": _DOC},
               {"op": "index", "id": 2, "doc": _DOC}]
    responses = [
        _Resp(errors=True, items=[
            {"index": {"status": 429, "error": {"type": "es_rejected_execution_exception"}}},
            {"index": {"status": 400, "error": {"type": "strict_dynamic_mapping_exception"}}},
        ]),
        _Resp(errors=False, items=[{"index": {"status": 201}}]),
    ]
    s = BulkStats()
    with patch("drill.common.time.sleep"):
        unresolved = bulk_send(_FakeES(responses), "i", actions, s, max_attempts=6)
    check("hard failure survives a successful retry", unresolved == {2},
          f"unresolved={unresolved} expected {{2}}")


def test_retry_exhaustion_reports_all() -> None:
    """Items still pending when attempts run out must come back unresolved."""
    actions = [{"op": "index", "id": 7, "doc": _DOC}]
    responses = [_Resp(errors=True, items=[
        {"index": {"status": 429, "error": {"type": "es_rejected_execution_exception"}}}])]
    s = BulkStats()
    with patch("drill.common.time.sleep"):
        unresolved = bulk_send(_FakeES(responses), "i", actions, s, max_attempts=2)
    check("retry exhaustion reports the item", unresolved == {7},
          f"unresolved={unresolved} expected {{7}}")


def test_delete_404_is_not_a_failure() -> None:
    """Deleting an absent document is the desired end state."""
    actions = [{"op": "delete", "id": 3, "doc": None}]
    responses = [_Resp(errors=True, items=[{"delete": {"status": 404, "result": "not_found"}}])]
    s = BulkStats()
    unresolved = bulk_send(_FakeES(responses), "i", actions, s, max_attempts=2)
    check("404 on delete is not a failure", unresolved == set() and s.items_failed_hard == 0,
          f"unresolved={unresolved} failed_hard={s.items_failed_hard}")


def test_digest_delimiter_cannot_be_forged() -> None:
    a = doc_digest(1, "a|b", "c", "PUBLISHED", 1)
    b = doc_digest(1, "a", "b|c", "PUBLISHED", 1)
    check("digest is unambiguous across field boundaries", a != b)


def test_median_tolerates_none() -> None:
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "_mf", pathlib.Path(__file__).resolve().parent.parent / "figures" / "make_figures.py")
    mf = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mf)
    check("median ignores None instead of raising",
          mf._median([1.0, None, 3.0]) == 2.0, f"got {mf._median([1.0, None, 3.0])}")


# ------------------------------------------------------------ live cluster
def test_real_mapping_rejection() -> None:
    client = es()
    recreate_index(client, "selftest_idx", replicas=0, refresh="1s")
    s = BulkStats()
    bad = [{"op": "index", "id": 1,
            "doc": {**_DOC, "not_in_mapping": "boom"}}]
    unresolved = bulk_send(client, "selftest_idx", bad, s, max_attempts=2)
    ok_strict = unresolved == {1}

    s2 = BulkStats()
    mixed = [{"op": "index", "id": 10, "doc": {**_DOC, "id": 10}},
             {"op": "index", "id": 11, "doc": {**_DOC, "id": 11, "updated_at": "nope"}}]
    u2 = bulk_send(client, "selftest_idx", mixed, s2, max_attempts=2)
    ok_mixed = u2 == {11} and s2.items_indexed == 1

    client.indices.delete(index="selftest_idx")
    client.close()
    check("real strict-mapping rejection is reported", ok_strict, f"unresolved={unresolved}")
    check("mixed good/bad batch attributes correctly", ok_mixed,
          f"unresolved={u2} indexed={s2.items_indexed}")


def test_gate_blocks_on_unresolved_write() -> None:
    """A document that never lands stops the alias swap."""
    from . import corrected as C
    from . import seed as seed_mod
    seed_mod.seed(20000)

    real, state = C.bulk_send, {"poisoned": None}

    def poisoned(client, index, actions, stats, max_attempts=6):
        idx = [a for a in actions if a["op"] == "index"]
        if idx and state["poisoned"] is None:
            state["poisoned"] = idx[0]["id"]
        keep = [a for a in actions if a["id"] != state["poisoned"]]
        failed = real(client, index, keep, stats, max_attempts)
        if any(a["id"] == state["poisoned"] for a in actions):
            failed = set(failed) | {state["poisoned"]}
        return failed

    C.bulk_send = poisoned
    try:
        r = C.run(2000, 0, "-1", 6.0)
    finally:
        C.bulk_send = real

    client = es()
    alias_now = current_alias_indices(client, ALIAS)
    client.close()
    check("one failed document counts as one", r["unresolved_writes"] == 1,
          f"unresolved_writes={r['unresolved_writes']}")
    check("drain stops instead of spinning", r["drain_stalled"] and len(r["drain_passes"]) < 5,
          f"passes={len(r['drain_passes'])} stalled={r['drain_stalled']}")
    check("gate refuses the swap", r["gate_passed"] is False and r["alias_swap_ms"] is None)
    check("alias did not move", r["index"] not in alias_now, f"alias={alias_now}")


def test_seed_reenables_trigger_after_failure() -> None:
    from . import seed as S
    from .common import pg
    real = S._copy_rows
    S._copy_rows = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("simulated"))
    try:
        S.seed(1000)
    except RuntimeError:
        pass
    finally:
        S._copy_rows = real
    with pg() as c:
        state = c.execute(
            "SELECT tgenabled FROM pg_trigger WHERE tgname='searchable_content_outbox'"
        ).fetchone()
    check("failed COPY leaves the outbox trigger enabled", state[0] == "O",
          f"tgenabled={state[0]!r}")


def test_failed_run_does_not_orphan_writers() -> None:
    """Writers from a run that raised are findable and stoppable."""
    from . import corrected as C
    from . import seed as seed_mod
    from .workload import live_writer_count, stop_all_live
    seed_mod.seed(20000)

    real = C.full_scan_load

    def boom(*a, **k):
        raise RuntimeError("simulated mid-run failure")

    C.full_scan_load = boom
    try:
        C.run(2000, 0, "-1", 6.0)
    except RuntimeError:
        pass
    finally:
        C.full_scan_load = real

    leaked = live_writer_count()
    stopped = stop_all_live()
    check("a failed run's writers are findable and stoppable",
          stopped >= 1 and live_writer_count() == 0,
          f"live_before={leaked} stopped={stopped} live_after={live_writer_count()}")


def test_orphans_stay_findable_when_barrier_fails() -> None:
    """A workload whose barrier fails stays reachable via the registry."""
    import threading
    import time as _t
    from .workload import Workload, _LIVE, live_writer_count, stop_all_live

    w = Workload.__new__(Workload)
    w._stop, w._threads = threading.Event(), []
    w._hold_cv, w._lock = threading.Condition(), threading.Lock()
    stuck = threading.Thread(target=lambda: _t.sleep(20), daemon=True)
    w._threads.append(stuck)
    stuck.start()
    _LIVE.add(w)
    try:
        Workload.barrier(w, timeout=0.2)
        raised = False
    except RuntimeError:
        raised = True
    found = live_writer_count()
    stopped = stop_all_live(timeout=0.2)
    check("orphans stay findable after barrier() raises",
          raised and found == 1 and stopped == 1,
          f"raised={raised} live={found} stopped={stopped}")


def test_negative_check_is_scoped_to_this_run() -> None:
    """A delete recorded by a different run must not count as a resurrection."""
    from .common import pg
    from .reconcile import negative_check
    with pg() as conn:
        conn.execute("INSERT INTO workload_deletes (id, run_label) VALUES (%s,%s) "
                     "ON CONFLICT (id) DO NOTHING", (999_111, "some_other_run"))
        client = es()
        recreate_index(client, "selftest_neg", replicas=0, refresh="1s")
        client.index(index="selftest_neg", id="999111",
                     document={**_DOC, "id": 999_111}, refresh=True)
        res = negative_check(conn, client, "selftest_neg")
        client.indices.delete(index="selftest_neg")
        client.close()
        conn.execute("DELETE FROM workload_deletes WHERE id = %s", (999_111,))
    check("negative check ignores other runs' deletes",
          res["deleted_ids"] == 0 and res["resurrected"] == 0,
          f"deleted_ids={res['deleted_ids']} resurrected={res['resurrected']}")


def test_clean_run_still_passes() -> None:
    from . import corrected as C
    from . import seed as seed_mod
    seed_mod.seed(20000)
    r = C.run(2000, 0, "-1", 6.0)
    c = r["checks"]
    check("clean run passes the gate and swaps",
          r["gate_passed"] and c["clean"] and r["alias_swap_ms"] is not None,
          f"gate={r['gate_passed']} clean={c['clean']}")


MOCKED = [
    test_orphans_stay_findable_when_barrier_fails,
    test_hard_failure_survives_successful_retry,
    test_retry_exhaustion_reports_all,
    test_delete_404_is_not_a_failure,
    test_digest_delimiter_cannot_be_forged,
    test_median_tolerates_none,
]
LIVE = [
    test_real_mapping_rejection,
    test_seed_reenables_trigger_after_failure,
    test_negative_check_is_scoped_to_this_run,
    test_failed_run_does_not_orphan_writers,
    test_gate_blocks_on_unresolved_write,
    test_clean_run_still_passes,
]


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--fast", action="store_true", help="skip tests needing a cluster")
    args = ap.parse_args()

    print("mocked:")
    for t in MOCKED:
        t()
    if not args.fast:
        print("live cluster:")
        for t in LIVE:
            t()

    failed = [r for r in _results if r[0] == FAIL]
    print(f"\n{len(_results) - len(failed)}/{len(_results)} passed")
    sys.exit(1 if failed else 0)
