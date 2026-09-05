"""Segmented counts, sampled field comparison, negative checks.

Reported separately - they fail for different reasons and a single
total hides all of them. Everything runs after an explicit _refresh.
"""
from __future__ import annotations

from .common import (
    DIGEST_SQL, PROJECTION_PREDICATE, SAMPLE_SIZE, SEGMENT_SIZE, doc_digest,
)


def _all_es_digests(client, index) -> dict[int, str]:
    """Every (id, digest) pair, recomputing the digest from the returned fields.

    Reading back a digest field written at index time would only prove that
    field is consistent with the source - not that title/body/status/version
    in the document are. A doc could carry a correct digest beside a truncated
    body and pass. So this pulls the actual _source and hashes it here, with
    the source side hashing the same fields in SQL.

    updated_at is deliberately excluded: it is not canonicalised identically
    on both sides, and including it without a defined representation would
    produce mismatches that mean nothing.
    """
    out: dict[int, str] = {}
    after = None
    while True:
        kwargs: dict = {
            "size": 5000,
            "sort": [{"id": "asc"}],
            "source": ["id", "title", "body", "status", "version"],
        }
        if after is not None:
            kwargs["search_after"] = after
        hits = client.search(index=index, **kwargs)["hits"]["hits"]
        if not hits:
            break
        for h in hits:
            s = h["_source"]
            out[int(h["_id"])] = doc_digest(
                s.get("id"), s.get("title"), s.get("body"),
                s.get("status"), s.get("version"),
            )
        after = hits[-1]["sort"]
    return out


def segmented_counts(conn, client, index) -> dict:
    client.indices.refresh(index=index)
    db = dict(
        conn.execute(
            f"SELECT id / {SEGMENT_SIZE} AS seg, count(*) FROM searchable_content "
            f"WHERE {PROJECTION_PREDICATE} GROUP BY seg ORDER BY seg"
        ).fetchall()
    )
    agg = client.search(
        index=index,
        size=0,
        aggs={
            "seg": {
                "histogram": {"field": "id", "interval": SEGMENT_SIZE, "min_doc_count": 0}
            }
        },
    )
    esc = {
        int(b["key"]) // SEGMENT_SIZE: b["doc_count"]
        for b in agg["aggregations"]["seg"]["buckets"]
    }
    segments = []
    mismatched = 0
    for seg in sorted(set(db) | set(esc)):
        d, e = db.get(seg, 0), esc.get(seg, 0)
        if d != e:
            mismatched += 1
        segments.append({"segment": int(seg), "db": d, "es": e, "delta": e - d})
    return {
        "mismatched_segments": mismatched,
        "db_total": sum(db.values()),
        "es_total": sum(esc.values()),
        "segments": segments,
    }


def sampled_fields(conn, client, index, sample_size: int = SAMPLE_SIZE) -> dict:
    rows = conn.execute(
        f"SELECT id, title, body, status, version FROM searchable_content "
        f"WHERE {PROJECTION_PREDICATE} ORDER BY random() LIMIT {sample_size}"
    ).fetchall()
    by_id = {r[0]: r for r in rows}
    ids = list(by_id)

    stale, absent, examples = 0, 0, []
    for i in range(0, len(ids), 1000):
        chunk = ids[i:i + 1000]
        resp = client.mget(index=index, ids=[str(c) for c in chunk])
        for doc in resp["docs"]:
            rid = int(doc["_id"])
            row = by_id[rid]
            if not doc.get("found"):
                absent += 1
                if len(examples) < 5:
                    examples.append({"id": rid, "reason": "absent_from_index"})
                continue
            src = doc["_source"]
            diffs = [
                f for f, want in (
                    ("title", row[1]), ("body", row[2]),
                    ("status", row[3]), ("version", row[4]),
                ) if src.get(f) != want
            ]
            if diffs:
                stale += 1
                if len(examples) < 5:
                    examples.append({
                        "id": rid, "fields": diffs,
                        "db_version": row[4], "es_version": src.get("version"),
                    })
    return {
        "sampled": len(ids),
        "stale": stale,
        "absent": absent,
        "examples": examples,
    }


def negative_check(conn, client, index) -> dict:
    # Scoped by run_label so ids from another run cannot count as
    # resurrections here.
    deleted = [r[0] for r in conn.execute(
        "SELECT id FROM workload_deletes WHERE run_label = %s", (index,)
    ).fetchall()]
    if not deleted:
        return {"deleted_ids": 0, "resurrected": 0, "examples": []}
    resurrected, examples = 0, []
    for i in range(0, len(deleted), 1000):
        chunk = deleted[i:i + 1000]
        resp = client.mget(index=index, ids=[str(c) for c in chunk])
        for doc in resp["docs"]:
            if doc.get("found"):
                resurrected += 1
                if len(examples) < 5:
                    examples.append({"id": int(doc["_id"]), "title": doc["_source"].get("title")})
    return {"deleted_ids": len(deleted), "resurrected": resurrected, "examples": examples}


def reconcile(conn, client, index) -> dict:
    """Run all three checks and derive the exact defect counts."""
    client.indices.refresh(index=index)

    seg = segmented_counts(conn, client, index)
    sample = sampled_fields(conn, client, index)
    neg = negative_check(conn, client, index)

    # Exact, not sampled: correctness claims need exact numbers.
    db_digests = {
        r[0]: r[1] for r in conn.execute(
            f"SELECT id, {DIGEST_SQL} FROM searchable_content "
            f"WHERE {PROJECTION_PREDICATE}"
        ).fetchall()
    }
    es_digests = _all_es_digests(client, index)
    db_ids, es_ids = set(db_digests), set(es_digests)
    missing = db_ids - es_ids
    extra = es_ids - db_ids
    stale_exact = [i for i in db_ids & es_ids if db_digests[i] != es_digests[i]]

    # Extra documents have two different causes and lumping them together
    # hides one of them: rows deleted outright, and rows that still exist but
    # no longer satisfy the projection predicate.
    deleted_ids = {
        r[0] for r in conn.execute(
            "SELECT id FROM workload_deletes WHERE run_label = %s", (index,)
        ).fetchall()
    }
    still_present = {
        r[0] for r in conn.execute(
            "SELECT id FROM searchable_content WHERE id = ANY(%s)", (sorted(extra),)
        ).fetchall()
    } if extra else set()
    # A mutually exclusive partition, so the three sum to extra.count.
    extra_predicate, extra_deleted, extra_other = [], [], []
    for i in sorted(extra):
        if i in still_present:
            extra_predicate.append(i)      # row exists, predicate excludes it
        elif i in deleted_ids:
            extra_deleted.append(i)        # row deleted, document survived
        else:
            extra_other.append(i)          # neither: unexplained
    assert len(extra_predicate) + len(extra_deleted) + len(extra_other) == len(extra)

    return {
        "stale_exact": {
            "count": len(stale_exact),
            "compared": len(db_ids & es_ids),
            "method": "md5 over id|title|body|status|version, computed in SQL "
                      "on the source and recomputed from the returned _source "
                      "on the index side; excludes updated_at",
            "examples": [
                {"id": i, "db_digest": db_digests[i], "es_digest": es_digests[i]}
                for i in sorted(stale_exact)[:5]
            ],
            "note": "whole-index digest comparison; the sampled check misses "
                    "commit-time skew because only a handful of rows are affected",
        },
        "segmented_counts": seg,
        "sampled_fields": sample,
        "negative": neg,
        "missing": {
            "count": len(missing),
            "examples": sorted(missing)[:5],
            "note": "matches the predicate in Postgres, absent from the index",
        },
        "extra": {
            "count": len(extra),
            "deleted_source_rows": len(extra_deleted),
            "predicate_excluded": len(extra_predicate),
            "unexplained": len(extra_other),
            "examples": sorted(extra)[:5],
            "note": "in the index but not in the projection; split by cause",
        },
        "resurrected": {
            "count": len(extra_deleted),
            "confirmed_deleted": neg["resurrected"],
            "examples": extra_deleted[:5],
            "note": "source row deleted, document still indexed",
        },
        "stale": {
            "count": sample["stale"],
            "sampled": sample["sampled"],
            "note": f"field-level mismatch over {sample['sampled']} sampled ids",
        },
        "clean": (
            len(missing) == 0
            and len(extra) == 0
            and sample["stale"] == 0
            and len(stale_exact) == 0
        ),
    }
