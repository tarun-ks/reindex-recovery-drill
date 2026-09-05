"""Shared plumbing: connections, index mapping, bulk with real retry accounting."""
from __future__ import annotations

import json
import os
import platform
import random
import shutil
import subprocess
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path

import hashlib

import psycopg
from elasticsearch import ApiError, Elasticsearch, NotFoundError

PG_DSN = os.environ.get(
    "DRILL_PG_DSN", "host=localhost port=5432 dbname=drill user=drill password=drill"
)
ES_URL = os.environ.get("DRILL_ES_URL", "http://localhost:9200")
ALIAS = "searchable_content"
ROW_COUNT = int(os.environ.get("DRILL_ROWS", "500000"))
SEGMENT_SIZE = 100_000
SAMPLE_SIZE = 5_000

RESULTS_DIR = Path(__file__).resolve().parent.parent / "results"

# Digest over id|title|body|status|version. Each field is length-prefixed
# because a plain delimiter is forgeable: with "a|b" + "c" and "a" + "b|c" a
# bare join produces the same bytes. Excludes updated_at, which is not
# canonicalised identically on both sides.
_DIGEST_FIELDS = ("id::text", "title", "body", "status", "version::text")
DIGEST_SQL = (
    "md5(" + " || ".join(
        f"length({f})::text || ':' || {f}" for f in _DIGEST_FIELDS
    ) + ")"
)

# One definition, read by both the load and the replay.
PROJECTION_PREDICATE = "status = 'PUBLISHED'"

INDEX_SETTINGS_TEMPLATE = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "refresh_interval": "-1",
    },
    "mappings": {
        "dynamic": "strict",
        "properties": {
            "id": {"type": "long"},
            "title": {
                "type": "text",
                "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
            },
            "body": {"type": "text"},
            "status": {"type": "keyword"},
            "version": {"type": "integer"},
            "updated_at": {"type": "date"},
        },
    },
}


def pg(autocommit: bool = True) -> psycopg.Connection:
    return psycopg.connect(PG_DSN, autocommit=autocommit)


def es(connections_per_node: int = 25) -> Elasticsearch:
    # Retries off: client-side retries would hide the rejections we measure.
    # connections_per_node default is 10, so anything above that queues in the
    # client and the server never sees a burst.
    return Elasticsearch(
        ES_URL,
        request_timeout=120,
        retry_on_timeout=False,
        max_retries=0,
        connections_per_node=connections_per_node,
    )


def index_body(replicas: int, refresh: str) -> dict:
    body = json.loads(json.dumps(INDEX_SETTINGS_TEMPLATE))
    body["settings"]["number_of_replicas"] = replicas
    body["settings"]["refresh_interval"] = refresh
    return body


def recreate_index(client: Elasticsearch, name: str, replicas: int, refresh: str) -> None:
    if client.indices.exists(index=name):
        client.indices.delete(index=name)
    body = index_body(replicas, refresh)
    client.indices.create(
        index=name, settings=body["settings"], mappings=body["mappings"]
    )


def current_alias_indices(client: Elasticsearch, alias: str) -> list[str]:
    """Indices behind the alias, [] if it does not exist yet. get_alias raises
    404 for a missing alias, which a first run always hits."""
    try:
        return list(client.indices.get_alias(name=alias))
    except NotFoundError:
        return []


def point_alias(client: Elasticsearch, alias: str, to_index: str) -> float:
    """Atomically repoint an alias. Returns how long _aliases actually took."""
    actions = [{"add": {"index": to_index, "alias": alias}}]
    for idx in current_alias_indices(client, alias):
        if idx != to_index:
            actions.insert(0, {"remove": {"index": idx, "alias": alias}})
    t0 = time.perf_counter()
    client.indices.update_aliases(actions=actions)
    return time.perf_counter() - t0


def doc_digest(rid, title, body, status, version) -> str:
    """Must match DIGEST_SQL exactly: same fields, order, and length prefixes."""
    parts = [str(rid), str(title), str(body), str(status), str(version)]
    joined = "".join(f"{len(v)}:{v}" for v in parts)
    return hashlib.md5(joined.encode()).hexdigest()


def row_to_doc(row) -> dict:
    rid, title, body, status, version, updated_at = row
    return {
        "id": rid,
        "title": title,
        "body": body,
        "status": status,
        "version": version,
        "updated_at": updated_at.isoformat(),
    }


@dataclass
class BulkStats:
    requests: int = 0
    items_sent: int = 0
    items_indexed: int = 0
    items_deleted: int = 0
    items_rejected: int = 0       # retryable rejections: per-item + whole-429
    items_retried: int = 0        # re-sent after a rejection
    items_failed_hard: int = 0    # gave up after max attempts
    http_200_with_errors: int = 0  # 200 OK whose body said errors=true
    http_429_whole_request: int = 0  # coordinating node refused the request
    error_types: dict = field(default_factory=dict)
    seconds_in_bulk: float = 0.0

    def merge(self, other: "BulkStats") -> None:
        for k, v in asdict(other).items():
            if k == "error_types":
                for et, n in v.items():
                    self.error_types[et] = self.error_types.get(et, 0) + n
            else:
                setattr(self, k, getattr(self, k) + v)


RETRYABLE = {"es_rejected_execution_exception", "circuit_breaking_exception"}


def bulk_send(
    client: Elasticsearch,
    index: str,
    actions: list[dict],
    stats: BulkStats,
    max_attempts: int = 6,
) -> set:
    """Send one bulk batch, retrying only rejected items.

    actions: [{"op": "index"|"delete", "id": int, "doc": dict|None}].
    A partially failed bulk comes back 200 with errors: true - nothing raises.

    Returns the ids that never landed; callers must not treat those as done.
    """
    pending = actions
    unresolved: set = set()
    attempt = 0
    while pending and attempt < max_attempts:
        attempt += 1
        lines: list[dict] = []
        for a in pending:
            if a["op"] == "delete":
                lines.append({"delete": {"_index": index, "_id": str(a["id"])}})
            else:
                lines.append({"index": {"_index": index, "_id": str(a["id"])}})
                lines.append(a["doc"])

        t0 = time.perf_counter()
        try:
            resp = client.bulk(operations=lines)
        except ApiError as exc:
            # Two shapes: 429 means the coordinating node refused the whole
            # request; 200 + errors:true means individual shard ops were
            # refused. The second one is the one that drops documents quietly.
            stats.seconds_in_bulk += time.perf_counter() - t0
            stats.requests += 1
            stats.items_sent += len(pending)
            if exc.meta.status == 429:
                stats.http_429_whole_request += 1
                stats.items_rejected += len(pending)
                stats.error_types["es_rejected_execution_exception(429)"] = (
                    stats.error_types.get("es_rejected_execution_exception(429)", 0) + 1
                )
                # Only count a retry that will actually happen: incrementing
                # before checking the budget records intent, not events.
                if attempt < max_attempts:
                    stats.items_retried += len(pending)
                    time.sleep(min(2.0, 0.05 * (2 ** attempt)) * (0.5 + random.random()))
                continue
            raise
        stats.seconds_in_bulk += time.perf_counter() - t0
        stats.requests += 1
        stats.items_sent += len(pending)

        meta = getattr(resp, "meta", None)
        status = getattr(meta, "status", 200)
        has_errors = bool(resp.get("errors"))
        if status == 200 and has_errors:
            stats.http_200_with_errors += 1

        if not has_errors:
            for a in pending:
                if a["op"] == "delete":
                    stats.items_deleted += 1
                else:
                    stats.items_indexed += 1
            # A clean retry does not resolve failures from earlier attempts.
            return unresolved

        retry: list[dict] = []
        for a, item in zip(pending, resp["items"], strict=True):
            result = next(iter(item.values()))
            err = result.get("error")
            if err is None:
                if a["op"] == "delete":
                    stats.items_deleted += 1
                else:
                    stats.items_indexed += 1
                continue
            etype = err.get("type", "unknown")
            stats.error_types[etype] = stats.error_types.get(etype, 0) + 1
            if etype in RETRYABLE:
                stats.items_rejected += 1
                retry.append(a)
            elif a["op"] == "delete" and result.get("status") == 404:
                # An absent document is the desired end state for a delete.
                stats.items_deleted += 1
            else:
                stats.items_failed_hard += 1
                unresolved.add(a["id"])

        pending = retry
        if pending and attempt < max_attempts:
            stats.items_retried += len(pending)
            # Jittered backoff, or retries land in the same saturated queue.
            time.sleep(min(2.0, 0.05 * (2 ** attempt)) * (0.5 + random.random()))

    if pending:
        stats.items_failed_hard += len(pending)
        unresolved.update(a["id"] for a in pending)
    return unresolved


WORDS = [
    "quantum", "ledger", "harbor", "cascade", "meridian", "tundra", "lattice",
    "cobalt", "ember", "verdant", "arcane", "sable", "onyx", "kestrel", "borealis",
    "silica", "monsoon", "citadel", "prism", "zenith", "alloy", "nimbus", "thicket",
    "gantry", "obsidian", "quarry", "trellis", "vellum", "wharf", "yarrow",
]


def random_title(rng: random.Random) -> str:
    return " ".join(rng.choice(WORDS) for _ in range(rng.randint(3, 6))).title()


def random_body(rng: random.Random) -> str:
    target = rng.randint(200, 400)
    out: list[str] = []
    size = 0
    while size < target:
        w = rng.choice(WORDS)
        out.append(w)
        size += len(w) + 1
    return " ".join(out)[:target]


def machine_specs() -> dict:
    spec = {
        "platform": platform.platform(),
        "python": platform.python_version(),
        "logical_cpus": os.cpu_count(),
    }
    try:
        if platform.system() == "Darwin":
            spec["cpu"] = subprocess.check_output(
                ["sysctl", "-n", "machdep.cpu.brand_string"], text=True
            ).strip()
            spec["ram_bytes"] = int(
                subprocess.check_output(["sysctl", "-n", "hw.memsize"], text=True).strip()
            )
        else:
            with open("/proc/cpuinfo") as fh:
                for line in fh:
                    if line.startswith("model name"):
                        spec["cpu"] = line.split(":", 1)[1].strip()
                        break
            with open("/proc/meminfo") as fh:
                for line in fh:
                    if line.startswith("MemTotal"):
                        spec["ram_bytes"] = int(line.split()[1]) * 1024
                        break
    except Exception as exc:  # noqa: BLE001 - specs are informational
        spec["probe_error"] = str(exc)

    if shutil.which("podman"):
        try:
            spec["containers"] = json.loads(
                subprocess.check_output(
                    ["podman", "ps", "--pod", "--format", "json"], text=True
                )
            )
            for name in ("pg", "es"):
                out = subprocess.check_output(
                    ["podman", "inspect", name, "--format",
                     "{{.HostConfig.Memory}} {{.HostConfig.NanoCpus}}"],
                    text=True, stderr=subprocess.DEVNULL,
                ).split()
                spec.setdefault("container_limits", {})[name] = {
                    "memory_bytes": int(out[0]), "nano_cpus": int(out[1]),
                }
        except Exception:  # noqa: BLE001
            pass
    return spec


def es_topology(client: Elasticsearch) -> dict:
    """Cluster shape. Recorded because replicas=1 on a single node leaves the
    replica UNASSIGNED, which makes replication look free."""
    health = client.cluster.health()
    nodes = client.nodes.info(metric="jvm")["nodes"]
    return {
        "nodes": health["number_of_nodes"],
        "data_nodes": health["number_of_data_nodes"],
        "status": health["status"],
        "unassigned_shards": health["unassigned_shards"],
        "heap_max_mb_per_node": sorted(
            n["jvm"]["mem"]["heap_max_in_bytes"] // (1024 * 1024) for n in nodes.values()
        ),
        "version": client.info()["version"]["number"],
    }


def index_shard_state(client: Elasticsearch, index: str, replicas: int) -> dict:
    h = client.cluster.health(index=index)
    return {
        "status": h["status"],
        "active_shards": h["active_shards"],
        "unassigned_shards": h["unassigned_shards"],
        # Only meaningful if replicas were requested at all.
        "replica_allocated": (
            bool(replicas) and h["unassigned_shards"] == 0 and h["active_shards"] > 1
        ),
    }


def reset_pg_stats(conn: psycopg.Connection) -> None:
    try:
        conn.execute("SELECT pg_stat_statements_reset()")
    except psycopg.Error:
        conn.rollback() if not conn.autocommit else None


def collect_pg_stats(conn: psycopg.Connection) -> dict:
    """pg_stat_statements totals, standing in for peak CPU."""
    try:
        row = conn.execute(
            """
            SELECT round(sum(total_exec_time)::numeric, 1),
                   sum(calls),
                   round(sum(rows)::numeric, 0)
            FROM pg_stat_statements
            """
        ).fetchone()
        top = conn.execute(
            """
            SELECT left(query, 90), calls, round(total_exec_time::numeric, 1)
            FROM pg_stat_statements
            ORDER BY total_exec_time DESC LIMIT 5
            """
        ).fetchall()
        return {
            "total_exec_ms": float(row[0] or 0),
            "calls": int(row[1] or 0),
            "rows": int(row[2] or 0),
            "top_statements": [
                {"query": q, "calls": c, "total_ms": float(t)} for q, c, t in top
            ],
        }
    except psycopg.Error as exc:
        if not conn.autocommit:
            conn.rollback()
        return {"error": str(exc)}


def full_scan_load(
    client: Elasticsearch,
    index: str,
    batch_size: int,
    stats: BulkStats,
) -> tuple[int, set]:
    """Stream every projectable row into `index` in batch_size chunks.

    Returns (documents that landed, ids that did not).

    Uses its own connection: a named cursor needs a transaction, and that
    transaction pins a snapshot for the cursor's life, so writes committed
    after it is declared are invisible to the scan. The caller's connection
    stays on a live snapshot for the replay queries.
    """
    sent = 0
    unresolved: set = set()
    with pg(autocommit=False) as scan_conn:
        with scan_conn.cursor(name="scan") as cur:
            cur.itersize = batch_size
            cur.execute(
                f"SELECT id, title, body, status, version, updated_at "
                f"FROM searchable_content WHERE {PROJECTION_PREDICATE} ORDER BY id"
            )
            batch: list[dict] = []
            for row in cur:
                batch.append({"op": "index", "id": row[0], "doc": row_to_doc(row)})
                if len(batch) >= batch_size:
                    failed = bulk_send(client, index, batch, stats)
                    unresolved |= failed
                    sent += len(batch) - len(failed)
                    batch = []
            if batch:
                failed = bulk_send(client, index, batch, stats)
                unresolved |= failed
                sent += len(batch) - len(failed)
        scan_conn.rollback()
    return sent, unresolved


def save_result(payload: dict) -> Path:
    """One artifact per run. Repeats get their own file so a claim about N runs
    has N pieces of evidence behind it."""
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    rep = payload.get("repeat")
    suffix = f"_run{rep}" if rep is not None else ""
    name = (f"{payload['recovery']}_b{payload['batch_size']}"
            f"_r{payload['replicas']}_{payload['refresh_label']}{suffix}.json")
    path = RESULTS_DIR / name.replace("/", "-")
    path.write_text(json.dumps(payload, indent=2, default=str))
    return path
