#!/usr/bin/env bash
# Postgres 16 + a two-node Elasticsearch cluster in one rootless Podman pod.
#
#   ./podman-setup.sh                 default write queue
#   ./podman-setup.sh --small-queue   thread_pool.write.queue_size=10
#
# Two Elasticsearch nodes, not one. A `number_of_replicas: 1` index on a
# single-node cluster leaves its replica permanently UNASSIGNED, so the
# "safe settings" row of the results table would measure no replication cost
# whatsoever and silently read as "replication is free".
#
# All three containers share the pod network namespace, so they reach each
# other on localhost and the host reaches Postgres and es1 on localhost too.
set -euo pipefail

POD=drill
SMALL_QUEUE=0
[[ "${1:-}" == "--small-queue" ]] && SMALL_QUEUE=1

PG_IMAGE=docker.io/library/postgres:16
# 8.15.0 bundles JDK 22, which dies with SIGILL in System.registerNatives on
# aarch64 under the applehv hypervisor (Apple silicon). The crash happens
# before any JVM flag can take effect, so -XX:UseSVE=0 does not help. 8.19.x
# bundles a fixed JDK. Override with DRILL_ES_IMAGE if you need a specific one.
ES_IMAGE=${DRILL_ES_IMAGE:-docker.elastic.co/elasticsearch/elasticsearch:8.19.3}
# 1g per node so two nodes plus Postgres fit an 8 GiB Podman machine.
ES_HEAP=${DRILL_ES_HEAP:-1g}

need_map_count() {
  local want=262144 have
  if [[ "$(uname -s)" == "Linux" ]]; then
    have=$(sysctl -n vm.max_map_count 2>/dev/null || echo 0)
    if (( have < want )); then
      echo "vm.max_map_count is $have, need >= $want. Elasticsearch will refuse to start."
      echo "Run:  sudo sysctl -w vm.max_map_count=$want"
      echo "Persist: echo 'vm.max_map_count=$want' | sudo tee /etc/sysctl.d/99-es.conf"
      exit 1
    fi
  else
    # macOS / Windows: the value lives in the Podman machine VM, not the host.
    have=$(podman machine ssh "sysctl -n vm.max_map_count" 2>/dev/null || echo 0)
    if (( have < want )); then
      echo "==> raising vm.max_map_count in the Podman machine ($have -> $want)"
      podman machine ssh "sudo sysctl -w vm.max_map_count=$want"
    fi
  fi
}

need_map_count

echo "==> tearing down any previous pod"
podman pod rm -f "$POD" >/dev/null 2>&1 || true

echo "==> creating pod $POD (5432, 9200)"
podman pod create --name "$POD" -p 5432:5432 -p 9200:9200 >/dev/null

echo "==> starting postgres"
podman run -d --pod "$POD" --name pg \
  -e POSTGRES_PASSWORD=drill \
  -e POSTGRES_DB=drill \
  -e POSTGRES_USER=drill \
  "$PG_IMAGE" \
  -c shared_preload_libraries=pg_stat_statements \
  -c pg_stat_statements.track=all \
  -c max_connections=200 >/dev/null

ES_COMMON=(
  -e xpack.security.enabled=false
  -e cluster.name=drill-cluster
  -e "discovery.seed_hosts=localhost:9300,localhost:9301"
  -e "cluster.initial_master_nodes=es1"
  -e "ES_JAVA_OPTS=-Xms$ES_HEAP -Xmx$ES_HEAP"
  --ulimit memlock=-1:-1
)
if (( SMALL_QUEUE )); then
  echo "==> starting elasticsearch x2 WITH thread_pool.write.queue_size=10"
  ES_COMMON+=(-e thread_pool.write.queue_size=10)
else
  echo "==> starting elasticsearch x2 (default write queue)"
fi

podman run -d --pod "$POD" --name es \
  -e node.name=es1 -e http.port=9200 -e transport.port=9300 \
  "${ES_COMMON[@]}" "$ES_IMAGE" >/dev/null
podman run -d --pod "$POD" --name es2 \
  -e node.name=es2 -e http.port=9201 -e transport.port=9301 \
  "${ES_COMMON[@]}" "$ES_IMAGE" >/dev/null

echo "==> waiting for postgres"
for i in $(seq 1 60); do
  if podman exec pg pg_isready -U drill -d drill >/dev/null 2>&1; then
    echo "    postgres ready"; break
  fi
  sleep 2
  if (( i == 60 )); then echo "postgres did not become ready"; exit 1; fi
done

echo "==> waiting for both elasticsearch nodes to form a cluster"
for i in $(seq 1 90); do
  # `|| echo 0` matters: while the nodes are still booting curl exits
  # non-zero, and under `set -o pipefail` that would abort the whole script
  # on the very first poll instead of retrying.
  n=$(curl -fsS localhost:9200/_cluster/health 2>/dev/null \
      | sed -n 's/.*"number_of_nodes":\([0-9]*\).*/\1/p' || echo 0)
  if [[ "$n" == "2" ]]; then echo "    cluster of 2 ready"; break; fi
  sleep 2
  if (( i == 90 )); then
    echo "cluster did not form; check: podman logs es ; podman logs es2"; exit 1
  fi
done

echo "==> enabling pg_stat_statements"
podman exec pg psql -U drill -d drill -qc \
  'CREATE EXTENSION IF NOT EXISTS pg_stat_statements' >/dev/null

echo
curl -s localhost:9200/_cluster/health | python3 -m json.tool
echo
echo "Ready. Next:"
echo "  python3 -m venv .venv && ./.venv/bin/pip install 'elasticsearch>=8,<9' 'psycopg[binary]'"
echo "  ./.venv/bin/python -m drill.report --all"
echo
echo "Tear down with: podman pod rm -f $POD"
