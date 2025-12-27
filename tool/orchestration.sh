#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INFRA_ENV_FILE="${INFRA_ENV_FILE:-$ROOT_DIR/config/infrastructure/env/docker.env}"

usage() {
  cat <<'USAGE'
Usage: ./tool/orchestration.sh <keywords...>

Keywords:
  ingest  - redis, kafka, worker
  stream  - redis, kafka, postgres, clickhouse, worker, flink (jm/tm)
  batch   - spark (master/worker/client)
  s       - starrocks (allin1 + init)
  m       - minio (server + mc init)
  o       - observability (loki/grafana/promtail/prometheus/kafka-exporter)
  a       - airflow (postgres/init/webserver/scheduler)
  w       - worker (datainjector/worker)
  cp      - control-plane (datainjector/control-plane-service)
  bd      - build selected services before start
  k       - kafka (zk/kafka/ui)
  c       - clickhouse

Examples:
  ./tool/orchestration.sh ingest
  ./tool/orchestration.sh stream o bd
  ./tool/orchestration.sh k c w
USAGE
}

compose_cmd() {
  if [[ ! -f "$INFRA_ENV_FILE" ]]; then
    echo "找不到环境变量文件: $INFRA_ENV_FILE" >&2
    exit 1
  fi
  (cd "$ROOT_DIR/automation/orchestration" && docker compose --env-file "$INFRA_ENV_FILE" -f docker-compose.yml "$@")
}

ensure_infra_network() {
  if [[ ! -f "$INFRA_ENV_FILE" ]]; then
    return
  fi
  local infra_network
  infra_network="$(awk -F= '$1=="INFRA_NETWORK"{print substr($0, index($0,$2))}' "$INFRA_ENV_FILE")"
  if [[ -z "$infra_network" ]]; then
    return
  fi
  if ! docker network inspect "$infra_network" >/dev/null 2>&1; then
    docker network create "$infra_network" >/dev/null
    echo "已创建网络: $infra_network"
  fi
}

add_service() {
  local svc="$1"
  if [[ " $SELECTED_SERVICES " != *" $svc "* ]]; then
    SELECTED_SERVICES+=" $svc"
  fi
}

add_group_kafka() {
  add_service zookeeper
  add_service kafka
  add_service kafka-ui
}

add_group_redis() {
  add_service redis
  add_service redisinsight
}

add_group_flink() {
  add_service jobmanager
  add_service taskmanager
}

add_group_spark() {
  add_service spark-master
  add_service spark-worker
  add_service spark-client
}

add_group_minio() {
  add_service minio
  add_service minio-mc
}

add_group_starrocks() {
  add_service starrocks
  add_service starrocks-init
}

add_group_airflow() {
  add_service airflow-postgres
  add_service airflow-init
  add_service airflow-webserver
  add_service airflow-scheduler
}

add_group_observability() {
  add_service prometheus
  add_service loki
  add_service grafana
  add_service promtail
  add_service kafka-exporter
}

has_service() {
  local svc="$1"
  [[ " $SELECTED_SERVICES " == *" $svc "* ]]
}

main() {
  if [[ $# -eq 0 ]]; then
    usage
    exit 1
  fi

  local build=false
  SELECTED_SERVICES=""

  for arg in "$@"; do
    case "$arg" in
      ingest)
        add_group_redis
        add_group_kafka
        add_service worker-app
        ;;
      stream)
        add_group_redis
        add_group_kafka
        add_service postgres
        add_service clickhouse
        add_group_flink
        add_service worker-app
        ;;
      batch)
        add_group_spark
        ;;
      s)
        add_group_starrocks
        ;;
      m)
        add_group_minio
        ;;
      o)
        add_group_observability
        ;;
      a)
        add_group_airflow
        ;;
      w)
        add_service worker-app
        ;;
      cp)
        add_service control-plane-app
        ;;
      bd|--build)
        build=true
        ;;
      k)
        add_group_kafka
        ;;
      c)
        add_service clickhouse
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        echo "未知关键词: $arg" >&2
        usage
        exit 1
        ;;
    esac
  done

  if [[ -z "$SELECTED_SERVICES" ]]; then
    echo "未选择任何服务" >&2
    usage
    exit 1
  fi

  ensure_infra_network

  local steps=(
    "zookeeper"
    "kafka"
    "kafka-ui"
    "redis redisinsight"
    "postgres"
    "clickhouse"
    "jobmanager"
    "taskmanager"
    "worker-app"
    "control-plane-app"
    "minio"
    "minio-mc"
    "starrocks"
    "starrocks-init"
    "spark-master"
    "spark-worker"
    "spark-client"
    "airflow-postgres"
    "airflow-init"
    "airflow-webserver"
    "airflow-scheduler"
    "prometheus"
    "loki"
    "grafana"
    "promtail"
    "kafka-exporter"
  )

  for step in "${steps[@]}"; do
    local to_start=()
    for svc in $step; do
      if has_service "$svc"; then
        to_start+=("$svc")
      fi
    done
    if [[ ${#to_start[@]} -gt 0 ]]; then
      if [[ "$build" == "true" ]]; then
        compose_cmd build "${to_start[@]}"
      fi
      compose_cmd up -d "${to_start[@]}"
    fi
  done

  echo "已启动服务: ${SELECTED_SERVICES# }"
}

main "$@"
