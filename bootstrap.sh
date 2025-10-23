#!/usr/bin/env bash
set -euo pipefail

NETWORK=system_network

# wait for service ready
wait_for_service() {
  local host=$1
  local port=$2
  echo "⏳ Waiting for $host:$port..."
  while ! (echo > /dev/tcp/$host/$port) >/dev/null 2>&1; do
    sleep 2
  done
  echo "✅ $host:$port is ready!"
}

# wait for localhost port
wait_for_local_port(){
  local port=$1
  local timeout=${2:-60} # seconds
  local sleep_sec=2
  local i=0

  info "Waiting for localhost:${port} (timeout ${timeout}s)..."
  while ! (timeout 1 bash -c "</dev/tcp/127.0.0.1/${port}" >/dev/null 2>&1); do
    i=$((i+sleep_sec))
    if [ "$i" -ge "$timeout" ]; then
      warn "timeout waiting for localhost:${port}"
      return 1
    fi
    sleep $sleep_sec
  done
  info "localhost:${port} is open"
  return 0
}

# 1. ensure docker network
if ! docker network ls | grep -qw "$NETWORK"; then
  echo "Creating docker network $NETWORK"
  docker network create "$NETWORK"
fi

# 2. grant exec permission for script.sh
chmod +x hadoop-cluster/init-datanode.sh || true
chmod +x hadoop-cluster/start-hdfs.sh || true
chmod +x metastore/start-metastore.sh || true
chmod +x orchestration/deploy/airflow/start-airflow.sh || true

# 3. bring up modules theo thứ tự
echo "🚀 Starting Hadoop cluster..."
(cd hadoop-cluster && docker compose up -d && wait_for_service tuankiet170-master 9870 && docker exec tuankiet170-master hdfs dfs -mkdir -p /user/ntk/warehouse)

echo "🚀 Starting Metastore..."
(cd metastore && docker compose run postgres-metastore && wait_for_service postgres-metastore 5432 && docker compose run hive-metastore)

wait_for_service hive-metastore 9083

echo "🚀 Starting Spark/Iceberg..."
(cd spark_iceberg && docker compose up -d)

echo "🚀 Starting Trino..."
(cd trino && docker compose up -d)

wait_for_service trino 8082

echo "🚀 Starting Superset..."
(cd superset && docker compose up -d)

wait_for_service superset 8088

echo "🚀 Starting Airflow Init..."
(cd orchestration && docker compose --profile init run --rm airflow-init)


# 4. initializations (superset, airflow)
echo "⚙ Initializing Superset..."
docker exec -it superset superset db upgrade
docker exec -it superset superset fab create-admin \
    --username admin --firstname Admin --lastname User \
    --email admin@example.com --password admin
docker exec -it superset superset init

echo "⚙ Initializing Airflow..."
(cd orchestration && docker compose up -d)

echo "🎉 All services up and initialized!"
