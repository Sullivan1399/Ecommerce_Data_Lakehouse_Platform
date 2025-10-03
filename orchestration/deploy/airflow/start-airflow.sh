#!/usr/bin/env bash
set -euo pipefail

ACTION=${1:-db_init}

# env defaults
export AIRFLOW_HOME=${AIRFLOW_HOME:-/opt/airflow}
export PATH=$PATH

# wait for Postgres ready
wait_for_postgres() {
  echo "Waiting for Postgres..."
  until pg_isready -h "${AIRFLOW__CORE__SQL_ALCHEMY_CONN_HOST:-airflow-postgres}" -p 5432 -U airflow >/dev/null 2>&1; do
    echo "Postgres not ready yet..."
    sleep 1
  done
  echo "Postgres ready."
}

case "$ACTION" in
  db_init)
    # initialize DB, create user, create connection to Trino (optional)
    # Ensure tools available
    apt-get update -qq || true
    apt-get install -y -qq postgresql-client || true

    wait_for_postgres

    airflow db upgrade

    airflow users create \
      --username admin \
      --password admin \
      --firstname Admin \
      --lastname User \
      --role Admin \
      --email admin@example.com || true

    # optional: create connection to Trino by CLI
    airflow connections add trino_default \
      --conn-uri "trino://trino@trino:8080" || true

    echo "DB init done."
    ;;
  *)
    echo "Unknown action: $ACTION"
    ;;
esac
