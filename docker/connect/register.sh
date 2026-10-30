#!/bin/sh
# Registers (idempotently) the Debezium PostgreSQL connector with Kafka Connect.
set -eu
CONNECT_URL="${CONNECT_URL:-http://connect:8083}"

until curl -fsS "$CONNECT_URL/connectors" >/dev/null 2>&1; do
  echo "waiting for Kafka Connect..."; sleep 3
done

cat > /tmp/connector.json <<EOF
{
  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
  "database.hostname": "postgres",
  "database.port": "5432",
  "database.user": "${POSTGRES_USER:-settlelake}",
  "database.password": "${POSTGRES_PASSWORD:-settlelake}",
  "database.dbname": "payments",
  "topic.prefix": "payments",
  "table.include.list": "public.payment_events",
  "plugin.name": "pgoutput",
  "slot.name": "settlelake_slot",
  "publication.autocreate.mode": "filtered",
  "snapshot.mode": "initial",
  "tombstones.on.delete": "false",
  "key.converter": "org.apache.kafka.connect.json.JsonConverter",
  "key.converter.schemas.enable": "false",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "false"
}
EOF

# PUT /config is create-or-update, so re-running is safe.
curl -fsS -X PUT -H "Content-Type: application/json" \
  --data @/tmp/connector.json "$CONNECT_URL/connectors/payments-cdc/config" >/dev/null
echo "connector payments-cdc registered"

for i in $(seq 1 30); do
  state=$(curl -fsS "$CONNECT_URL/connectors/payments-cdc/status" | grep -o '"state":"[A-Z]*"' | head -1 || true)
  echo "connector status: $state"
  case "$state" in *RUNNING*) exit 0 ;; esac
  sleep 2
done
echo "connector did not reach RUNNING" >&2
exit 1
