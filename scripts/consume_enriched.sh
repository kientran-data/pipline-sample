
#!/usr/bin/env bash
set -e

export MSYS_NO_PATHCONV=1
cd "$(dirname "$0")/.."

echo "Consuming new messages from orders.enriched..."

docker compose exec kafka bash -lc \
  '/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic orders.enriched \
  --consumer-property auto.offset.reset=latest \
  --timeout-ms 60000'
