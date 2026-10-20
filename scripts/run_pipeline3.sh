
#!/usr/bin/env bash
set -e

# Fix Git Bash path conversion on Windows
export MSYS_NO_PATHCONV=1

# Navigate to the project root
cd "$(dirname "$0")/.."

echo "=================================="
echo " Pipeline 3: Kafka -> Spark -> Kafka"
echo "=================================="

# Start required Docker services
echo "Starting Kafka and Spark..."

docker compose up -d \
  kafka \
  spark-master \
  spark-worker

# Display container status
docker compose ps

echo "Starting Spark Structured Streaming..."

# Submit Spark application
docker compose exec spark-master sh -c \
  'mkdir -p /tmp/spark-ivy && \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-master \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.jars.ivy=/tmp/spark-ivy \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 \
  /opt/spark/work-dir/src/pipelines/orders_stream_enrichment.py'
