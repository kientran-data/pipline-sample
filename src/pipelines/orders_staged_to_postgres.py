
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    DecimalType,
)

spark = (
    SparkSession.builder
    .appName("OrdersStagedToPostgres")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 1. Define the Kafka message schema
schema = StructType([
    StructField("order_id", LongType()),
    StructField("customer_id", LongType()),
    StructField("amount", DecimalType(12, 2)),
    StructField("status", StringType()),
    StructField("created_at_epoch_ms", LongType()),
])

# 2. Consume Kafka events
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "orders.staged")
    .option("startingOffsets", "earliest")
    .load()
)

# 3. Parse JSON messages
parsed_df = (
    raw_df
    .select(
        F.from_json(
            F.col("value").cast("string"),
            schema
        ).alias("order")
    )
    .select("order.*")
)

# 4. Validate orders
valid_df = parsed_df.filter(
    F.col("order_id").isNotNull()
    & F.col("customer_id").isNotNull()
    & F.col("amount").isNotNull()
    & (F.col("amount") > 0)
    & F.col("created_at_epoch_ms").isNotNull()
    & (F.col("status") == "completed")
)

# 5. Write each micro-batch to PostgreSQL
def write_to_postgres(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    (
        batch_df.coalesce(1)
        .write
        .format("jdbc")
        .option(
            "url",
            f"jdbc:postgresql://postgres:5432/"
            f"{os.environ['POSTGRES_DB']}"
        )
        .option("dbtable", "public.orders_processed_sink")
        .option("user", os.environ["POSTGRES_USER"])
        .option("password", os.environ["POSTGRES_PASSWORD"])
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )

    print(f"Successfully wrote micro-batch {batch_id}")


# 6. Start Structured Streaming
query = (
    valid_df.writeStream
    .foreachBatch(write_to_postgres)
    .option(
        "checkpointLocation",
        "/opt/spark/checkpoints/orders_staged_to_postgres"
    )
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()
spark.stop()
