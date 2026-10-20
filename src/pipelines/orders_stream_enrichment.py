
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
    .appName("OrdersStreamEnrichment")
    .config("spark.sql.shuffle.partitions", "3")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 1. Define the input JSON schema
order_schema = StructType([
    StructField("order_id", LongType()),
    StructField("customer_id", LongType()),
    StructField("amount", StringType()),
    StructField("status", StringType()),
    StructField("created_at", StringType()),
])

# 2. Read Kafka as a continuous stream
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "orders.raw")
    .option("startingOffsets", "latest")
    .load()
)

# 3. Parse JSON and keep Kafka metadata
parsed_df = (
    raw_df
    .select(
        F.from_json(
            F.col("value").cast("string"),
            order_schema
        ).alias("order"),
        F.col("partition").alias("source_partition"),
        F.col("offset").alias("source_offset"),
    )
    .select(
        "order.*",
        "source_partition",
        "source_offset",
    )
)

# 4. Validate and enrich orders
enriched_df = (
    parsed_df
    .withColumn(
        "amount_decimal",
        F.col("amount").cast(DecimalType(12, 2))
    )
    .withColumn(
        "created_at_epoch_ms",
        F.col("created_at").cast("long")
    )
    .filter(
        F.col("order_id").isNotNull()
        & F.col("customer_id").isNotNull()
        & F.col("amount_decimal").isNotNull()
        & (F.col("amount_decimal") > 0)
        & F.col("created_at_epoch_ms").isNotNull()
        & F.col("status").isin(
            "completed", "pending", "cancelled"
        )
    )
    .withColumn(
        "amount_category",
        F.when(
            F.col("amount_decimal") >= 500,
            "high_value"
        ).otherwise("standard")
    )
    .withColumn(
        "processed_at",
        F.current_timestamp()
    )
)

# 5. Convert enriched records into Kafka messages
output_df = enriched_df.select(
    F.col("order_id").cast("string").alias("key"),
    F.to_json(
        F.struct(
            "order_id",
            "customer_id",
            F.col("amount_decimal").alias("amount"),
            "status",
            "amount_category",
            "created_at_epoch_ms",
            "processed_at",
            "source_partition",
            "source_offset",
        )
    ).alias("value")
)

# 6. Start continuous streaming
query = (
    output_df.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("topic", "orders.enriched")
    .option(
    "checkpointLocation",
    "/opt/spark/checkpoints/orders_stream_enrichment_live_v2"
)
    .outputMode("append")
    .trigger(processingTime="10 seconds")
    .start()
)

print("Streaming started: orders.raw -> orders.enriched")

query.awaitTermination()
