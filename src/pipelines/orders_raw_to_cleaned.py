
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
)

spark = (
    SparkSession.builder
    .appName("OrdersRawToCleaned")
    .config("spark.sql.shuffle.partitions", "3")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 1. Read raw events from Kafka
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "orders.raw")
    .option("startingOffsets", "earliest")
    .load()
)

# 2. Define the schema of NiFi's JSON messages
order_schema = StructType([
    StructField("order_id", LongType()),
    StructField("customer_id", LongType()),
    StructField("amount", StringType()),
    StructField("status", StringType()),
    StructField("created_at", StringType()),
])

# 3. Parse Kafka message values
parsed_df = (
    raw_df
    .selectExpr("CAST(value AS STRING) AS json_value")
    .select(
        F.from_json("json_value", order_schema)
        .alias("order")
    )
    .select("order.*")
)

# 4. Transform and validate
cleaned_df = (
    parsed_df
    .withColumn(
        "amount_decimal",
        F.col("amount").cast("decimal(12,2)")
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
        & (F.col("status") == "completed")
    )
)

# 5. Serialize cleaned orders for Kafka
output_df = cleaned_df.select(
    F.col("order_id").cast("string").alias("key"),
    F.to_json(
        F.struct(
            "order_id",
            "customer_id",
            F.col("amount_decimal").alias("amount"),
            "status",
            "created_at_epoch_ms",
        )
    ).alias("value")
)

# 6. Write cleaned events to Kafka
query = (
    output_df.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("topic", "orders.cleaned")
    .option(
        "checkpointLocation",
        "/opt/spark/checkpoints/orders_raw_to_cleaned"
    )
    .outputMode("append")
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()
spark.stop()
