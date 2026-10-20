
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    DecimalType,
    StringType,
)

# 1. Initialize Spark
spark = (
    SparkSession.builder
    .appName("MinIOCsvToParquet")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 2. Configure Hadoop S3A for MinIO
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

hadoop_conf.set(
    "fs.s3a.impl",
    "org.apache.hadoop.fs.s3a.S3AFileSystem"
)
hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
hadoop_conf.set("fs.s3a.endpoint.region", "us-east-1")

hadoop_conf.set(
    "fs.s3a.aws.credentials.provider",
    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
)
hadoop_conf.set(
    "fs.s3a.access.key",
    os.environ["MINIO_ROOT_USER"]
)
hadoop_conf.set(
    "fs.s3a.secret.key",
    os.environ["MINIO_ROOT_PASSWORD"]
)

# 3. Define an explicit CSV schema
order_schema = StructType([
    StructField("order_id", LongType()),
    StructField("customer_id", LongType()),
    StructField("amount", DecimalType(12, 2)),
    StructField("status", StringType()),
    StructField("country", StringType()),
])

input_path = "s3a://de-raw/orders/orders.csv"
output_path = "s3a://de-processed/orders_parquet/"

# 4. Read CSV from MinIO
orders_df = (
    spark.read
    .option("header", "true")
    .option("mode", "FAILFAST")
    .schema(order_schema)
    .csv(input_path)
)

print("=== RAW ORDERS ===")
orders_df.show(truncate=False)
print(f"Raw records: {orders_df.count()}")

# 5. Clean and transform
cleaned_df = (
    orders_df
    .filter(
        F.col("order_id").isNotNull()
        & F.col("customer_id").isNotNull()
        & F.col("amount").isNotNull()
        & (F.col("amount") > 0)
        & (F.col("status") == "completed")
        & F.col("country").isNotNull()
        & (F.length(F.trim("country")) > 0)
    )
    .withColumn("country", F.upper(F.trim("country")))
    .withColumn("processed_at", F.current_timestamp())
)

print("=== CLEANED ORDERS ===")
cleaned_df.orderBy("order_id").show(truncate=False)

# 6. Write Parquet partitioned by country
(
    cleaned_df.write
    .mode("errorifexists")
    .partitionBy("country")
    .parquet(output_path)
)

print("Parquet written successfully!")

# 7. Read Parquet back to validate
result_df = spark.read.parquet(output_path)

result_count = result_df.count()

print("=== PARQUET VALIDATION ===")
result_df.orderBy("order_id").show(truncate=False)
print(f"Parquet records: {result_count}")

assert result_count == 4, (
    f"Expected 4 records, found {result_count}"
)

print("PIPELINE 4 VALIDATION PASSED")

spark.stop()
