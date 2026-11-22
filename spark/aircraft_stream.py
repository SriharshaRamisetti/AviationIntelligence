
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    current_date,
    coalesce
)


# =========================================================
# CONFIGURATION
# =========================================================

KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_TOPIC = "aircraft-positions"

BRONZE_OUTPUT_PATH = (
    "/opt/project/storage/bronze/aircraft_positions"
)

CHECKPOINT_PATH = (
    "/tmp/aviation-checkpoints/bronze_aircraft_positions"
)


# =========================================================
# SPARK SESSION
# =========================================================

spark = (
    SparkSession.builder
    .appName("AviationIntelligenceBronzeIngestion")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "3")
    .config("spark.sql.streaming.metricsEnabled", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# =========================================================
# READ RAW EVENTS FROM KAFKA
# =========================================================

kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .option("maxOffsetsPerTrigger", "5000")
    .load()
)


# =========================================================
# BRONZE LAYER - RAW DATA PRESERVATION
# =========================================================

bronze_stream = (
    kafka_stream
    .select(
        col("key").cast("string").alias("raw_key"),

        col("value").cast("string").alias("raw_value"),

        col("topic").alias("source_topic"),

        col("partition").alias("source_partition"),

        col("offset").alias("source_offset"),

        col("timestamp").alias("kafka_timestamp"),

        col("timestampType").alias("kafka_timestamp_type")
    )
    .withColumn(
        "ingestion_date",
        coalesce(
            to_date(col("kafka_timestamp")),
            current_date()
        )
    )
)


# =========================================================
# WRITE BRONZE DATA AS PARQUET
# =========================================================

query = (
    bronze_stream.writeStream
    .format("parquet")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .partitionBy("ingestion_date")
    .trigger(processingTime="10 seconds")
    .start(BRONZE_OUTPUT_PATH)
)


# =========================================================
# KEEP STREAM ALIVE
# =========================================================

print("========================================")
print("AVIATION INTELLIGENCE - BRONZE LAYER")
print("========================================")
print("Kafka Topic:", KAFKA_TOPIC)
print("Output:", BRONZE_OUTPUT_PATH)
print("Checkpoint:", CHECKPOINT_PATH)
print("Status: Streaming raw aircraft events...")
print("========================================")

query.awaitTermination()
