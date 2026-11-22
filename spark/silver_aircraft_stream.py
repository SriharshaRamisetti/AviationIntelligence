
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    trim,
    lower,
    coalesce,
    timestamp_seconds,
    when
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    BooleanType,
    IntegerType
)

BRONZE_PATH = "/opt/project/storage/bronze/aircraft_positions"
SILVER_PATH = "/opt/project/storage/silver/aircraft_positions"

CHECKPOINT_PATH = (
    "/tmp/aviation-checkpoints/silver_aircraft_positions"
)

aircraft_schema = StructType([
    StructField("icao24", StringType(), True),
    StructField("callsign", StringType(), True),
    StructField("origin_country", StringType(), True),
    StructField("time_position", LongType(), True),
    StructField("last_contact", LongType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("baro_altitude_m", DoubleType(), True),
    StructField("on_ground", BooleanType(), True),
    StructField("velocity_m_s", DoubleType(), True),
    StructField("heading_deg", DoubleType(), True),
    StructField("vertical_rate_m_s", DoubleType(), True),
    StructField("geo_altitude_m", DoubleType(), True),
    StructField("squawk", StringType(), True),
    StructField("position_source", IntegerType(), True),
    StructField("aircraft_category", IntegerType(), True),
    StructField("snapshot_time", LongType(), True),
    StructField("ingestion_time", StringType(), True)
])

spark = (
    SparkSession.builder
    .appName("AviationIntelligenceSilver")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "3")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

bronze_schema = StructType([
    StructField("raw_key", StringType(), True),
    StructField("raw_value", StringType(), True),
    StructField("source_topic", StringType(), True),
    StructField("source_partition", IntegerType(), True),
    StructField("source_offset", LongType(), True),
    StructField("kafka_timestamp", 
                __import__("pyspark.sql.types", fromlist=["TimestampType"]).TimestampType(), True),
    StructField("kafka_timestamp_type", IntegerType(), True),
    StructField("ingestion_date",
                __import__("pyspark.sql.types", fromlist=["DateType"]).DateType(), True)
])

bronze_stream = (
    spark.readStream
    .schema(bronze_schema)
    .option("maxFilesPerTrigger", 100)
    .parquet(BRONZE_PATH)
)

parsed_stream = (
    bronze_stream
    .withColumn(
        "aircraft",
        from_json(col("raw_value"), aircraft_schema)
    )
)

silver_stream = (
    parsed_stream
    .select(
        lower(trim(col("aircraft.icao24"))).alias("icao24"),
        trim(col("aircraft.callsign")).alias("callsign"),
        col("aircraft.origin_country").alias("origin_country"),

        col("aircraft.time_position").alias("time_position"),
        col("aircraft.last_contact").alias("last_contact"),

        col("aircraft.longitude").alias("longitude"),
        col("aircraft.latitude").alias("latitude"),

        col("aircraft.baro_altitude_m").alias("baro_altitude_m"),
        col("aircraft.geo_altitude_m").alias("geo_altitude_m"),
        col("aircraft.on_ground").alias("on_ground"),

        col("aircraft.velocity_m_s").alias("velocity_m_s"),
        col("aircraft.heading_deg").alias("heading_deg"),
        col("aircraft.vertical_rate_m_s").alias("vertical_rate_m_s"),

        col("aircraft.squawk").alias("squawk"),
        col("aircraft.position_source").alias("position_source"),
        col("aircraft.aircraft_category").alias("aircraft_category"),

        col("aircraft.snapshot_time").alias("snapshot_time"),
        col("aircraft.ingestion_time").alias("ingestion_time"),

        col("source_topic"),
        col("source_partition"),
        col("source_offset"),
        col("kafka_timestamp"),
        col("ingestion_date")
    )
    .withColumn(
        "event_time",
        timestamp_seconds(
            coalesce(col("time_position"), col("snapshot_time"))
        )
    )
    .withColumn(
        "velocity_kmh",
        col("velocity_m_s") * 3.6
    )
    .withColumn(
        "baro_altitude_km",
        col("baro_altitude_m") / 1000
    )
    .withColumn(
        "callsign",
        when(col("callsign") == "", None).otherwise(col("callsign"))
    )
    .filter(
        col("icao24").rlike("^[0-9a-f]{6}$") &
        col("longitude").between(-180, 180) &
        col("latitude").between(-90, 90) &
        col("event_time").isNotNull()
    )
)

query = (
    silver_stream.writeStream
    .format("parquet")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .partitionBy("ingestion_date")
    .trigger(processingTime="10 seconds")
    .start(SILVER_PATH)
)

print("========================================")
print("AVIATION INTELLIGENCE - SILVER LAYER")
print("========================================")
print("Source:", BRONZE_PATH)
print("Output:", SILVER_PATH)
print("Status: Parsing and validating aircraft events")
print("========================================")

query.awaitTermination()
