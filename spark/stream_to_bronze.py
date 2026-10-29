from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

KAFKA_BOOTSTRAP_SERVERS = "kafka:19092"
TOPIC = "velib.status.raw"
TARGET_TABLE = "demo.velib.station_status_bronze"
CHECKPOINT_PATH = "/home/iceberg/checkpoints/station_status_bronze"

STATUS_SCHEMA = StructType([
    StructField("station_id", LongType()),
    StructField("num_bikes_available", IntegerType()),
    StructField("num_docks_available", IntegerType()),
    StructField("is_installed", IntegerType()),
    StructField("is_renting", IntegerType()),
    StructField("is_returning", IntegerType()),
    StructField("last_reported", LongType()),
    StructField("stationCode", StringType()),
    StructField("ingestion_time", StringType()),
])


def create_table(spark):
    spark.sql("CREATE DATABASE IF NOT EXISTS demo.velib")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
            station_id BIGINT,
            num_bikes_available INT,
            num_docks_available INT,
            is_installed INT,
            is_renting INT,
            is_returning INT,
            last_reported BIGINT,
            stationCode STRING,
            ingestion_time STRING,
            kafka_partition INT,
            kafka_offset BIGINT
        ) USING iceberg
    """)


def main():
    spark = SparkSession.builder.appName("stream_to_bronze").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    create_table(spark)

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed = (
        raw.select(
            from_json(col("value").cast("string"), STATUS_SCHEMA).alias("s"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
        )
        .select("s.*", "kafka_partition", "kafka_offset")
    )

    query = (
        parsed.writeStream.format("iceberg")
        .outputMode("append")
        .trigger(processingTime="1 minute")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .toTable(TARGET_TABLE)
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()