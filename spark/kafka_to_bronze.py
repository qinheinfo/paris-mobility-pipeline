from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

KAFKA_BOOTSTRAP_SERVERS = "kafka:19092"
TOPIC = "velib.status.raw"
TARGET_TABLE = "demo.velib.station_status_bronze"

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


def main():
    spark = SparkSession.builder.appName("kafka_to_bronze").getOrCreate()
    spark = SparkSession.builder.appName("kafka_to_bronze").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed = (
        raw.select(from_json(col("value").cast("string"), STATUS_SCHEMA).alias("s"))
        .select("s.*")
    )

    spark.sql("CREATE DATABASE IF NOT EXISTS demo.velib")
    parsed.writeTo(TARGET_TABLE).createOrReplace()

    count = spark.table(TARGET_TABLE).count()
    print(f"Wrote {count} rows to {TARGET_TABLE}")

    spark.stop()


if __name__ == "__main__":
    main()