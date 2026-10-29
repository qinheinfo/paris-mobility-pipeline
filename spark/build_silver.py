from pyspark.sql import SparkSession

BRONZE_TABLE = "demo.velib.station_status_bronze"
SILVER_TABLE = "demo.velib.station_status_silver"

BUILD_SILVER_SQL = f"""
CREATE OR REPLACE TABLE {SILVER_TABLE}
USING iceberg
AS
WITH ordered AS (
    SELECT
        *,
        LAG(num_bikes_available) OVER w AS prev_bikes,
        LAG(num_docks_available) OVER w AS prev_docks,
        LAG(is_renting) OVER w AS prev_renting,
        LAG(is_returning) OVER w AS prev_returning
    FROM {BRONZE_TABLE}
    WINDOW w AS (PARTITION BY station_id ORDER BY ingestion_time)
)
SELECT
    station_id,
    stationCode AS station_code,
    num_bikes_available AS bikes_available,
    num_docks_available AS docks_available,
    is_installed = 1 AS is_installed,
    is_renting = 1 AS is_renting,
    is_returning = 1 AS is_returning,
    timestamp_seconds(last_reported) AS reported_at,
    CAST(ingestion_time AS TIMESTAMP) AS ingested_at
FROM ordered
WHERE prev_bikes IS NULL
   OR num_bikes_available != prev_bikes
   OR num_docks_available != prev_docks
   OR is_renting != prev_renting
   OR is_returning != prev_returning
"""


def main():
    spark = SparkSession.builder.appName("build_silver").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    spark.sql(BUILD_SILVER_SQL)

    bronze_count = spark.table(BRONZE_TABLE).count()
    silver_count = spark.table(SILVER_TABLE).count()
    kept_ratio = silver_count / bronze_count

    print(f"Bronze rows: {bronze_count}")
    print(f"Silver rows: {silver_count}")
    print(f"Kept {kept_ratio:.1%} of rows, removed {1 - kept_ratio:.1%} as duplicates")

    spark.stop()


if __name__ == "__main__":
    main()