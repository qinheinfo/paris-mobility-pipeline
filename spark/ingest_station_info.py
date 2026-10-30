import json
from urllib.request import Request, urlopen

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

INFO_URL = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_information.json"
TARGET_TABLE = "demo.velib.station_info"


def fetch_stations():
    request = Request(INFO_URL, headers={"User-Agent": "paris-mobility-pipeline/1.0"})
    with urlopen(request, timeout=30) as response:
        payload = json.load(response)
    return payload["data"]["stations"]


def main():
    spark = SparkSession.builder.appName("ingest_station_info").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    stations = fetch_stations()

    rows = [
        (
            station["station_id"],
            station["stationCode"],
            station["name"],
            station["lat"],
            station["lon"],
            station["capacity"],
        )
        for station in stations
    ]

    df = spark.createDataFrame(
        rows,
        "station_id BIGINT, station_code STRING, name STRING, lat DOUBLE, lon DOUBLE, capacity INT",
    )
    df = df.withColumn("loaded_at", current_timestamp())

    spark.sql("CREATE DATABASE IF NOT EXISTS demo.velib")
    df.writeTo(TARGET_TABLE).createOrReplace()

    print(f"Wrote {df.count()} stations to {TARGET_TABLE}")

    spark.stop()


if __name__ == "__main__":
    main()