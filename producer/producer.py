import json
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer

STATUS_URL = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_status.json"
TOPIC = "velib.status.raw"
POLL_INTERVAL_SECONDS = 60

producer = Producer({"bootstrap.servers": "localhost:9092"})


def fetch_stations():
    response = requests.get(STATUS_URL, timeout=10)
    response.raise_for_status()
    payload = response.json()
    return payload["data"]["stations"]


def send_stations(stations):
    ingestion_time = datetime.now(timezone.utc).isoformat()

    for station in stations:
        station["ingestion_time"] = ingestion_time
        producer.produce(
            TOPIC,
            key=str(station["station_id"]),
            value=json.dumps(station),
        )

    producer.flush()


def main():
    stations = fetch_stations()
    send_stations(stations)
    print(f"{datetime.now()}  sent {len(stations)} messages")


if __name__ == "__main__":
    while True:
        main()
        time.sleep(POLL_INTERVAL_SECONDS)