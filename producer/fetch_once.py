import requests
import time

from datetime import datetime

STATUS_URL = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_status.json"
MY_STATION_ID = 214134714


def main():
    response = requests.get(STATUS_URL, timeout=10)
    response.raise_for_status()
    payload = response.json()

    stations = payload["data"]["stations"]
    print(f"Total stations: {len(stations)}")

    for station in stations:
        if station["station_id"] == MY_STATION_ID:
            print(f"Bikes available: {station['num_bikes_available']}")
            print(f"E-bikes available: {station['num_bikes_available_types'][1]['ebike']}")
            print(f"Docks available: {station['num_docks_available']}")
            print(f"Last reported: {datetime.fromtimestamp(station['last_reported'])}")
            print(f"Fetched at: {datetime.now()}")
            break

if __name__ == "__main__":
    while True:
        main()
        time.sleep(5)
