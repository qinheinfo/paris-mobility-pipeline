# Data Dictionary

Source: Vélib' Métropole open data API (GBFS 1.0). Updated every minute, no API key required.

## station_status (real-time station status)

Endpoint: https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_status.json

**Grain:** one row per station per API snapshot.

| Field | Type | Example | Description |
|---|---|---|---|
| station_id | integer | 214134714 | Unique station identifier; join key with station_information |
| num_bikes_available | integer | 2 | Total number of bikes currently available for rental |
| numBikesAvailable | integer | 2 | Duplicate of num_bikes_available (camelCase variant) |
| num_bikes_available_types | list | [{"mechanical": 0}, {"ebike": 2}] | Available bikes broken down by type (mechanical / e-bike) |
| num_docks_available | integer | 24 | Number of empty docks available for returning a bike |
| numDocksAvailable | integer | 24 | Duplicate of num_docks_available (camelCase variant) |
| is_installed | integer (0/1) | 1 | Whether the station is installed and in service |
| is_renting | integer (0/1) | 1 | Whether bikes can currently be rented from this station |
| is_returning | integer (0/1) | 1 | Whether bikes can currently be returned to this station |
| last_reported | integer (Unix timestamp) | 1793211473 | Time of the station's last status update (event time) |
| stationCode | string | "42301" | Public-facing station code used by Vélib'; stored as a string identifier |
| station_opening_hours | null | null | Opening hours; null in observed data (likely unused) |


## station_information (static station metadata)

Endpoint: https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_information.json

**Grain:** one row per station (slowly changing reference data).

| Field | Type | Example | Description |
|---|---|---|---|
| station_id | integer | 214134714 | Unique station identifier; join key with station_status |
| stationCode | string | "42301" | Public-facing station code used by Vélib'; stored as a string identifier |
| name | string | "Gare Arcueil - Cachan" | Human-readable station name |
| lat | float | 48.799824 | Latitude in WGS84 (decimal degrees) |
| lon | float | 2.328171 | Longitude in WGS84 (decimal degrees) |
| capacity | integer | 26 | Total number of docks; expected to equal num_bikes_available + num_docks_available in station_status (excluding disabled docks) |
| rental_methods | list | ["CREDITCARD"] | Payment methods accepted at the station kiosk |
| station_opening_hours | null | null | Opening hours; null in observed data (likely unused) |