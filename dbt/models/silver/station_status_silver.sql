WITH ordered AS (
    SELECT
        *,
        LAG(num_bikes_available) OVER w AS prev_bikes,
        LAG(num_docks_available) OVER w AS prev_docks,
        LAG(is_renting) OVER w AS prev_renting,
        LAG(is_returning) OVER w AS prev_returning
    FROM {{ source('velib', 'station_status_bronze') }}
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