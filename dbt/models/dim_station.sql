SELECT
    station_id,
    station_code,
    name AS station_name,
    lat,
    lon,
    capacity,
    loaded_at
FROM {{ source('velib', 'station_info') }}