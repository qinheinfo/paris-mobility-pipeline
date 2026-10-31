SELECT
    station_id,
    coverage_pct,
    empty_pct,
    full_pct
FROM {{ ref('mart_station_availability') }}
WHERE coverage_pct NOT BETWEEN 0 AND 100
   OR empty_pct NOT BETWEEN 0 AND 100
   OR full_pct NOT BETWEEN 0 AND 100