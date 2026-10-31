WITH snapshots AS (
    SELECT COUNT(DISTINCT ingestion_time) AS total_snapshots
    FROM {{ source('velib', 'station_status_bronze') }}
),
availability AS (
    SELECT
        station_id,
        COUNT(*) AS samples,
        ROUND(AVG(num_bikes_available), 1) AS avg_bikes,
        ROUND(AVG(CASE WHEN num_bikes_available = 0 THEN 1.0 ELSE 0.0 END) * 100, 1) AS empty_pct,
        ROUND(AVG(CASE WHEN num_docks_available = 0 THEN 1.0 ELSE 0.0 END) * 100, 1) AS full_pct
    FROM {{ source('velib', 'station_status_bronze') }}
    WHERE is_renting = 1
    GROUP BY station_id
)
SELECT
    d.station_id,
    d.station_name,
    d.capacity,
    a.samples,
    ROUND(a.samples * 100.0 / s.total_snapshots, 1) AS coverage_pct,
    a.avg_bikes,
    a.empty_pct,
    a.full_pct
FROM availability a
JOIN {{ ref('dim_station') }} d
    ON a.station_id = d.station_id
CROSS JOIN snapshots s