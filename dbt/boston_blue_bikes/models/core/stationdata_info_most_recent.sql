{{ config(materialized='table') }}


SELECT
    regions.region_name, 
    station_info.capacity, 
    station_info.station_type, 
    station_info.short_name, 
    station_info.station_id, 
    station_info.station_name, 
    station_info.station_longitude,
    station_info.station_latitude
FROM {{ source('staging', 'stationdata_information') }} AS station_info
JOIN {{ ref('regions') }} AS regions
    ON regions.region_id = station_info.region_id
WHERE time_prepared = (
    SELECT MAX(time_prepared)
    FROM {{ source('staging', 'stationdata_information') }}
    )