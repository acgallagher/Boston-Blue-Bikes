{{ config(materialized='table') }}

SELECT  
    info.region_name, 
    info.capacity, 
    info.station_type, 
    info.short_name, 
    info.station_id, 
    info.station_name,
    ST_GEOGPOINT(info.station_longitude, info.station_latitude) station_location,

    status.station_status,
    status.num_docks_available,
    status.num_docks_disabled,
    (status.num_docks_disabled / info.capacity) * 100 AS docks_disabled_pct,
    status.num_bikes_available,
    (status.num_bikes_available / info.capacity)  * 100 AS bikes_available_pct,
    status.num_bikes_disabled,
    status.num_ebikes_available,
FROM {{ ref('stationdata_info_most_recent') }} AS info
JOIN {{ ref('stationdata_status_most_recent') }} AS status
    ON status.station_id = info.station_id 
WHERE status.is_renting = true AND info.capacity > 0