{{ config(materialized='table') }}

SELECT
    trips.tripduration, 
    trips.starttime, 
    trips.stoptime, 
    trips.bikeid, 
    trips.usertype,
    trips.start_station_id, 
    stations_start.station_name AS start_station_name, 
    stations_start.region_name AS start_region,
    ST_GEOGPOINT(stations_start.station_longitude, stations_start.station_latitude) AS start_station_location,
    trips.end_station_id,
    stations_end.region_name AS end_region,
    stations_end.station_name AS end_station_name,
    ST_GEOGPOINT(stations_end.station_longitude, stations_end.station_latitude) AS end_station_location,
    ST_MAKELINE(ST_GEOGPOINT(stations_start.station_longitude, stations_start.station_latitude),
                ST_GEOGPOINT(stations_end.station_longitude, stations_end.station_latitude)) AS trip_path,
    DATE(starttime) AS Date
FROM {{ ref('trips') }} AS trips
JOIN {{ ref('stationdata_info_most_recent')}} AS stations_start
    ON stations_start.station_id = trips.start_station_id
JOIN {{ ref('stationdata_info_most_recent')}} AS stations_end
    ON stations_end.station_id = trips.end_station_id