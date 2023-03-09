{{ config(materialized='table') }}

SELECT
    tripduration, 
    starttime, 
    stoptime, 
    bikeid, 
    usertype,
    start_station_id, 
    end_station_id
FROM {{ source('core', 'tripdata') }}