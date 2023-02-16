{{ config(materialized='table') }}

SELECT 
    -- index
    -- CAST(__index_level_0__ AS INTEGER) AS trip_id,

    -- timestamps
    CAST(starttime AS TIMESTAMP) AS starttime,
    CAST(stoptime AS TIMESTAMP) AS stoptime,

    -- station details
    CAST(start_station_id AS INTEGER) AS start_station_id,
    CAST(end_station_id AS INTEGER) AS end_station_id,

    -- trip details
    CAST(tripduration AS INTEGER) AS tripduration,
    CAST(bikeid AS STRING) AS bike_id,

    -- user details
    CAST(usertype AS STRING) AS usertype,
    CAST(birth_year AS NUMERIC) AS birth_year,
    CAST(gender AS NUMERIC) AS gender,
    CAST(postal_code AS NUMERIC) AS postal_code

FROM {{ source('staging', 'tripdata') }} 
