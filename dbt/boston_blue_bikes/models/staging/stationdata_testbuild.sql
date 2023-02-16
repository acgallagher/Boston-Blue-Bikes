{{ config(materialized='table') }}

SELECT * 
FROM {{ source('staging', 'stationdata') }}
ORDER BY station_name DESC
