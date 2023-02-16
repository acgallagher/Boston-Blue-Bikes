{{ config(materialized='table') }}

SELECT 
    *
FROM {{ source('staging', 'stationdata') }} 
