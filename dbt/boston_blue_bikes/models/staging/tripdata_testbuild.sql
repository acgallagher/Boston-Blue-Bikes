{{ config(materialized='table') }}

SELECT * 
FROM {{ source('staging', 'tripdata') }}
LIMIT 100