{{ config(materialized='table') }}

SELECT
    *, 
    DATE(starttime) AS Date
FROM {{ ref('trips') }}