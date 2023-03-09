{{ config(materialized='table') }}

SELECT
    *, 
    DATE(EXTRACT(YEAR FROM starttime), EXTRACT(MONTH FROM starttime), 1) Date
FROM {{ ref('trips') }}