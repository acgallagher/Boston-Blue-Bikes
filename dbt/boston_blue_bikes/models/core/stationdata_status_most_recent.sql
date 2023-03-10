{{ config(materialized='table') }}


SELECT
    is_renting,
    last_reported,
    eightd_has_available_keys,
    num_docks_available,
    is_returning,
    station_status,
    num_bikes_available,
    station_id,
    num_bikes_disabled,
    is_installed,
    num_ebikes_available,
    num_docks_disabled
FROM {{ source('staging', 'stationdata_status') }}
WHERE time_prepared = (
    SELECT MAX(time_prepared)
    FROM {{ source('staging', 'stationdata_status') }}
)