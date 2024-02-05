-- models/dim_location.sql
WITH source AS (
    SELECT
        address,
        city,
        zipcode,
        battalion,
        station_area,
        neighborhood_district,
        supervisor_district,
        point
    FROM {{ ref('fire_incidents_staging') }}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY address, city) AS location_id,
    address,
    city,
    zipcode,
    battalion,
    station_area,
    neighborhood_district,
    supervisor_district,
    point
FROM source
GROUP BY address, city, zipcode, battalion, station_area, neighborhood_district, supervisor_district, point
