WITH date_series AS (
    SELECT DISTINCT incident_date::DATE AS date
    FROM {{ ref('fire_incidents_staging') }}
)

SELECT
    date,
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(MONTH FROM date) AS month,
    EXTRACT(DAY FROM date) AS day,
    EXTRACT(DOW FROM date) AS day_of_week,
    EXTRACT(DOY FROM date) AS day_of_year,
    EXTRACT(QUARTER FROM date) AS quarter
FROM date_series