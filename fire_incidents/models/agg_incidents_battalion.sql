SELECT
    l.battalion,
    COUNT(DISTINCT f.incident_number) AS total_incidents,
    SUM(f.suppression_units) AS total_suppression_units,
    SUM(f.suppression_personnel) AS total_suppression_personnel
FROM {{ ref('fact_incidents') }} f
JOIN {{ ref('dim_location') }} l ON f.location_id = l.location_id
GROUP BY l.battalion