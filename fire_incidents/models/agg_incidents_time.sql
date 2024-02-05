SELECT
    t.year,
    t.month,
    COUNT(DISTINCT f.incident_number) AS total_incidents,
    SUM(f.suppression_units) AS total_suppression_units,
    SUM(f.suppression_personnel) AS total_suppression_personnel
FROM {{ ref('fact_incidents') }} f
JOIN {{ ref('dim_time') }} t ON f.time_id = t.date
GROUP BY t.year, t.month