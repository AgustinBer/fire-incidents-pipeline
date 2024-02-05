SELECT
    ROW_NUMBER() OVER (ORDER BY primary_situation) AS incident_type_id,
    primary_situation
FROM (
    SELECT DISTINCT primary_situation
    FROM {{ ref('fire_incidents_staging') }}
) AS incident_types
