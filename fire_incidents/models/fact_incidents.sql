SELECT
    i.incident_number,
    DATE(i.incident_date) AS incident_date,
    lt.location_id,
    it.incident_type_id,
    t.date AS time_id,
    i.suppression_units,
    i.suppression_personnel,
    i.ems_units,
    i.ems_personnel,
    i.other_units,
    i.other_personnel,
    i.estimated_property_loss,
    i.estimated_contents_loss,
    i.fire_fatalities,
    i.fire_injuries,
    i.civilian_fatalities,
    i.civilian_injuries
FROM {{ ref('fire_incidents_staging') }} i
LEFT JOIN {{ ref('dim_location') }} lt ON i.address = lt.address AND i.city = lt.city AND i.zipcode = lt.zipcode
LEFT JOIN {{ ref('dim_incident_type') }} it ON i.primary_situation = it.primary_situation
LEFT JOIN {{ ref('dim_time') }} t ON DATE(i.incident_date) = t.date
