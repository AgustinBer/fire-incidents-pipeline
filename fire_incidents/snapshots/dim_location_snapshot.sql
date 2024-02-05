{% snapshot dim_location_snapshot %}

{{ 
    config(
        unique_key='location_id',
        strategy='check',
        check_cols=['city','batallion'],
        target_schema='snapshot'
    ) 
        
}}

SELECT
    *
FROM {{ ref('dim_location') }}

{% endsnapshot %}

