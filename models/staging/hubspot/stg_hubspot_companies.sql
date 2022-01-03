WITH source as (
    SELECT * FROM {{source('hubspot', 'contact')}}
), 
renamed as (
    SELECT 
    concat('hubspot-', replace(lower(business_name), ' ','-')) as business_id,
    business_name as name
    FROM source
    GROUP BY name
)
SELECT * FROM renamed