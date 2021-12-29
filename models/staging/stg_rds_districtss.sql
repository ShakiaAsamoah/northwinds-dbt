WITH source as (
    SELECT * FROM {{source('rds', 'dimcustomer')}}
), 
renamed as (
    SELECT 
    concat('rds-', replace(lower(district), ' ','-')) as district_id,
    district,
    max(address) as address,
    max(city) as city,
    max(postal_code) as postal_code,
    max(country) as country
    FROM source
    GROUP BY district
)
SELECT * FROM renamed