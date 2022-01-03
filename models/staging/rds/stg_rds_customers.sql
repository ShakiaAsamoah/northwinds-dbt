WITH source as (
  SELECT * FROM {{source('rds', 'dimcustomer')}}
), 
districts as (
  SELECT * FROM dbt_sasamoah.stg_rds_districtss
),
renamed as (
    SELECT 
    concat ('rds-', customer_id) as customer_id, 
    country, 
    district,
    SPLIT_PART(first_name, ' ', 1) as first_name,
    SPLIT_PART(last_name, ' ', -1) as last_name,
    REPLACE (TRANSLATE(phone, '(,),-,.',''), ' ', '') as updated_phone,
    CASE WHEN LENGTH(updated_phone) = 10 THEN
      '(' || SUBSTRING(updated_phone, 1, 3) || ') ' || 
       SUBSTRING(updated_phone, 4, 3) || '-' ||
       SUBSTRING(updated_phone, 7, 4) 
       END as phone
    FROM source
), 
final as (
  SELECT 
  customer_id,
  first_name,
  last_name,
  phone
  FROM renamed
)
SELECT * FROM final 