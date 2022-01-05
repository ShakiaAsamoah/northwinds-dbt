WITH contact as (
  SELECT * FROM {{source('hubspot', 'contact')}}
), 
companies as (
  SELECT * FROM DBT_SASAMOAH.STG_HUBSPOT_COMPANIES
),
renamed as (
    SELECT 
    concat ('hubspot-', hubspot_id) as contact_id, 
    SPLIT_PART(first_name, ' ', 1) as first_name,
    SPLIT_PART(last_name, ' ', -1) as last_name,
    REPLACE (TRANSLATE(phone, '(,),-,.',''), ' ', '') as updated_phone,
    CASE WHEN LENGTH(updated_phone) = 10 THEN
      '(' || SUBSTRING(updated_phone, 1, 3) || ') ' || 
       SUBSTRING(updated_phone, 4, 3) || '-' ||
       SUBSTRING(updated_phone, 7, 4) 
       END as phone,
    business_id
    FROM contact JOIN companies ON companies.name = contact.business_name
), 
final as (
  SELECT 
  contact_id,
  first_name,
  last_name,
  phone,
  business_id
  FROM renamed
)
SELECT * FROM final 