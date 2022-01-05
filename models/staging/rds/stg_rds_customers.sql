WITH customers as (
  SELECT * FROM {{source('rds', 'customers')}}
),
companies as (
  select * from NORTHWINDS_RDS.DBT_SASAMOAH.STG_RDS_COMPANIES
),
renamed as (
    SELECT 
    concat ('rds-', customerid) as contact_id, 
    customers.country as country, 
    companies.companyname as company_name,
    SPLIT_PART(contactname, ' ', 1) as first_name,
    SPLIT_PART(contactname, ' ', -1) as last_name,
    REPLACE (TRANSLATE(phone, '(,),-,.',''), ' ', '') as updated_phone,
    CASE WHEN LENGTH(updated_phone) = 10 THEN
      '(' || SUBSTRING(updated_phone, 1, 3) || ') ' || 
       SUBSTRING(updated_phone, 4, 3) || '-' ||
       SUBSTRING(updated_phone, 7, 4) 
       END as phone,
       company_id
    FROM customers join companies on companies.companyname = customers.companyname
), 
final as (
  SELECT 
  contact_id,
  first_name,
  last_name,
  phone, 
  company_id
  FROM renamed
)
SELECT * FROM final 