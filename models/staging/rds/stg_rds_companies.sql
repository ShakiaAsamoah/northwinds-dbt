with source as (
    select * from {{source('rds','customers')}}
),
renamed as (
    select
    concat('rds-', replace(lower(companyname), ' ', '-')) as company_id,
    companyname,
    max(address) as address,
    max(city) as city,
    max(postalcode) as postal_code,
    max(country) as country
    from source
    group by companyname
)
select * from renamed