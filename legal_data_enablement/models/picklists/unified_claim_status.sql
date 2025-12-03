with all_values as (
    select claim_status, source_application 
    from {{ ref('stg_cms_app_a') }}

    union all

    select claim_status, source_application
    from {{ ref('stg_cms_app_b') }}

    union all

    select claim_status, source_application
    from {{ ref('stg_cms_app_c') }}
),

cleaned as (
    select
        source_application,
        claim_status as raw_value,
        case
            when lower(trim(claim_status)) in ('open', 'o', 'active') then 'open'
            when lower(trim(claim_status)) in ('closed', 'c') then 'closed'
            when lower(trim(claim_status)) in ('settled', 'complete') then 'settled'
            else lower(trim(claim_status))
        end as normalised_value
    from all_values
)

select *
from cleaned
order by source_application, raw_value
