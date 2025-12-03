with all_values as (
    select case_type, source_application 
    from {{ ref('stg_cms_app_a') }}

    union all

    select case_type, source_application 
    from {{ ref('stg_cms_app_b') }}

    union all

    select case_type, source_application
    from {{ ref('stg_cms_app_c') }}
),

cleaned as (
    select
        source_application,
        case_type as raw_value,
        lower(trim(case_type)) as normalised_value
    from all_values
)

select * 
from cleaned
order by source_application, raw_value
