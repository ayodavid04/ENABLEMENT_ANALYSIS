with unified_cases as (

    select 
        case_id,
        open_date,
        close_date,
        fee_earner,
        case_type,
        injury_severity,
        client_name,
        claim_status,
        region,
        source_application
    from {{ ref('stg_cms_app_a') }}

    union all

    select 
        case_id,
        open_date,
        close_date,
        fee_earner,
        case_type,
        injury_severity,
        client_name,
        claim_status,
        region,
        source_application
    from {{ ref('stg_cms_app_b') }}

    union all

    select 
        case_id,
        open_date,
        close_date,
        fee_earner,
        case_type,
        injury_severity,
        client_name,
        claim_status,
        region,
        source_application
    from {{ ref('stg_cms_app_c') }}

),

gisds_aligned as (
    select
        case_id as gisds_case_reference,
        open_date as gisds_open_date,
        close_date as gisds_close_date,
        fee_earner as gisds_handler,
        case
            when lower(case_type) in ('rta','motor') then 'MOTOR'
            when lower(case_type) in ('el') then 'EMPLOYERS LIABILITY'
            when lower(case_type) in ('pl','public liability') then 'PUBLIC LIABILITY'
            else 'OTHER'
        end as gisds_case_category,
        case 
            when lower(injury_severity) in ('low','minor') then 'LOW'
            when lower(injury_severity) in ('medium','moderate') then 'MEDIUM'
            when lower(injury_severity) in ('high','severe') then 'HIGH'
            else null
        end as gisds_severity_band,
        client_name as gisds_claimant,
        claim_status as gisds_status,
        region as gisds_location,
        source_application as original_source
    from unified_cases
)

select * from gisds_aligned

