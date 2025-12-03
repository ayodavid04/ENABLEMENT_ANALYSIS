{{ config(
    schema = 'staging_curated',
    materialized = 'view'
) }}

with gisds as (
    select *
    from {{ ref('gisds_case_mapping') }}
),

case_type_map as (
    select distinct normalised_value
    from {{ ref('unified_case_type') }}
),

status_map as (
    select distinct normalised_value
    from {{ ref('unified_claim_status') }}
),

enhanced as (
    select
        g.*,

        -- Data Quality Checks
        case 
            when gisds_case_reference is null then 'MISSING_CASE_ID'
            when length(gisds_case_reference) < 1 then 'EMPTY_CASE_ID'
            else 'OK'
        end as dq_case_id_status,

        case 
            when gisds_status is null then 'MISSING_STATUS'
            else 'OK'
        end as dq_status_status,

        case 
            when gisds_case_category = 'OTHER' then 'UNMAPPED_CASE_CATEGORY'
            else 'OK'
        end as dq_category_mapping,

        case 
            when gisds_severity_band is null then 'MISSING_SEVERITY'
            else 'OK'
        end as dq_severity_mapping,

        -- Picklist Mismatch Flags (GISDS vs NORMALISED PICKLIST VALUES)
        case 
            when lower(g.gisds_case_category) not in (
                select lower(normalised_value) from case_type_map
            ) then 'UNRECOGNISED_CASE_TYPE'
            else 'OK'
        end as picklist_case_type_flag,

        case 
            when lower(g.gisds_status) not in (
                select lower(normalised_value) from status_map
            ) then 'UNRECOGNISED_STATUS'
            else 'OK'
        end as picklist_status_flag

    from gisds g
)

select *
from enhanced