select
    case_id::varchar as case_id,
    open_date::text as open_date,
    coalesce(nullif(close_date, ''), null)::text as close_date,
    fee_earner::varchar as fee_earner,
    lower(case_type)::varchar as case_type,
    lower(injury_severity)::varchar as injury_severity,
    client_name::varchar as client_name,
    lower(claim_status)::varchar as claim_status,
    lower(region)::varchar as region,
    'cms_app_a' as source_application
from raw.cms_app_a
