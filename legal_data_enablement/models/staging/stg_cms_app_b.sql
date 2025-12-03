select
    id::varchar as case_id,
    date_opened::text as open_date,
    null::text as close_date,
    solicitor::varchar as fee_earner,
    lower(category)::varchar as case_type,
    lower(severity)::varchar as injury_severity,
    claimant::varchar as client_name,
    lower(status)::varchar as claim_status,
    lower(division)::varchar as region,
    'cms_app_b' as source_application
from raw.cms_app_b
