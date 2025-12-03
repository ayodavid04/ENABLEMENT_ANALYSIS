select
    caseRef::varchar as case_id,
    start_dt::text as open_date,
    null::text as close_date,
    feeEarner::varchar as fee_earner,
    case
        when typeCode = '1' then 'rta'
        when typeCode = '2' then 'el'
        when typeCode = '3' then 'pl'
        else null
    end as case_type,
    case
        when sevCode = '10' then 'low'
        when sevCode = '20' then 'medium'
        when sevCode = '30' then 'high'
        else null
    end as injury_severity,
    partyName::varchar as client_name,
    case
        when caseStatus = 'O' then 'open'
        when caseStatus = 'C' then 'closed'
        when caseStatus = 'S' then 'settled'
        else null
    end as claim_status,
    lower(area)::varchar as region,
    'cms_app_c' as source_application
from raw.cms_app_c
