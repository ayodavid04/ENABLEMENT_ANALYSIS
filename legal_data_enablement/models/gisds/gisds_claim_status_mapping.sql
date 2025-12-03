with unified as (
    select distinct normalised_value
    from {{ ref('unified_claim_status') }}
)

select
    normalised_value as source_value,

    case
        when normalised_value = 'open' then 'OPEN'
        when normalised_value = 'closed' then 'CLOSED'
        when normalised_value = 'settled' then 'SETTLED'
        else 'OTHER'
    end as gisds_claim_status
from unified
