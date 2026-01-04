-- ANALYSIS QUERIES FOR LEGAL DATA ENABLEMENT PROJECT

-- 1. Cases by claimant and status
select
    gisds_claimant,
    gisds_status,
    count(*) as case_count
from curated.unified_case_model
group by gisds_claimant, gisds_status
order by gisds_claimant, gisds_status;


-- 2. Case volume by category and severity band
select
    gisds_case_category,
    gisds_severity_band,
    count(*) as case_count
from curated.unified_case_model
group by gisds_case_category, gisds_severity_band
order by gisds_case_category, gisds_severity_band;


-- 3. Case volume by source application and status
select
    original_source as source_application,
    gisds_status,
    count(*) as case_count
from curated.unified_case_model
group by original_source, gisds_status
order by original_source, gisds_status;


-- 4. Data quality issues by source application
select
    original_source as source_application,
    sum(case when dq_case_id_status <> 'OK' then 1 else 0 end) as bad_case_id_count,
    sum(case when dq_status_status  <> 'OK' then 1 else 0 end) as bad_status_count,
    sum(case when dq_category_mapping = 'UNMAPPED_CASE_CATEGORY' then 1 else 0 end) as unmapped_category_count,
    sum(case when dq_severity_mapping = 'MISSING_SEVERITY' then 1 else 0 end) as missing_severity_count,
    count(*) as total_cases
from curated.unified_case_model
group by original_source
order by total_cases desc;


-- 5. Handler (fee earner) data quality score
-- Only include handlers with at least 5 cases so the metric isn't noisy.
with per_case as (
    select
        gisds_handler,
        gisds_case_reference,
        case 
            when dq_case_id_status <> 'OK'
              or dq_status_status  <> 'OK'
              or dq_category_mapping = 'UNMAPPED_CASE_CATEGORY'
              or dq_severity_mapping = 'MISSING_SEVERITY'
            then 1 else 0 
        end as has_dq_issue
    from curated.unified_case_model
),
agg as (
    select
        gisds_handler,
        count(*) as total_cases,
        sum(has_dq_issue) as dq_issue_count
    from per_case
    group by gisds_handler
)
select
    gisds_handler,
    total_cases,
    dq_issue_count,
    1.0 - (dq_issue_count::decimal / nullif(total_cases, 0)) as dq_score
from agg
where total_cases >= 5
order by dq_score asc, total_cases desc;


-- 6. Monthly case intake by application and category
-- Safe cast: only treat gisds_open_date as a date if it looks like YYYY-MM-DD
with typed_dates as (
    select
        case
            when gisds_open_date ~ '^\d{4}-\d{2}-\d{2}' 
                then to_date(gisds_open_date, 'YYYY-MM-DD')
            else null
        end as open_date,
        original_source as source_application,
        gisds_case_category
    from curated.unified_case_model
),
filtered as (
    select *
    from typed_dates
    where open_date is not null
)
select
    to_char(open_date, 'YYYY-MM') as open_month,
    source_application,
    gisds_case_category,
    count(*) as case_count
from filtered
group by to_char(open_date, 'YYYY-MM'), source_application, gisds_case_category
order by open_month, source_application, gisds_case_category;



-- 7. High-severity open cases (operational-style view)
select
    gisds_case_reference,
    gisds_claimant,
    gisds_handler,
    original_source as source_application,
    gisds_case_category,
    gisds_severity_band,
    gisds_status,
    gisds_open_date
from curated.unified_case_model
where gisds_severity_band = 'HIGH'
  and gisds_status <> 'CLOSED'
order by gisds_open_date;
