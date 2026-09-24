{{ config(unique_key='date_sk', pre_hook='{{ reconcile_snapshot() }}') }}

with bounds as (
    select
        least(min(signing_date), min(start_date), min(end_date)) as minimum_date,
        greatest(max(signing_date), max(start_date), max(end_date)) as maximum_date
    from {{ ref('int_contract_enriched') }}
), dates as (
    select day::date as calendar_date
    from bounds
    cross join lateral generate_series(minimum_date, maximum_date, interval '1 day') as day
    where minimum_date is not null and maximum_date is not null
)

select
    0::integer as date_sk,
    null::date as calendar_date,
    null::smallint as year,
    null::smallint as quarter,
    null::smallint as month,
    null::text as month_name,
    null::smallint as day_of_month,
    null::smallint as day_of_week,
    null::text as day_name,
    'UNKNOWN'::text as member_status
union all
select
    to_char(calendar_date, 'YYYYMMDD')::integer,
    calendar_date,
    extract(year from calendar_date)::smallint,
    extract(quarter from calendar_date)::smallint,
    extract(month from calendar_date)::smallint,
    to_char(calendar_date, 'FMMonth'),
    extract(day from calendar_date)::smallint,
    extract(isodow from calendar_date)::smallint,
    to_char(calendar_date, 'FMDay'),
    'KNOWN'::text
from dates
