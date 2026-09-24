{{ config(unique_key='municipality_sk', pre_hook='{{ reconcile_snapshot() }}') }}

with members as (
    select distinct
        municipality_natural_key,
        case when municipality_natural_key = {{ unknown_natural_key() }} then null else municipality_name end
            as municipality_name,
        case when municipality_natural_key = {{ unknown_natural_key() }} then 'UNKNOWN' else 'KNOWN' end
            as member_status
    from {{ ref('int_contract_enriched') }}
    union all
    select {{ unknown_natural_key() }}, null::text, 'UNKNOWN'
    where not exists (
        select 1 from {{ ref('int_contract_enriched') }}
        where municipality_natural_key = {{ unknown_natural_key() }}
    )
)

select
    {{ star_key('municipality', 'municipality_natural_key') }} as municipality_sk,
    municipality_natural_key,
    municipality_name,
    member_status
from members
