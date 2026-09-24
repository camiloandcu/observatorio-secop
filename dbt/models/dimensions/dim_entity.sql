{{ config(unique_key='entity_sk', pre_hook='{{ reconcile_snapshot() }}') }}

with members as (
    select distinct
        entity_natural_key,
        source_dataset_id,
        entity_name,
        case when entity_natural_key = {{ unknown_natural_key() }} then 'UNKNOWN' else 'KNOWN' end
            as member_status
    from {{ ref('int_contract_enriched') }}
    union all
    select {{ unknown_natural_key() }}, null::text, null::text, 'UNKNOWN'
    where not exists (
        select 1 from {{ ref('int_contract_enriched') }}
        where entity_natural_key = {{ unknown_natural_key() }}
    )
)

select
    {{ star_key('entity', 'entity_natural_key') }} as entity_sk,
    entity_natural_key,
    source_dataset_id,
    entity_name,
    member_status
from members
