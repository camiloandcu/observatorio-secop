{{ config(unique_key='modality_sk', pre_hook='{{ reconcile_snapshot() }}') }}

with members as (
    select distinct
        modality_natural_key,
        case when modality_natural_key = {{ unknown_natural_key() }} then null else procurement_method end
            as modality_name,
        case when modality_natural_key = {{ unknown_natural_key() }} then 'UNKNOWN' else 'KNOWN' end
            as member_status
    from {{ ref('int_contract_enriched') }}
    union all
    select {{ unknown_natural_key() }}, null::text, 'UNKNOWN'
    where not exists (
        select 1 from {{ ref('int_contract_enriched') }}
        where modality_natural_key = {{ unknown_natural_key() }}
    )
)

select
    {{ star_key('modality', 'modality_natural_key') }} as modality_sk,
    modality_natural_key,
    modality_name,
    member_status
from members
