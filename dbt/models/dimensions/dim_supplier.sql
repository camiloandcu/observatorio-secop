{{ config(unique_key='supplier_sk', pre_hook='{{ reconcile_snapshot() }}') }}

with ranked as (
    select
        supplier_natural_key,
        source_dataset_id,
        supplier_code,
        supplier_name,
        row_number() over (
            partition by supplier_natural_key
            order by
                (nullif(btrim(supplier_name), '') is not null) desc,
                source_updated_at desc nulls last,
                bronze_fetched_at_utc desc,
                contract_key desc,
                bronze_run_id desc,
                bronze_page_path desc,
                bronze_row_ordinal desc
        ) as label_rank
    from {{ ref('int_contract_enriched') }}
    where supplier_identity_status = 'KNOWN'
), members as (
    select supplier_natural_key, source_dataset_id, supplier_code, supplier_name, 'KNOWN' as member_status
    from ranked where label_rank = 1
    union all
    select {{ unknown_natural_key() }}, null::text, null::text, null::text, 'UNKNOWN'
)

select
    {{ platform_star_key('supplier', 'supplier_natural_key') }} as supplier_sk,
    supplier_natural_key,
    source_dataset_id,
    supplier_code,
    supplier_name,
    member_status
from members
