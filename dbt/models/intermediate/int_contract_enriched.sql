with normalized as (
    select
        *,
        {{ canonical_text('entity_name') }} as entity_name_normalized,
        {{ canonical_text('procurement_method') }} as modality_name_normalized
    from {{ ref('stg_silver_contract') }}
)

select
    *,
    case
        when entity_name_normalized is null then {{ unknown_natural_key() }}
        else source_dataset_id || '|' || entity_name_normalized
    end as entity_natural_key,
    {{ unknown_natural_key() }} as supplier_natural_key,
    case
        when municipality_status = 'UNKNOWN' or municipality_key is null
            then {{ unknown_natural_key() }}
        else municipality_key
    end as municipality_natural_key,
    coalesce(modality_name_normalized, {{ unknown_natural_key() }}) as modality_natural_key,
    coalesce(to_char(signing_date, 'YYYYMMDD')::integer, 0) as signing_date_sk,
    coalesce(to_char(start_date, 'YYYYMMDD')::integer, 0) as start_date_sk,
    coalesce(to_char(end_date, 'YYYYMMDD')::integer, 0) as end_date_sk,
    case when start_date is not null and end_date is not null then end_date - start_date end
        as duration_days
from normalized
