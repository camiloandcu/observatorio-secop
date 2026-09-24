{{ config(unique_key='contract_sk', pre_hook='{{ reconcile_snapshot() }}') }}

select
    {{ star_key('contract', 'contracts.contract_key') }} as contract_sk,
    contracts.contract_key,
    contracts.contract_id,
    contracts.process_id,
    entities.entity_sk,
    suppliers.supplier_sk,
    municipalities.municipality_sk,
    modalities.modality_sk,
    contracts.signing_date_sk,
    contracts.start_date_sk,
    contracts.end_date_sk,
    contracts.signing_date,
    contracts.start_date,
    contracts.end_date,
    contracts.contract_value_cop,
    contracts.duration_days,
    contracts.status,
    contracts.source_url,
    contracts.source_updated_at,
    contracts.bronze_fetched_at_utc,
    contracts.silver_version_id,
    contracts.silver_manifest_sha256,
    contracts.payload_sha256,
    contracts.loaded_at
from {{ ref('int_contract_enriched') }} as contracts
left join {{ ref('dim_entity') }} as entities
    on entities.entity_natural_key = contracts.entity_natural_key
left join {{ ref('dim_supplier') }} as suppliers
    on suppliers.supplier_natural_key = contracts.supplier_natural_key
left join {{ ref('dim_municipality') }} as municipalities
    on municipalities.municipality_natural_key = contracts.municipality_natural_key
left join {{ ref('dim_modality') }} as modalities
    on modalities.modality_natural_key = contracts.modality_natural_key
