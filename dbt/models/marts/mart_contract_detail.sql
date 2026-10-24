with source as (
select f.*, s.contract_reference, s.quality_warning_codes,
    e.entity_name, e.entity_code, u.supplier_name, u.supplier_code,
    p.scope_id, p.coverage_state, p.coverage_known, p.receipt as coverage_receipt,
    c.condition_state, c.condition,
    q.summary, q.evidence_sha256 as quality_evidence_sha256
from {{ ref('fact_contract') }} f
join {{ ref('stg_silver_contract') }} s using (silver_version_id, silver_manifest_sha256, contract_key)
join {{ ref('dim_entity') }} e using (entity_sk)
join {{ ref('dim_supplier') }} u using (supplier_sk)
join {{ ref('stg_population_coverage') }} p using (silver_version_id, silver_manifest_sha256)
join {{ ref('stg_contract_observation_condition') }} c
    on c.silver_version_id=f.silver_version_id and c.silver_manifest_sha256=f.silver_manifest_sha256
    and c.contract_key=f.contract_key and c.scope_id is not distinct from p.scope_id
join {{ ref('stg_quality_summary') }} q
    on q.silver_version_id=f.silver_version_id and q.silver_manifest_sha256=f.silver_manifest_sha256
    and q.scope_id is not distinct from p.scope_id
)
select contract_sk, contract_key, contract_id, contract_reference, process_id,
    entity_sk, entity_name, entity_code, entity_identity_status, entity_identity_reason,
    supplier_sk, supplier_name, supplier_code, supplier_identity_status, supplier_identity_reason,
    municipality_sk, modality_sk, signing_date_sk, start_date_sk, end_date_sk,
    signing_date, start_date, end_date, date_trunc('month',signing_date)::date as signing_month,
    contract_value_cop, duration_days, status as status_raw,
    coalesce(nullif(lower(btrim(status)),''),'__MISSING__') as status_bucket,
    case when nullif(btrim(status),'') is null then 'MISSING'
         when lower(btrim(status)) in ('en aprobación','terminado','borrador','modificado','cerrado')
         then 'PROFILED' else 'UNPROFILED' end as status_profile_state,
    'ALL_VALID_OBSERVED'::text as base_cohort,
    case when source_url ~ '^https://(www[.])?community[.]secop[.]gov[.]co(/|$)'
         then source_url end as source_process_url,
    case when source_url is null or btrim(source_url)='' then 'MISSING'
         when source_url ~ '^https://(www[.])?community[.]secop[.]gov[.]co(/|$)' then 'AVAILABLE'
         else 'DISALLOWED' end as source_link_state,
    source_updated_at, bronze_fetched_at_utc as acquisition_observed_at_utc,
    (summary->>'pipeline_completed_at_utc')::timestamptz as pipeline_completed_at_utc,
    bronze_run_id, bronze_row_ordinal, payload_sha256,
    condition_state,
    {{ gold_sanitized_condition('condition') }} as observation_condition,
    (select coalesce(jsonb_agg(l.evidence - 'winner_page_path' - 'rejected_page_path' order by l.link_id),'[]'::jsonb)
     from {{ ref('stg_contract_observation_link') }} l
     where l.silver_version_id=source.silver_version_id and l.silver_manifest_sha256=source.silver_manifest_sha256
       and l.contract_key=source.contract_key) as attributable_rejection_evidence,
    quality_warning_codes, quality_evidence_sha256,
    silver_version_id, silver_manifest_sha256, scope_id, coverage_state, coverage_known,
    jsonb_build_object('state',coverage_receipt->'state','basis',coverage_receipt->'basis',
      'coverage_known',coverage_receipt->'coverage_known','scope_id',scope_id,
      'limitations',coverage_receipt->'limitations',
      'acquisitions',coverage_receipt->'acquisitions') as coverage_metadata,
    '{{ gold_catalog_digest() }}'::text as catalog_sha256,
    '{{ gold_policy_digest() }}'::text as policy_sha256,
    '{{ var("gold_policy_version", "gold-hhi-policy-v1") }}'::text as policy_version,
    current_timestamp as candidate_built_at_utc,
    'LATEST_OBSERVED_BY_SIGNING_DATE'::text as temporal_basis,
    'COP_NOMINAL_OBSERVED'::text as value_unit,
    'SOURCE_CALENDAR_ZONE_UNDECLARED'::text as source_time_basis
from source
