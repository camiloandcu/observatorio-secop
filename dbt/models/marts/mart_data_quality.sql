with global_counts as (
select q.silver_version_id,q.silver_manifest_sha256,q.scope_id,
    counts.key as control_id,(counts.value #>> '{}')::bigint as numerator,
    (q.summary->'counts'->>'input_observations')::bigint as denominator,
    'OBSERVATION'::text as evaluation_grain,'SNAPSHOT_SCOPE_ONLY'::text as filter_applicability,
    q.summary->>'state' as control_state,q.summary->>'reason' as reason,
    q.summary->>'pipeline_completed_at_utc' as pipeline_completed_at_utc,
    q.evidence_sha256
from {{ ref('stg_quality_summary') }} q
cross join lateral jsonb_each(coalesce(nullif(q.summary->'counts','null'::jsonb),
    '{"input_observations":null,"accepted_valid_observations":null,"output_contracts":null,"rejected_observations":null,"exact_duplicates":null,"superseded_valid_observations":null}'::jsonb)) counts
), contract_controls as (
select silver_version_id,silver_manifest_sha256,scope_id, control_id,
    count(*) filter(where affected) as numerator,count(*) as denominator,
    'UNIQUE_CONTRACT'::text as evaluation_grain,'DETAIL_SUPPORTED_FILTERS'::text as filter_applicability,
    'AVAILABLE'::text as control_state,null::text as reason,
    max(pipeline_completed_at_utc)::text as pipeline_completed_at_utc,
    max(quality_evidence_sha256) as evidence_sha256
from {{ ref('mart_contract_detail') }} d
cross join lateral (values
    ('missing_signing_date',signing_date is null),
    ('unknown_supplier',supplier_identity_status<>'KNOWN'),
    ('unknown_entity',entity_identity_status<>'KNOWN'),
    ('missing_status',status_profile_state='MISSING'),
    ('unprofiled_status',status_profile_state='UNPROFILED'),
    ('missing_source_update',source_updated_at is null),
    ('later_invalid_observed',condition_state='LATER_INVALID_OBSERVED'),
    ('temporal_order_indeterminate',condition_state='TEMPORAL_ORDER_INDETERMINATE'),
    ('legacy_unassessed',condition_state='LEGACY_UNASSESSED'),
    ('selected_unique_contracts',true),
    ('excluded_status_baseline',false)
) controls(control_id,affected)
group by silver_version_id,silver_manifest_sha256,scope_id,control_id
), coverage_controls as (
select p.silver_version_id,p.silver_manifest_sha256,p.scope_id,
 'acquisition:'||(a->>'campaign_id')||':'||field.name as control_id,
 case when (a->>field.name)::boolean then 1 else 0 end::bigint as numerator,
 1::bigint as denominator,'ACQUISITION_LANE'::text as evaluation_grain,
 'SNAPSHOT_SCOPE_ONLY'::text as filter_applicability,a->>'state' as control_state,
 p.coverage_state as reason,a->>'observed_to_utc' as pipeline_completed_at_utc,
 q.evidence_sha256
from {{ ref('stg_population_coverage') }} p
join {{ ref('stg_quality_summary') }} q using(silver_version_id,silver_manifest_sha256)
cross join lateral jsonb_array_elements(p.receipt->'acquisitions') a
cross join (values ('truncated'),('scope_scan_complete'),('baseline_check_complete')) field(name)
), verified_controls as (
select q.silver_version_id,q.silver_manifest_sha256,q.scope_id,
    c->>'check_id' as control_id,(c->>'affected_rows')::bigint as numerator,
    (c->>'denominator')::bigint as denominator,c->>'denominator_grain' as evaluation_grain,
    'SNAPSHOT_SCOPE_ONLY'::text as filter_applicability,
    case when (c->>'passed')::boolean then 'PASSED' else 'FAILED' end as control_state,
    c->>'severity' as reason,q.summary->>'pipeline_completed_at_utc' as pipeline_completed_at_utc,q.evidence_sha256
from {{ ref('stg_quality_summary') }} q cross join lateral jsonb_array_elements(q.summary->'controls') c
)
select *, '{{ gold_catalog_digest() }}'::text as catalog_sha256,
    '{{ gold_policy_digest() }}'::text as policy_sha256 from (
select * from global_counts union all select * from contract_controls union all select * from verified_controls union all select * from coverage_controls
) controls
