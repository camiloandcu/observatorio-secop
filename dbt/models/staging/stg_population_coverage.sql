select
    p.silver_version_id,
    p.manifest_sha256 as silver_manifest_sha256,
    coalesce(p.scope_id, b.scope_id) as scope_id,
    coalesce((p.receipt ->> 'coverage_known')::boolean,
             p.receipt ->> 'schema_version' = 'population-coverage-v1') as coverage_known,
    p.receipt ->> 'state' as coverage_state,
    p.receipt ->> 'basis' as basis,
    p.receipt
from {{ source('silver', 'contract_current_population') }} p
inner join (
    select distinct silver_version_id, silver_manifest_sha256
    from {{ source('silver', 'contract_current') }}
) active on active.silver_version_id = p.silver_version_id
    and active.silver_manifest_sha256 = p.manifest_sha256
left join {{ source('silver', 'contract_current_population_binding') }} b on b.singleton
