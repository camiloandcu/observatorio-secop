select q.silver_version_id, q.manifest_sha256 as silver_manifest_sha256,
    coalesce(q.scope_id,b.scope_id) as scope_id, q.summary, q.evidence_sha256
from {{ source('silver', 'contract_current_quality') }} q
inner join (select distinct silver_version_id,silver_manifest_sha256 from {{ source('silver','contract_current') }}) active
 on active.silver_version_id=q.silver_version_id and active.silver_manifest_sha256=q.manifest_sha256
left join {{ source('silver', 'contract_current_population_binding') }} b on b.singleton
