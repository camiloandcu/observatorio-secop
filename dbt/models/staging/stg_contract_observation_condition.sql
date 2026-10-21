select
    c.silver_version_id,
    c.silver_manifest_sha256,
    c.contract_key,
    coalesce(o.scope_id, b.scope_id) as scope_id,
    coalesce(o.condition ->> 'state', 'LEGACY_UNASSESSED') as condition_state,
    o.condition
from {{ source('silver', 'contract_current') }} c
left join {{ source('silver', 'contract_current_observation_condition') }} o
    on o.silver_version_id = c.silver_version_id
    and o.manifest_sha256 = c.silver_manifest_sha256
    and o.contract_key = c.contract_key
left join {{ source('silver', 'contract_current_population_binding') }} b on b.singleton
