select
    l.silver_version_id,
    l.manifest_sha256 as silver_manifest_sha256,
    l.scope_id,
    l.contract_key,
    l.link_id,
    l.evidence
from {{ source('silver', 'contract_current_observation_link') }} l
inner join {{ source('silver', 'contract_current') }} c
    on c.silver_version_id = l.silver_version_id
    and c.silver_manifest_sha256 = l.manifest_sha256
    and c.contract_key = l.contract_key
