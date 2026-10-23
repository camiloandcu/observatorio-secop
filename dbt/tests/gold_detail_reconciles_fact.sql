select coalesce(f.contract_key,d.contract_key) as contract_key
from {{ ref('fact_contract') }} f
full outer join {{ ref('mart_contract_detail') }} d using(contract_key)
where f.contract_key is null or d.contract_key is null
or f.contract_value_cop is distinct from d.contract_value_cop
or f.silver_version_id is distinct from d.silver_version_id
or f.silver_manifest_sha256 is distinct from d.silver_manifest_sha256
union all
select d.contract_key from {{ ref('mart_contract_detail') }} d
left join {{ source('silver','contract_current') }} c using(contract_key,silver_version_id,silver_manifest_sha256)
where c.contract_key is null
