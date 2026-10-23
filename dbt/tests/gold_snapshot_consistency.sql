with tuples as (
select distinct silver_version_id,silver_manifest_sha256,scope_id,catalog_sha256,policy_sha256 from {{ ref('mart_contract_detail') }}
union select distinct silver_version_id,silver_manifest_sha256,scope_id,catalog_sha256,policy_sha256 from {{ ref('mart_contract_overview') }}
union select distinct silver_version_id,silver_manifest_sha256,scope_id,catalog_sha256,policy_sha256 from {{ ref('mart_entity_monthly') }}
union select distinct silver_version_id,silver_manifest_sha256,scope_id,catalog_sha256,policy_sha256 from {{ ref('mart_supplier_concentration') }} where total_contract_count>0
union select distinct silver_version_id,silver_manifest_sha256,scope_id,catalog_sha256,policy_sha256 from {{ ref('mart_data_quality') }}
)
select * from tuples where (select count(*) from tuples)<>1
