select source.contract_key
from {{ ref('stg_silver_contract') }} as source
join {{ ref('fact_contract') }} as fact using (contract_key)
where fact.contract_id is distinct from source.contract_id
   or fact.process_id is distinct from source.process_id
   or fact.source_url is distinct from source.source_url
   or fact.source_updated_at is distinct from source.source_updated_at
   or fact.bronze_fetched_at_utc is distinct from source.bronze_fetched_at_utc
   or fact.silver_version_id is distinct from source.silver_version_id
   or fact.silver_manifest_sha256 is distinct from source.silver_manifest_sha256
   or fact.payload_sha256 is distinct from source.payload_sha256
   or fact.loaded_at is distinct from source.loaded_at
