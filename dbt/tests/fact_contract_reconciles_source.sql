select source.contract_key
from {{ ref('stg_silver_contract') }} as source
full outer join {{ ref('fact_contract') }} as fact using (contract_key)
where source.contract_key is null or fact.contract_key is null
