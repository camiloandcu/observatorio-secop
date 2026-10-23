select contract_key from {{ ref('mart_contract_detail') }} group by contract_key having count(*)<>1
