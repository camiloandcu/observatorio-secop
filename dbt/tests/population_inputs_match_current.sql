with conditions as (
    select contract_key, count(*) as rows
    from {{ ref('stg_contract_observation_condition') }}
    group by contract_key
), current_contracts as (
    select contract_key from {{ ref('stg_silver_contract') }}
)
select c.contract_key
from current_contracts c
left join conditions o using (contract_key)
where coalesce(o.rows, 0) <> 1
union all
select o.contract_key
from conditions o
left join current_contracts c using (contract_key)
where c.contract_key is null
