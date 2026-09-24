select 'entity' as model_name, entity_sk as surrogate_key
from {{ ref('dim_entity') }}
group by entity_sk
having count(distinct entity_natural_key) <> 1
union all
select 'municipality', municipality_sk
from {{ ref('dim_municipality') }}
group by municipality_sk
having count(distinct municipality_natural_key) <> 1
union all
select 'modality', modality_sk
from {{ ref('dim_modality') }}
group by modality_sk
having count(distinct modality_natural_key) <> 1
union all
select 'contract', contract_sk
from {{ ref('fact_contract') }}
group by contract_sk
having count(distinct contract_key) <> 1
