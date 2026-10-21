with counts as (
    select contract_key,
        count(*) filter (where evidence ->> 'temporal_relation' = 'LATER') as later_count,
        count(*) filter (where evidence ->> 'temporal_relation' = 'EQUAL') as equal_count
    from {{ ref('stg_contract_observation_link') }}
    group by contract_key
)
select c.contract_key
from {{ ref('stg_contract_observation_condition') }} c
left join counts l using (contract_key)
where c.condition is not null and (
    (c.condition ->> 'later_invalid_count')::bigint <> coalesce(l.later_count, 0)
    or (c.condition ->> 'equal_invalid_count')::bigint <> coalesce(l.equal_count, 0)
)
