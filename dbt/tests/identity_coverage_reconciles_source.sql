with expected as (
    select
        entity_identity_status,
        supplier_identity_status,
        count(*) as contract_count,
        sum(contract_value_cop) as value_cop
    from {{ source('silver', 'contract_current') }}
    group by 1, 2
), actual as (
    select
        entity_identity_status,
        supplier_identity_status,
        count(*) as contract_count,
        sum(contract_value_cop) as value_cop
    from {{ ref('fact_contract') }}
    group by 1, 2
)
select * from (
    (select * from expected except select * from actual)
    union all
    (select * from actual except select * from expected)
) differences
