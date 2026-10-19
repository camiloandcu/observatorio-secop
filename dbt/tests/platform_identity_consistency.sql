with roles as (
    select
        'entity' as role,
        f.contract_key,
        f.entity_identity_status as identity_status,
        f.entity_identity_reason as identity_reason,
        e.entity_code as code,
        e.member_status
    from {{ ref('fact_contract') }} f
    left join {{ ref('dim_entity') }} e using (entity_sk)
    union all
    select
        'supplier', f.contract_key, f.supplier_identity_status,
        f.supplier_identity_reason, s.supplier_code, s.member_status
    from {{ ref('fact_contract') }} f
    left join {{ ref('dim_supplier') }} s using (supplier_sk)
)
select * from roles
where identity_status is distinct from member_status
    or (identity_status = 'KNOWN') is distinct from (code is not null)
    or (identity_status = 'UNKNOWN') is distinct from (identity_reason is not null)
