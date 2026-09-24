select 'dim_entity' as model_name
where (select count(*) from {{ ref('dim_entity') }} where entity_natural_key = {{ unknown_natural_key() }}) <> 1
union all
select 'dim_supplier'
where (select count(*) from {{ ref('dim_supplier') }} where supplier_natural_key = {{ unknown_natural_key() }}) <> 1
union all
select 'dim_municipality'
where (select count(*) from {{ ref('dim_municipality') }} where municipality_natural_key = {{ unknown_natural_key() }}) <> 1
union all
select 'dim_modality'
where (select count(*) from {{ ref('dim_modality') }} where modality_natural_key = {{ unknown_natural_key() }}) <> 1
union all
select 'dim_date'
where (select count(*) from {{ ref('dim_date') }} where date_sk = 0 and member_status = 'UNKNOWN') <> 1
