{% macro gold_catalog_digest() %}{{ return(var('gold_catalog_sha256')) }}{% endmacro %}
{% macro gold_policy_digest() %}{{ return(var('gold_policy_sha256')) }}{% endmacro %}

{% macro gold_sanitized_condition(expression) %}
({{ expression }} - 'winner_page_path' - 'rejected_page_path')
{% endmacro %}

{% macro gold_parameter(filters, name, type, bound) %}
{% if bound %}cast(%({{ name }})s as {{ type }}){% elif filters.get(name) is none %}null::{{ type }}{% elif type.endswith('[]') %}
array[{% for item in filters[name] %}'{{ item|string|replace("'", "''") }}'{% if not loop.last %},{% endif %}{% endfor %}]::{{ type }}
{% else %}'{{ filters[name]|string|replace("'", "''") }}'::{{ type }}{% endif %}
{% endmacro %}

{% macro gold_filtered_ctes(relation, filters={}, bound=false) %}
{% set types={'entity_keys':'text[]','municipality_keys':'text[]','modality_keys':'text[]','statuses':'text[]','exclude_statuses':'text[]','start_date':'date','end_date':'date','missing_signatures':'boolean','selected_supplier':'text'} %}
{% for key in filters %}{% if key not in types %}{{ exceptions.raise_compiler_error('UNSUPPORTED_GOLD_FILTER: ' ~ key) }}{% endif %}{% endfor %}
{% if relation is string %}{{ exceptions.raise_compiler_error('Gold requires a trusted dbt Relation') }}{% endif %}
parameters as (select {% for name,type in types.items() %}{{ gold_parameter(filters,name,type,bound) }} as {{ name }}{% if not loop.last %},{% endif %}{% endfor %}),
validated as (
select *, case
 when missing_signatures is true and (start_date is not null or end_date is not null)
 then 'INVALID_GOLD_SELECTION_MISSING_SIGNATURE_WITH_INTERVAL'
 when statuses && exclude_statuses then 'INVALID_GOLD_STATUS_INCLUDE_EXCLUDE_OVERLAP'
 when start_date >= end_date then 'INVALID_GOLD_SELECTION_INTERVAL'
 when entity_keys is not null and exists(select 1 from unnest(entity_keys) k where not exists(select 1 from {{ relation }} d where d.entity_sk=k)) then 'INVALID_GOLD_ENTITY_KEY'
 when municipality_keys is not null and exists(select 1 from unnest(municipality_keys) k where not exists(select 1 from {{ relation }} d where d.municipality_sk=k)) then 'INVALID_GOLD_MUNICIPALITY_KEY'
 when modality_keys is not null and exists(select 1 from unnest(modality_keys) k where not exists(select 1 from {{ relation }} d where d.modality_sk=k)) then 'INVALID_GOLD_MODALITY_KEY'
 when selected_supplier is not null and not exists(select 1 from {{ relation }} d where d.supplier_sk=selected_supplier) then 'INVALID_GOLD_SUPPLIER_KEY'
 else '1' end::integer as valid
from parameters),
reference as (
select d.* from {{ relation }} d cross join validated p
where p.valid=1
 and (p.entity_keys is null or d.entity_sk=any(p.entity_keys))
 and (p.municipality_keys is null or d.municipality_sk=any(p.municipality_keys))
 and (p.modality_keys is null or d.modality_sk=any(p.modality_keys))
 and (p.statuses is null or d.status_bucket=any(p.statuses))
 and (p.exclude_statuses is null or not d.status_bucket=any(p.exclude_statuses))
 and (p.start_date is null or d.signing_date>=p.start_date)
 and (p.end_date is null or d.signing_date<p.end_date)
 and (p.missing_signatures is null or (d.signing_date is null)=p.missing_signatures)
), selected as (
select d.* from reference d cross join validated p
where p.selected_supplier is null or d.supplier_sk=p.selected_supplier
)
{% endmacro %}

{% macro gold_numeric_quantile(array_expression, probability) %}
case when cardinality({{ array_expression }})>0 then
 ({{ array_expression }})[floor((cardinality({{ array_expression }})-1)*{{ probability }}::numeric)::integer+1]
 + (({{ array_expression }})[ceil((cardinality({{ array_expression }})-1)*{{ probability }}::numeric)::integer+1]
 - ({{ array_expression }})[floor((cardinality({{ array_expression }})-1)*{{ probability }}::numeric)::integer+1])
 * (((cardinality({{ array_expression }})-1)*{{ probability }}::numeric)-floor((cardinality({{ array_expression }})-1)*{{ probability }}::numeric)) end
{% endmacro %}

{% macro gold_distribution_columns() %}
{{ gold_numeric_quantile('amounts','0.25') }} as value_p25_cop,
{{ gold_numeric_quantile('amounts','0.5') }} as value_median_cop,
{{ gold_numeric_quantile('amounts','0.75') }} as value_p75_cop,
case when contract_count=0 then 'NO_CONTRACTS' when cardinality(amounts)=0 then 'NO_NON_NULL_VALUES' else 'AVAILABLE' end as distribution_state
{% endmacro %}

{% macro gold_overview_query(relation, filters={}, bound=false, stored=false) %}
with {{ gold_filtered_ctes(relation,filters,bound) }}, aggregated as (
select {% if stored %}silver_version_id,silver_manifest_sha256,scope_id, signing_month, municipality_sk, modality_sk,status_bucket,catalog_sha256,policy_sha256,{% endif %}
 {% if not stored %}(select max(silver_version_id) from {{ relation }}) as silver_version_id,(select max(silver_manifest_sha256) from {{ relation }}) as silver_manifest_sha256,(select max(scope_id) from {{ relation }}) as scope_id,(select max(catalog_sha256) from {{ relation }}) as catalog_sha256,(select max(policy_sha256) from {{ relation }}) as policy_sha256,{% endif %}
 count(distinct contract_key) as contract_count,
 coalesce(sum(contract_value_cop),0.00::numeric) as contract_value_cop,
 count(*) filter(where contract_value_cop is null) as missing_value_count,
 count(*) filter(where signing_date is null) as missing_signing_date_count,
 array_agg(contract_value_cop order by contract_value_cop) filter(where contract_value_cop is not null) as amounts
from selected {% if stored %}group by silver_version_id,silver_manifest_sha256,scope_id,signing_month,municipality_sk,modality_sk,status_bucket,catalog_sha256,policy_sha256{% endif %}
)
select aggregated.*, {{ gold_distribution_columns() }},
 to_jsonb(parameters) as selection_metadata,'SELECTED_CONTRACT_POPULATION'::text as denominator_basis,
 contract_count::numeric/nullif(sum(contract_count) over(),0) as contract_count_share,
 contract_value_cop/nullif(sum(contract_value_cop) over(),0) as contract_value_share,
 case when sum(contract_count) over()=0 then 'ZERO_DENOMINATOR' else 'AVAILABLE' end as count_share_state,
 case when sum(contract_value_cop) over()=0 then 'ZERO_DENOMINATOR' else 'AVAILABLE' end as value_share_state
from aggregated cross join parameters cross join validated where valid=1
{% endmacro %}

{% macro gold_entity_monthly_query(relation, filters={}, bound=false) %}
with {{ gold_filtered_ctes(relation,filters,bound) }}, aggregated as (
select silver_version_id,silver_manifest_sha256,scope_id,entity_sk,signing_month,catalog_sha256,policy_sha256,
 count(distinct contract_key) as contract_count,coalesce(sum(contract_value_cop),0.00::numeric) as contract_value_cop,
 count(*) filter(where contract_value_cop is null) as missing_value_count,
 array_agg(contract_value_cop order by contract_value_cop) filter(where contract_value_cop is not null) as amounts,
 jsonb_object_agg(status_bucket,status_count) as status_distribution
from (select *,count(*) over(partition by entity_sk,signing_month,status_bucket) as status_count from selected) d
group by silver_version_id,silver_manifest_sha256,scope_id,entity_sk,signing_month,catalog_sha256,policy_sha256)
select aggregated.*, {{ gold_distribution_columns() }} from aggregated cross join validated where valid=1
{% endmacro %}

{% macro gold_concentration_query(relation, filters={}, bound=false, stored=false) %}
with {{ gold_filtered_ctes(relation,filters,bound) }}, grouped as (
select silver_version_id,silver_manifest_sha256,scope_id,entity_sk,
 {% if stored %}signing_month,{% endif %}catalog_sha256,policy_sha256,
 count(*) as total_contract_count,count(*) filter(where supplier_identity_status='KNOWN') as known_contract_count,
 coalesce(sum(contract_value_cop),0::numeric) as total_value_cop,
 coalesce(sum(contract_value_cop) filter(where supplier_identity_status='KNOWN'),0::numeric) as known_value_cop,
 min(entity_identity_status) as entity_identity_status
from reference group by silver_version_id,silver_manifest_sha256,scope_id,entity_sk,{% if stored %}signing_month,{% endif %}catalog_sha256,policy_sha256
union all
select (select max(silver_version_id) from {{ relation }}),(select max(silver_manifest_sha256) from {{ relation }}),(select max(scope_id) from {{ relation }}),null::text,{% if stored %}null::date,{% endif %}(select max(catalog_sha256) from {{ relation }}),(select max(policy_sha256) from {{ relation }}),
 0::bigint,0::bigint,0::numeric,0::numeric,null::text
where not exists(select 1 from reference)
), supplier_values as (
select entity_sk,{% if stored %}signing_month,{% endif %}supplier_sk,sum(contract_value_cop) as supplier_value_cop
from reference where supplier_identity_status='KNOWN'
group by entity_sk,{% if stored %}signing_month,{% endif %}supplier_sk
), measured as (
select g.*,
 known_contract_count::numeric/nullif(total_contract_count,0) as count_identity_coverage,
 known_value_cop/nullif(total_value_cop,0) as value_identity_coverage,
 (select sum((s.supplier_value_cop/nullif(g.known_value_cop,0))^2) from supplier_values s
 where s.entity_sk=g.entity_sk {% if stored %}and s.signing_month is not distinct from g.signing_month{% endif %}) as raw_hhi,
 (select sum(s.supplier_value_cop)/nullif(g.known_value_cop,0) from supplier_values s cross join parameters p
 where s.entity_sk=g.entity_sk and s.supplier_sk=p.selected_supplier {% if stored %}and s.signing_month is not distinct from g.signing_month{% endif %}) as selected_supplier_reference_share,
 (select count(*) from selected d where d.entity_sk=g.entity_sk {% if stored %}and d.signing_month is not distinct from g.signing_month{% endif %}) as selected_contract_count
from grouped g
), reasoned as (
select *,case
 {% if var('gold_min_count_coverage',none) is none or var('gold_min_value_coverage',none) is none %}when true then 'POLICY_UNCONFIGURED'{% else %}
 when {{ var('gold_min_count_coverage',none) }}::numeric not between 0 and 1 or {{ var('gold_min_value_coverage',none) }}::numeric not between 0 and 1 then 'POLICY_UNCONFIGURED'{% endif %}
 when total_contract_count=0 then 'NO_CONTRACTS'
 when entity_identity_status<>'KNOWN' then 'UNKNOWN_ENTITY'
 when known_contract_count=0 then 'NO_KNOWN_SUPPLIERS'
 when total_value_cop=0 then 'ZERO_TOTAL_VALUE'
 when known_value_cop=0 then 'ZERO_KNOWN_VALUE'
 {% if var('gold_min_count_coverage',none) is not none and var('gold_min_value_coverage',none) is not none %}
 when count_identity_coverage<{{ var('gold_min_count_coverage',none) }}::numeric or value_identity_coverage<{{ var('gold_min_value_coverage',none) }}::numeric then 'INSUFFICIENT_IDENTITY_COVERAGE'{% endif %}
 else 'AVAILABLE' end as hhi_state from measured
)
select silver_version_id,silver_manifest_sha256,scope_id,entity_sk,{% if stored %}signing_month,{% endif %}
 catalog_sha256,policy_sha256,total_contract_count,known_contract_count,total_value_cop,known_value_cop,
 count_identity_coverage,value_identity_coverage,selected_supplier_reference_share,selected_contract_count,hhi_state,
 case when known_contract_count=0 then 'NO_KNOWN_SUPPLIERS'
      when known_value_cop=0 then 'ZERO_KNOWN_VALUE'
      when selected_supplier_reference_share is null then 'NO_SELECTED_KNOWN_SUPPLIER'
      else 'AVAILABLE' end as selected_supplier_reference_share_state,
 case when hhi_state='AVAILABLE' then raw_hhi end as hhi,
 case when total_contract_count=0 then 'NO_CONTRACTS' else 'AVAILABLE' end as count_coverage_state,
 case when total_value_cop=0 then 'ZERO_TOTAL_VALUE' else 'AVAILABLE' end as value_coverage_state,
 'ALL_VALID_OBSERVED'::text as base_cohort,'KNOWN_SUPPLIER_VALUE'::text as hhi_denominator
 ,to_jsonb(parameters) as selection_metadata, (to_jsonb(parameters)-'selected_supplier') as reference_metadata
from reasoned cross join parameters cross join validated where valid=1
{% endmacro %}

{% macro gold_detail_query(relation, filters={}, bound=false) %}
with {{ gold_filtered_ctes(relation,filters,bound) }}
select selected.*,to_jsonb(parameters) as selection_metadata from selected cross join parameters
{% endmacro %}

{% macro gold_quality_query(relation, filters={}, bound=false) %}
with {{ gold_filtered_ctes(relation,filters,bound) }}
select control_id,count(contract_key) filter(where affected) as numerator,count(contract_key) as denominator,
 (select max(silver_version_id) from {{ relation }}) as silver_version_id,
 (select max(silver_manifest_sha256) from {{ relation }}) as silver_manifest_sha256,
 (select max(scope_id) from {{ relation }}) as scope_id,
 (select max(catalog_sha256) from {{ relation }}) as catalog_sha256,
 (select max(policy_sha256) from {{ relation }}) as policy_sha256,
 'UNIQUE_CONTRACT'::text as evaluation_grain,'DETAIL_SUPPORTED_FILTERS'::text as filter_applicability,
 to_jsonb(parameters) as selection_metadata
from parameters left join selected on true
cross join lateral (values
 ('missing_signing_date',signing_date is null),('unknown_supplier',supplier_identity_status<>'KNOWN'),
 ('unknown_entity',entity_identity_status<>'KNOWN'),('missing_status',status_profile_state='MISSING'),
 ('unprofiled_status',status_profile_state='UNPROFILED'),('missing_source_update',source_updated_at is null),
 ('later_invalid_observed',condition_state='LATER_INVALID_OBSERVED'),
 ('temporal_order_indeterminate',condition_state='TEMPORAL_ORDER_INDETERMINATE'),
 ('legacy_unassessed',condition_state='LEGACY_UNASSESSED'),('selected_unique_contracts',true)
) controls(control_id,affected)
group by control_id,to_jsonb(parameters)
{% endmacro %}
