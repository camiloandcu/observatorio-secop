{% macro gold_source_binding() %}
jsonb_build_object(
 'contracts',(select jsonb_agg(distinct jsonb_build_array(silver_version_id,silver_manifest_sha256)) from {{ source('silver','contract_current') }}),
 'population',(select coalesce(jsonb_agg(to_jsonb(p) order by silver_version_id,manifest_sha256),'[]'::jsonb) from {{ source('silver','contract_current_population') }} p),
 'quality',(select coalesce(jsonb_agg(to_jsonb(q) order by silver_version_id,manifest_sha256),'[]'::jsonb) from {{ source('silver','contract_current_quality') }} q),
 'scope',(select coalesce(jsonb_agg(to_jsonb(b)),'[]'::jsonb) from {{ source('silver','contract_current_population_binding') }} b),
 'conditions',(select coalesce(jsonb_agg(to_jsonb(c) order by silver_version_id,manifest_sha256,contract_key),'[]'::jsonb) from {{ source('silver','contract_current_observation_condition') }} c),
 'links',(select coalesce(jsonb_agg(to_jsonb(l) order by silver_version_id,manifest_sha256,contract_key,link_id),'[]'::jsonb) from {{ source('silver','contract_current_observation_link') }} l))
{% endmacro %}

{% macro gold_capture_build() %}
{% if execute and flags.WHICH in ['run','build'] %}
{% set build_schema = generate_schema_name('analytics', none) | trim %}
select case when (select count(*) from (select distinct silver_version_id,silver_manifest_sha256 from {{ source('silver','contract_current') }}) identities)=1
 then '1' else 'GOLD_MIXED_OR_EMPTY_SOURCE' end::integer;
{% if var('publication_id', none) is none %}
create schema if not exists {{ adapter.quote(build_schema) }};
{% endif %}
create table if not exists {{ adapter.quote(build_schema) }}._gold_build_binding (invocation_id text primary key, binding jsonb not null);
insert into {{ adapter.quote(build_schema) }}._gold_build_binding select '{{ invocation_id }}',{{ gold_source_binding() }}
on conflict(invocation_id) do update set binding=excluded.binding;
{% else %}select 1;{% endif %}
{% endmacro %}

{% macro gold_verify_build() %}
{% if execute and flags.WHICH in ['run','build'] %}
{% set build_schema = generate_schema_name('analytics', none) | trim %}
select case when binding = {{ gold_source_binding() }} then '1' else 'GOLD_INPUT_DRIFT' end::integer
from {{ adapter.quote(build_schema) }}._gold_build_binding where invocation_id='{{ invocation_id }}';
delete from {{ adapter.quote(build_schema) }}._gold_build_binding where invocation_id='{{ invocation_id }}';
{% else %}select 1;{% endif %}
{% endmacro %}
