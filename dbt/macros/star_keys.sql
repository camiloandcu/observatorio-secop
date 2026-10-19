{% macro canonical_text(expression) -%}
nullif(lower(regexp_replace(btrim({{ expression }}), '\s+', ' ', 'g')), '')
{%- endmacro %}

{% macro star_key(namespace, natural_key) -%}
md5('{{ var("star_key_version") }}|{{ namespace }}|' || {{ natural_key }})
{%- endmacro %}

{% macro unknown_natural_key() -%}
'__UNKNOWN__'
{%- endmacro %}

{% macro platform_natural_key(role) -%}
case when {{ role }}_identity_status = 'KNOWN'
    then jsonb_build_array(source_dataset_id, '{{ role }}', {{ role }}_code)::text
    else {{ unknown_natural_key() }}
end
{%- endmacro %}

{% macro platform_star_key(role, natural_key) -%}
case when {{ natural_key }} = {{ unknown_natural_key() }}
    then {{ star_key(role, natural_key) }}
    else md5('{{ var("identity_key_version") }}|{{ role }}|' || {{ natural_key }})
end
{%- endmacro %}

{% macro reconcile_snapshot() -%}
    {%- if is_incremental() -%}
truncate table {{ this }}
    {%- else -%}
select 1
    {%- endif -%}
{%- endmacro %}
