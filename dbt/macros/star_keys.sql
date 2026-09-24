{% macro canonical_text(expression) -%}
nullif(lower(regexp_replace(btrim({{ expression }}), '\s+', ' ', 'g')), '')
{%- endmacro %}

{% macro star_key(namespace, natural_key) -%}
md5('{{ var("star_key_version") }}|{{ namespace }}|' || {{ natural_key }})
{%- endmacro %}

{% macro unknown_natural_key() -%}
'__UNKNOWN__'
{%- endmacro %}

{% macro reconcile_snapshot() -%}
    {%- if is_incremental() -%}
truncate table {{ this }}
    {%- else -%}
select 1
    {%- endif -%}
{%- endmacro %}
