{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if var('publication_id', '') -%}
        {%- set publication_id = var('publication_id') -%}
        {%- if not modules.re.fullmatch('[0-9a-f]{32}', publication_id) -%}
            {{ exceptions.raise_compiler_error('Invalid reserved publication identity') }}
        {%- endif -%}
        candidate_{{ publication_id }}_{{ custom_schema_name | trim if custom_schema_name else 'analytics' }}
    {%- elif custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
