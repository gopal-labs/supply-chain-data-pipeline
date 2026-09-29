{#
  Use the custom schema name exactly as configured (staging, marts) instead of
  dbt default "<target_schema>_<custom_schema>" prefixing, so models land in
  clean staging and marts schemas.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
