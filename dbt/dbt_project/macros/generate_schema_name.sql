{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {# 
       LOGIQUE :
       Si un schéma est configuré (ex: 'SILVER' ou 'GOLD'), on utilise ce nom EXACTEMENT.
       Sinon, on utilise le schéma par défaut (PUBLIC).
    #}
    
    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}