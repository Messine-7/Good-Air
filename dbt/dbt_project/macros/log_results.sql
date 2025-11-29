{% macro log_dbt_results(results) %}
    
    {%- if execute -%}
        -- Récupère dynamiquement le nom de la base de données utilisée par dbt
        {%- set destination_db = target.database -%}
        {%- set destination_schema = 'LOGS' -%}
        
        {%- for res in results -%}
            
            {%- if res.node.resource_type == 'model' and res.status == 'success' -%}

                {%- set rows_affected = res.adapter_response['rows_affected'] | default(0) -%}
                {%- set table_relation = res.node.relation_name -%}

                -- On récupère le total
                {%- set count_query -%}
                    SELECT COUNT(*) FROM {{ table_relation }}
                {%- endset -%}
                
                {%- set count_result = run_query(count_query) -%}
                {%- set total_rows = count_result.columns[0].values()[0] -%}

                {%- set model_name = res.node.name -%}
                {%- set stage_name = 'dbt_' ~ res.node.config.schema -%} 

                -- INSERTION DYNAMIQUE (Plus de nom de base en dur)
                INSERT INTO {{ destination_db }}.{{ destination_schema }}.PIPELINE_METRICS (
                    pipeline_stage, 
                    dataset_name, 
                    rows_affected, 
                    total_rows_in_table, 
                    status
                ) 
                VALUES (
                    '{{ stage_name }}', 
                    '{{ model_name }}', 
                    {{ rows_affected }}, 
                    {{ total_rows }},
                    'SUCCESS'
                );

            {%- endif -%}
            
        {%- endfor -%}
    {%- endif -%}

{% endmacro %}