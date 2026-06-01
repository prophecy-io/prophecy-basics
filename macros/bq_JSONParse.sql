{% macro JSONParse(relation_name, columnName, parsingMethod, sampleRecord, sampleSchema, generated_sql) %}
  {% if generated_sql and generated_sql.strip() and not generated_sql.startswith("-- Error") %}

    {% if generated_sql.strip().startswith("JSON_EXTRACT(") %}
      {# Handle arrays - no STRUCT wrapper needed #}
      SELECT 
        *,
        {{ generated_sql }} AS {{ columnName }}_parsed
      FROM {{ relation_name }}

    {% else %}
      {# Handle structs - wrap in STRUCT #}
      SELECT 
        *,
        STRUCT(
          {{ generated_sql }}
        ) AS {{ columnName }}_parsed
      FROM {{ relation_name }}
    {% endif %}

  {% else %}
    SELECT * FROM {{ relation_name }}
  {% endif %}

{% endmacro %}
