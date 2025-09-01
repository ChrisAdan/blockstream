-- macros/data_freshness_check.sql
{% macro check_data_freshness(relation, timestamp_column, max_age_hours=25) %}
  {% set freshness_query %}
    SELECT 
      MAX({{ timestamp_column }}) as latest_data,
      CURRENT_TIMESTAMP as check_time,
      EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX({{ timestamp_column }})))/3600 as hours_old
    FROM {{ relation }}
  {% endset %}
  
  {% if execute %}
    {% set results = run_query(freshness_query) %}
    {% if results %}
      {% set hours_old = results.columns[2].values()[0] %}
      {% if hours_old > max_age_hours %}
        {{ log("⚠️ DATA FRESHNESS WARNING: " ~ relation ~ " is " ~ hours_old ~ " hours old", info=true) }}
      {% else %}
        {{ log("✅ Data freshness OK: " ~ hours_old ~ " hours old", info=true) }}
      {% endif %}
    {% endif %}
  {% endif %}
{% endmacro %}