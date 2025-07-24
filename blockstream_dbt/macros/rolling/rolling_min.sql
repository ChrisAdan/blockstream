{% macro rolling_min(column_name, window_days=7) %}
    {{ rolling_metric(column_name, 'min', window_days) }}
{% endmacro %}