{% macro rolling_max(column_name, window_days=7) %}
    {{ rolling_metric(column_name, 'max', window_days) }}
{% endmacro %}