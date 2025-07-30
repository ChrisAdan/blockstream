{% macro rolling_sum(column_name, window_days=7) %}
    {{ rolling_metric(column_name, 'sum', window_days) }}
{% endmacro %}