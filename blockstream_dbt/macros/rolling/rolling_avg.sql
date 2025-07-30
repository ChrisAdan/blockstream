{% macro rolling_avg(column_name, window_days=7) %}
    {{ rolling_metric(column_name, 'avg', window_days) }}
{% endmacro %}