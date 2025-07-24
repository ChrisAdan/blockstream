{% macro rolling_stddev(column_name, window_days=7) %}
    {{ rolling_metric(column_name, 'stddev', window_days) }}
{% endmacro %}