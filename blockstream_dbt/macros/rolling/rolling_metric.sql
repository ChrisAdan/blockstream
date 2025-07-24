-- Base macro that handles all types of windowed aggregations
{% macro rolling_metric(column_name, agg_func, window_days, order_by='record_date', partition_by='symbol') %}
    {{ agg_func }}({{ column_name }}) over (
        partition by {{ partition_by }}
        order by {{ order_by }}
        rows between {{ window_days - 1 }} preceding and current row
    )
{% endmacro %}
