{% macro rolling_max_drawdown(column_name, window_days=7) %}
    (
        max({{ column_name }}) over (
            partition by symbol
            order by record_date
            rows between {{ window_days - 1 }} preceding and current row
        ) - {{ column_name }}
    ) / nullif(
        max({{ column_name }}) over (
            partition by symbol
            order by record_date
            rows between {{ window_days - 1 }} preceding and current row
        ), 0
    )
{% endmacro %}
