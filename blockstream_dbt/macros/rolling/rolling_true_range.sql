{% macro rolling_true_range(high_col, low_col, window_days=7) %}
    max({{ high_col }}) over (
        partition by symbol
        order by record_date
        rows between {{ window_days - 1 }} preceding and current row
    ) -
    min({{ low_col }}) over (
        partition by symbol
        order by record_date
        rows between {{ window_days - 1 }} preceding and current row
    )
{% endmacro %}
