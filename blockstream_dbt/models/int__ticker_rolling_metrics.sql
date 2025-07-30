{{ config(
    materialized = 'incremental',
    unique_key = 'symbol || record_date'
) }}

-- int__ticker_rolling_metrics
-- Generates rolling aggregates (avg, sum, stddev) and derived indicators 
-- for Binance US 24hr ticker data.

{% set windows = [7, 14, 28] %}

{# Define all metrics and their applicable aggregations. Special metrics have custom agg functions listed #}
{% set metric_aggs = {
    'last_price': ['avg', 'stddev'],
    'high_price': ['avg', 'stddev'],
    'low_price': ['avg', 'stddev'],
    'volume': ['avg', 'sum', 'stddev'],
    'quote_volume': ['avg', 'sum', 'stddev'],
    'trade_count': ['avg', 'sum', 'stddev'],
    'bid_qty': ['avg', 'stddev'],
    'ask_qty': ['avg', 'stddev'],
    'true_range': ['true_range'],
    'max_drawdown': ['max_drawdown']
} %}

with base as (

    select
        record_date,
        symbol,
        price_change,
        price_change_percent,
        weighted_avg_price,
        prev_close_price,
        last_price,
        last_qty,
        bid_price,
        bid_qty,
        ask_price,
        ask_qty,
        open_price,
        high_price,
        low_price,
        volume,
        quote_volume,
        open_time_utc,
        close_time_utc,
        first_trade_id,
        last_trade_id,
        trade_count
    from {{ ref('stg__binance_us_24hr_ticker') }}
    {% if is_incremental() %}
        where record_date > (select max(record_date) from {{ this }})
    {% endif %}

),

rolling_metrics as (

    select
        base.*,

        {# Calculate rolling metrics for each metric and window #}
        {% set metric_names = metric_aggs.keys() | list %}
        {% for m_idx in range(metric_names | length) %}
        {% set metric = metric_names[m_idx] %}
        {% set aggs = metric_aggs[metric] %}
        {% for w_idx in range(windows | length) %}
            {% set window = windows[w_idx] %}

            {% for a_idx in range(aggs | length) %}
                {% set agg = aggs[a_idx] %}
                {%- set is_last_metric = (m_idx == (metric_names | length - 1)) %}
                {%- set is_last_window = (w_idx == (windows | length - 1)) %}
                {%- set is_last_agg = (a_idx == (aggs | length - 1)) %}
                {%- set is_last_item = is_last_metric and is_last_window and is_last_agg %}

                {% if agg == 'true_range' %}
                    {{ rolling_true_range('high_price', 'low_price', window) }} as true_range_{{ window }}d
                    {%- if not is_last_item %},{% endif %}
                {% elif agg == 'max_drawdown' %}
                    {{ rolling_max_drawdown('last_price', window) }} as max_drawdown_{{ window }}d
                    {%- if not is_last_item %},{% endif %}
                {% elif agg == 'avg' %}
                    {{ rolling_avg(metric, window) }} as {{ metric }}_avg_{{ window }}d
                    {%- if not is_last_item %},{% endif %}
                {% elif agg == 'sum' %}
                    {{ rolling_sum(metric, window) }} as {{ metric }}_sum_{{ window }}d
                    {%- if not is_last_item %},{% endif %}
                {% elif agg == 'stddev' %}
                    {{ rolling_stddev(metric, window) }} as {{ metric }}_stddev_{{ window }}d
                    {%- if not is_last_item %},{% endif %}
                {% endif %}

            {% endfor %}
        {% endfor %}
    {% endfor %}


    from base

),

derived_metrics as (

    select
        *,
        
        -- Relative Change vs avg
        (last_price - last_price_avg_7d) / nullif(last_price_avg_7d, 0) as pct_change_7d,

        -- Liquidity Ratio
        volume_avg_7d / nullif(last_price_avg_7d, 0) as liquidity_ratio,

        -- Ratios
        last_price / nullif(volume, 0) as price_to_volume_ratio,
        ask_price - bid_price as bid_ask_spread,
        (ask_price - bid_price) / nullif(last_price, 0) as spread_pct,

        -- Efficiency: Movement vs range
        abs(price_change) / nullif((high_price_avg_7d - low_price_avg_7d), 0) as price_efficiency_ratio,

        -- Order Book Depth Ratios
        case when ask_qty_avg_7d + bid_qty_avg_7d > 0
            then bid_qty_avg_7d / (bid_qty_avg_7d + ask_qty_avg_7d)
            else null
        end as bid_ratio_7d,

        case when ask_qty_avg_7d + bid_qty_avg_7d > 0
            then ask_qty_avg_7d / (bid_qty_avg_7d + ask_qty_avg_7d)
            else null
        end as ask_ratio_7d

    from rolling_metrics

),

ranked as (

    select
        *,
        dense_rank() over (partition by record_date order by pct_change_7d desc) as rank_price_increase_7d,
        dense_rank() over (partition by record_date order by last_price_stddev_7d desc) as rank_volatility_7d,
        dense_rank() over (partition by record_date order by volume_sum_7d desc) as rank_volume_7d,
        dense_rank() over (partition by record_date order by liquidity_ratio desc) as rank_liquidity_7d,
        dense_rank() over (partition by record_date order by trade_count_sum_7d desc) as rank_trade_activity_7d

    from derived_metrics

)

select * from ranked
