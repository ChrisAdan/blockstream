{{ config(
    materialized = 'incremental',
    unique_key = 'symbol || record_date'
) }}

{% set basic_metrics = ['last_price', 'high_price', 'low_price', 'volume', 'quote_volume', 'trade_count', 'bid_qty', 'ask_qty'] %}
{% set special_metrics = [
    {'name': 'true_range', 'cols': ['high_price', 'low_price']},
    {'name': 'max_drawdown', 'cols': ['last_price']}
] %}
{% set windows = [7, 14, 28] %}

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
    from {{ ref('stg_binance_us_24hr_ticker') }}
    {% if is_incremental() %}
        where record_date > (select max(record_date) from {{ this }})
    {% endif %}

),

rolling_metrics as (

    select
        base.*,

        -- BASIC METRICS: Rolling avg, sum, stddev
        {% for col in basic_metrics %}
            {% for w in windows %}
                {{ rolling_avg(col, w) }} as {{ col }}_avg_{{ w }}d,
                {{ rolling_sum(col, w) }} as {{ col }}_sum_{{ w }}d,
                {{ rolling_stddev(col, w) }} as {{ col }}_stddev_{{ w }}d
                {% if not loop.last or not loop.parent.last %}, {% endif %}
            {% endfor %}
        {% endfor %}

        -- SPECIAL METRICS: true_range and max_drawdown
        {% for metric in special_metrics %}
            {% set name = metric.name %}
            {% set cols = metric.cols %}
            {% for w in windows %}
                {% if name == 'true_range' %}
                    , {{ rolling_true_range(cols[0], cols[1], w) }} as {{ name }}_{{ w }}d
                {% elif name == 'max_drawdown' %}
                    , {{ rolling_max_drawdown(cols[0], w) }} as {{ name }}_{{ w }}d
                {% endif %}
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
        abs(price_change) / nullif((high_price_7d - low_price_7d), 0) as price_efficiency_ratio,

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
