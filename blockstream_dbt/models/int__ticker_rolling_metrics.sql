{{ config(
    materialized='incremental',
    unique_key='symbol || record_date'
) }}

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
        *,
        -- Prices
        {{ rolling_avg_price('last_price', 7) }} as avg_price_7d,
        {{ rolling_avg_price('last_price', 14) }} as avg_price_14d,
        {{ rolling_avg_price('last_price', 28) }} as avg_price_28d,

        {{ rolling_max('high_price', 7) }} as high_price_7d,
        {{ rolling_min('low_price', 7) }} as low_price_7d,
        {{ rolling_stddev('last_price', 7) }} as price_volatility_7d,

        -- Volume & Liquidity
        {{ rolling_sum('volume', 7) }} as total_volume_7d,
        {{ rolling_avg('volume', 7) }} as avg_daily_volume_7d,
        {{ rolling_stddev('volume', 7) }} as volume_volatility_7d,

        -- Quote volume (USD equivalent)
        {{ rolling_sum('quote_volume', 7) }} as total_quote_volume_7d,
        {{ rolling_avg('quote_volume', 7) }} as avg_quote_volume_7d,

        -- Trades
        {{ rolling_sum('trade_count', 7) }} as total_trades_7d,
        {{ rolling_avg('trade_count', 7) }} as avg_trades_7d,

        -- Spread & Bid/Ask Ratios
        {{ rolling_avg('bid_qty', 7) }} as avg_bid_qty_7d,
        {{ rolling_avg('ask_qty', 7) }} as avg_ask_qty_7d,

        -- Volatility / Risk
        {{ rolling_true_range('high_price', 'low_price', 7) }} as true_range_7d,
        {{ rolling_stddev('last_price', 7) }} as price_stddev_7d,
        {{ rolling_max_drawdown('last_price', 7) }} as price_drawdown_7d

    from base

),

derived_metrics as (

    select
        *,
        -- Relative Change
        (last_price - avg_price_7d) / nullif(avg_price_7d, 0) as pct_change_7d,

        -- Liquidity Ratio
        avg_daily_volume_7d / nullif(avg_price_7d, 0) as liquidity_ratio,

        -- Ratios
        last_price / nullif(volume, 0) as price_to_volume_ratio,
        ask_price - bid_price as bid_ask_spread,
        (ask_price - bid_price) / nullif(last_price, 0) as spread_pct,

        -- Efficiency
        abs(price_change) / nullif((high_price_7d - low_price_7d), 0) as price_efficiency_ratio,

        -- Order book depth
        case when avg_ask_qty_7d + avg_bid_qty_7d > 0
            then avg_bid_qty_7d / (avg_bid_qty_7d + avg_ask_qty_7d)
            else null
        end as bid_ratio_7d,

        case when avg_ask_qty_7d + avg_bid_qty_7d > 0
            then avg_ask_qty_7d / (avg_bid_qty_7d + avg_ask_qty_7d)
            else null
        end as ask_ratio_7d

    from rolling_metrics

),

ranked as (

    select
        *,
        dense_rank() over (partition by record_date order by pct_change_7d desc) as rank_price_increase_7d,
        dense_rank() over (partition by record_date order by price_stddev_7d desc) as rank_volatility_7d,
        dense_rank() over (partition by record_date order by total_volume_7d desc) as rank_volume_7d,
        dense_rank() over (partition by record_date order by liquidity_ratio desc) as rank_liquidity_7d,
        dense_rank() over (partition by record_date order by total_trades_7d desc) as rank_trade_activity_7d

    from derived_metrics

)

select * from ranked
