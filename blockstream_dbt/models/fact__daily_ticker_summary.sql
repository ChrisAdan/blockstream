{{ config(
    materialized='table',
    indexes=[
      {'columns': ['date_key', 'symbol'], 'unique': true},
      {'columns': ['date_key']},
      {'columns': ['symbol']},
      {'columns': ['market_cap_rank']}
    ]
) }}

-- fact__daily_ticker_summary
-- Analytics-ready mart combining core ticker data with rolling metrics
-- Designed for dashboards, reporting, and further analysis

with base_metrics as (
    select
        -- Date and symbol keys
        strftime(open_time_utc, '%Y-%m-%d') as date_key,
        symbol,
        record_date,
        
        -- Core price data
        last_price as price_usd,
        price_change_percent as daily_return_pct,
        volume as volume_24h,
        quote_volume as volume_usd_24h,
        
        -- OHLC
        open_price,
        high_price,
        low_price,
        
        -- Trading activity
        trade_count,
        
        -- Order book metrics
        bid_price,
        ask_price,
        spread_percentage,
        
        -- Data quality
        data_quality_score
        
    from {{ ref('stg__binance_us_24hr_ticker') }}
),

enriched_with_rolling as (
    select
        base.*,
        
        -- Rolling price metrics
        rolling.last_price_avg_7d as price_avg_7d,
        rolling.last_price_avg_14d as price_avg_14d,
        rolling.last_price_stddev_7d as volatility_7d,
        rolling.last_price_stddev_14d as volatility_14d,
        
        -- Rolling volume metrics  
        rolling.volume_sum_7d as volume_7d_sum,
        rolling.volume_avg_7d as volume_7d_avg,
        rolling.quote_volume_sum_7d as volume_usd_7d_sum,
        
        -- Momentum indicators
        rolling.pct_change_7d as momentum_7d,
        rolling.price_efficiency_ratio,
        
        -- Liquidity metrics
        rolling.liquidity_ratio,
        rolling.bid_ratio_7d,
        rolling.ask_ratio_7d,
        
        -- Market rankings
        rolling.rank_price_increase_7d,
        rolling.rank_volatility_7d, 
        rolling.rank_volume_7d,
        rolling.rank_liquidity_7d,
        rolling.rank_trade_activity_7d
        
    from base_metrics base
    inner join {{ ref('int__ticker_rolling_metrics') }} rolling
        on base.symbol = rolling.symbol 
        and base.record_date = rolling.record_date
),

market_context as (
    select
        *,
        
        -- Market cap proxy ranking (volume * price as rough proxy)
        dense_rank() over (
            partition by date_key 
            order by (volume_usd_24h * price_usd) desc nulls last
        ) as market_cap_rank,
        
        -- Percentile rankings for key metrics
        percent_rank() over (
            partition by date_key 
            order by daily_return_pct
        ) as return_percentile,
        
        percent_rank() over (
            partition by date_key 
            order by volatility_7d
        ) as volatility_percentile,
        
        percent_rank() over (
            partition by date_key 
            order by volume_usd_24h
        ) as volume_percentile,
        
        -- Market classification
        case 
            when dense_rank() over (partition by date_key order by volume_usd_24h desc) <= 10 
            then 'Major'
            when dense_rank() over (partition by date_key order by volume_usd_24h desc) <= 50
            then 'Mid-Cap' 
            else 'Small-Cap'
        end as market_tier,
        
        -- Volatility classification
        case 
            when volatility_7d > 0.15 then 'High'
            when volatility_7d > 0.05 then 'Medium'  
            else 'Low'
        end as volatility_tier,
        
        -- Performance classification
        case
            when daily_return_pct > 10 then 'Strong Gain'
            when daily_return_pct > 2 then 'Moderate Gain'
            when daily_return_pct > -2 then 'Stable'
            when daily_return_pct > -10 then 'Moderate Loss'
            else 'Strong Loss'
        end as performance_tier
        
    from enriched_with_rolling
),

final as (
    select
        -- Primary keys
        date_key,
        symbol,
        
        -- Core metrics
        price_usd,
        daily_return_pct,
        volume_24h,
        volume_usd_24h,
        trade_count,
        
        -- OHLC
        open_price,
        high_price, 
        low_price,
        
        -- Rolling metrics
        price_avg_7d,
        price_avg_14d,
        volatility_7d,
        volatility_14d,
        volume_7d_sum,
        volume_usd_7d_sum,
        
        -- Momentum & Technical
        momentum_7d,
        price_efficiency_ratio,
        
        -- Liquidity & Order Book
        liquidity_ratio,
        spread_percentage,
        bid_ratio_7d,
        ask_ratio_7d,
        
        -- Rankings
        market_cap_rank,
        rank_price_increase_7d,
        rank_volatility_7d,
        rank_volume_7d,
        
        -- Percentiles (0-1 scale)
        return_percentile,
        volatility_percentile, 
        volume_percentile,
        
        -- Classifications
        market_tier,
        volatility_tier,
        performance_tier,
        
        -- Quality metrics
        data_quality_score,
        
        -- Metadata
        record_date as source_record_date,
        current_timestamp as dbt_updated_at
        
    from market_context
    where data_quality_score >= 0.75  -- Filter out poor quality data
)

select * from final