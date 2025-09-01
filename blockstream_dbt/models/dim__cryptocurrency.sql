{{ config(
    materialized='table',
    unique_key='symbol'
) }}

-- dim__cryptocurrency
-- Slowly changing dimension for cryptocurrency metadata and lifecycle tracking

with symbol_lifecycle as (
    select
        symbol,
        min(strftime(open_time_utc, '%Y-%m-%d')) as first_seen_date,
        max(strftime(open_time_utc, '%Y-%m-%d')) as last_seen_date,
        count(distinct strftime(open_time_utc, '%Y-%m-%d')) as days_active,
        
        -- Recent activity check (last 7 days)
        max(case when open_time_utc >= current_date - interval '7 days' 
                 then 1 else 0 end) as active_last_7d,
        
        -- Data quality metrics
        avg(data_quality_score) as avg_data_quality,
        min(data_quality_score) as min_data_quality
        
    from {{ ref('stg__binance_us_24hr_ticker') }}
    group by symbol
),

symbol_parsing as (
    select
        symbol,
        first_seen_date,
        last_seen_date,
        days_active,
        active_last_7d,
        avg_data_quality,
        min_data_quality,
        
        -- Parse base and quote assets from symbol
        case 
            when symbol like '%USD' then left(symbol, length(symbol) - 3)
            when symbol like '%USDT' then left(symbol, length(symbol) - 4)
            when symbol like '%USDC' then left(symbol, length(symbol) - 4)
            when symbol like '%BTC' then left(symbol, length(symbol) - 3)
            when symbol like '%ETH' then left(symbol, length(symbol) - 3)
            else symbol
        end as base_asset,
        
        case 
            when symbol like '%USD' and not (symbol like '%USDT' or symbol like '%USDC') then 'USD'
            when symbol like '%USDT' then 'USDT'
            when symbol like '%USDC' then 'USDC'  
            when symbol like '%BTC' then 'BTC'
            when symbol like '%ETH' then 'ETH'
            else 'UNKNOWN'
        end as quote_asset
        
    from symbol_lifecycle
),

asset_classification as (
    select
        *,
        
        -- Classify asset categories
        case 
            when base_asset in ('BTC', 'ETH') then 'Major'
            when base_asset in ('ADA', 'SOL', 'DOT', 'AVAX', 'MATIC', 'LINK', 'ATOM') then 'Large Cap'
            when base_asset in ('USDT', 'USDC', 'BUSD', 'DAI') then 'Stablecoin'
            when quote_asset = 'USD' and days_active >= 30 then 'Established Altcoin'
            when quote_asset = 'USD' and days_active < 30 then 'New Altcoin'
            else 'Other'
        end as asset_category,
        
        -- Determine if currently active
        case 
            when active_last_7d = 1 and last_seen_date >= current_date - interval '2 days' then true
            else false
        end as is_active,
        
        -- Trading pair type
        case
            when quote_asset = 'USD' then 'Fiat Pair'
            when quote_asset in ('USDT', 'USDC') then 'Stablecoin Pair'
            when quote_asset in ('BTC', 'ETH') then 'Crypto Pair'
            else 'Other Pair'
        end as pair_type,
        
        -- Data reliability score
        case 
            when avg_data_quality >= 0.95 and min_data_quality >= 0.8 then 'High'
            when avg_data_quality >= 0.85 and min_data_quality >= 0.6 then 'Medium'
            else 'Low'
        end as data_reliability
        
    from symbol_parsing
),

final as (
    select
        -- Natural key
        symbol,
        
        -- Asset breakdown
        base_asset,
        quote_asset,
        
        -- Lifecycle tracking
        first_seen_date,
        last_seen_date,
        days_active,
        is_active,
        
        -- Classification
        asset_category,
        pair_type,
        data_reliability,
        
        -- Quality metrics
        round(avg_data_quality, 4) as avg_data_quality_score,
        round(min_data_quality, 4) as min_data_quality_score,
        
        -- Derived attributes
        case 
            when asset_category in ('Major', 'Large Cap') then true 
            else false 
        end as is_major_asset,
        
        case 
            when quote_asset = 'USD' then true 
            else false 
        end as is_usd_quoted,
        
        case
            when days_active >= 365 then 'Mature'
            when days_active >= 90 then 'Established'  
            when days_active >= 30 then 'Growing'
            else 'New'
        end as maturity_stage,
        
        -- Metadata
        current_timestamp as last_updated,
        '{{ run_started_at }}' as dbt_run_timestamp
        
    from asset_classification
)

select * from final
order by 
    case asset_category
        when 'Major' then 1
        when 'Large Cap' then 2  
        when 'Stablecoin' then 3
        when 'Established Altcoin' then 4
        else 5
    end,
    days_active desc,
    symbol