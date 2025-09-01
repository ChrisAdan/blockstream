{{ config(
    materialized='incremental',
    unique_key=['symbol', 'record_date'],
    incremental_strategy='merge',
    partition_by=['record_date'],
    pre_hook="{{ log('Processing staging data for date: ' ~ var('target_date', 'current'), info=true) }}"
) }}

with source_data as (
    select
        id as record_date,
        raw_response,
        created_at as ingestion_timestamp
    from {{ source('raw', 'raw_binance_us_24hr_ticker') }}
    
    {% if is_incremental() %}
        where id > (select max(record_date) from {{ this }})
    {% endif %}
),

parsed_json as (
    select
        record_date,
        ingestion_timestamp,
        case 
            when raw_response is null or trim(raw_response) = '' then null
            else try_cast(raw_response as json)
        end as parsed_response
    from source_data
    where raw_response is not null 
      and trim(raw_response) != ''
      and trim(raw_response) != '[]'
),

exploded_tickers as (
    select
        record_date,
        ingestion_timestamp,
        parsed_response -> i as ticker_obj,
        i as array_index
    from parsed_json,
         range(0, coalesce(json_array_length(parsed_response), 0)::bigint) as t(i)
    where parsed_response is not null
),

cleaned_and_typed as (
    select
        -- Identifiers
        record_date,
        ticker_obj ->> 'symbol' as symbol,
        ingestion_timestamp,
        array_index,
        
        -- Price metrics
        try_cast(ticker_obj ->> 'priceChange' as decimal(18,8)) as price_change,
        try_cast(ticker_obj ->> 'priceChangePercent' as decimal(10,4)) as price_change_percent,
        try_cast(ticker_obj ->> 'weightedAvgPrice' as decimal(18,8)) as weighted_avg_price,
        try_cast(ticker_obj ->> 'prevClosePrice' as decimal(18,8)) as prev_close_price,
        try_cast(ticker_obj ->> 'lastPrice' as decimal(18,8)) as last_price,
        try_cast(ticker_obj ->> 'lastQty' as decimal(18,8)) as last_qty,
        
        -- Order book
        try_cast(ticker_obj ->> 'bidPrice' as decimal(18,8)) as bid_price,
        try_cast(ticker_obj ->> 'bidQty' as decimal(18,8)) as bid_qty,
        try_cast(ticker_obj ->> 'askPrice' as decimal(18,8)) as ask_price,
        try_cast(ticker_obj ->> 'askQty' as decimal(18,8)) as ask_qty,
        
        -- OHLC
        try_cast(ticker_obj ->> 'openPrice' as decimal(18,8)) as open_price,
        try_cast(ticker_obj ->> 'highPrice' as decimal(18,8)) as high_price,
        try_cast(ticker_obj ->> 'lowPrice' as decimal(18,8)) as low_price,
        
        -- Volume
        try_cast(ticker_obj ->> 'volume' as decimal(18,8)) as volume,
        try_cast(ticker_obj ->> 'quoteVolume' as decimal(18,8)) as quote_volume,
        
        -- Timestamps
        epoch_ms(try_cast(ticker_obj ->> 'openTime' as bigint)) as open_time_utc,
        epoch_ms(try_cast(ticker_obj ->> 'closeTime' as bigint)) as close_time_utc,
        
        -- Trade info
        try_cast(ticker_obj ->> 'firstId' as bigint) as first_trade_id,
        try_cast(ticker_obj ->> 'lastId' as bigint) as last_trade_id,
        try_cast(ticker_obj ->> 'count' as integer) as trade_count
        
    from exploded_tickers
    where ticker_obj ->> 'symbol' is not null
      and ticker_obj ->> 'symbol' != ''
),

validated as (
    select
        *,
        -- Data quality flags
        case when high_price < low_price then true else false end as price_anomaly_flag,
        case when open_time_utc >= close_time_utc then true else false end as time_anomaly_flag,
        case when first_trade_id > last_trade_id then true else false end as trade_id_anomaly_flag,
        case when volume = 0 and abs(price_change_percent) > 0.01 then true else false end as volume_anomaly_flag
        
    from cleaned_and_typed
    where symbol is not null
      and last_price > 0
      and high_price >= low_price  -- Basic validation
      and open_time_utc < close_time_utc
),

final as (
    select
        -- Primary keys
        record_date,
        symbol,
        
        -- Core metrics
        price_change,
        price_change_percent,
        weighted_avg_price,
        prev_close_price,
        last_price,
        last_qty,
        
        -- Order book
        bid_price,
        bid_qty,
        ask_price,
        ask_qty,
        
        -- OHLC
        open_price,
        high_price,
        low_price,
        
        -- Volume
        volume,
        quote_volume,
        trade_count,
        
        -- Timestamps
        open_time_utc,
        close_time_utc,
        ingestion_timestamp,
        
        -- Trade IDs
        first_trade_id,
        last_trade_id,
        
        -- Derived fields
        ask_price - bid_price as bid_ask_spread,
        case when last_price > 0 
             then (ask_price - bid_price) / last_price 
             else null 
        end as spread_percentage,
        
        case when volume > 0 
             then quote_volume / volume 
             else null 
        end as avg_trade_price,
        
        -- Data quality flags
        price_anomaly_flag,
        time_anomaly_flag,
        trade_id_anomaly_flag,
        volume_anomaly_flag,
        
        -- Row quality score (0-1, where 1 is perfect)
        1.0 - (
            cast(price_anomaly_flag as int) + 
            cast(time_anomaly_flag as int) + 
            cast(trade_id_anomaly_flag as int) + 
            cast(volume_anomaly_flag as int)
        ) * 0.25 as data_quality_score
        
    from validated
)

select * from final