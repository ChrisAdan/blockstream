{{ config(materialized='view') }}

with exploded as (
    select
        id as record_date,
        raw_response[i] as obj
    from {{ source('raw', 'raw_binance_us_24hr_ticker') }},
         range(0, length(raw_response)) as t(i)
),

typed as (
    select
        record_date,
        obj->>'symbol' as symbol,
        obj->>'priceChange'::float as price_change,
        obj->>'priceChangePercent'::float as price_change_percent,
        obj->>'weightedAvgPrice'::float as weighted_avg_price,
        obj->>'prevClosePrice'::float as prev_close_price,
        obj->>'lastPrice'::float as last_price,
        obj->>'lastQty'::float as last_qty,
        obj->>'bidPrice'::float as bid_price,
        obj->>'bidQty'::float as bid_qty,
        obj->>'askPrice'::float as ask_price,
        obj->>'askQty'::float as ask_qty,
        obj->>'openPrice'::float as open_price,
        obj->>'highPrice'::float as high_price,
        obj->>'lowPrice'::float as low_price,
        obj->>'volume'::float as volume,
        obj->>'quoteVolume'::float as quote_volume,
        (epoch_ms((obj->>'openTime')::BIGINT) AT TIME ZONE 'UTC') AS open_time_utc,
        (epoch_ms((obj->>'closeTime')::BIGINT) AT TIME ZONE 'UTC') AS close_time_utc
        obj->>'firstId'::bigint as first_trade_id,
        obj->>'lastId'::bigint as last_trade_id,
        obj->>'count'::int as trade_count
    from exploded
)

select * from typed
