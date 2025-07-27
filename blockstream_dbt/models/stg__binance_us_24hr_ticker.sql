{{ config(
  materialized='incremental',
  unique_key='symbol || date',
  incremental_strategy='insert_overwrite',
  partition_by={'field': 'date', 'data_type': 'date'}
) }}

with parsed as (
    select
        id as record_date,
        try_cast(raw_response as JSON) as parsed_json
    from {{ source('raw', 'raw_binance_us_24hr_ticker') }}
),

exploded as (
    select
        record_date,
        parsed_json -> i as obj
    from parsed,
         range(0, json_array_length(parsed_json)::bigint) as t(i)
),

typed as (
    select
        record_date,
        obj ->> 'symbol' as symbol,
        (obj ->> 'priceChange')::FLOAT as price_change,
        (obj ->> 'priceChangePercent')::FLOAT as price_change_percent,
        (obj ->> 'weightedAvgPrice')::FLOAT as weighted_avg_price,
        (obj ->> 'prevClosePrice')::FLOAT as prev_close_price,
        (obj ->> 'lastPrice')::FLOAT as last_price,
        (obj ->> 'lastQty')::FLOAT as last_qty,
        (obj ->> 'bidPrice')::FLOAT as bid_price,
        (obj ->> 'bidQty')::FLOAT as bid_qty,
        (obj ->> 'askPrice')::FLOAT as ask_price,
        (obj ->> 'askQty')::FLOAT as ask_qty,
        (obj ->> 'openPrice')::FLOAT as open_price,
        (obj ->> 'highPrice')::FLOAT as high_price,
        (obj ->> 'lowPrice')::FLOAT as low_price,
        (obj ->> 'volume')::FLOAT as volume,
        (obj ->> 'quoteVolume')::FLOAT as quote_volume,
        (epoch_ms((obj ->> 'openTime')::BIGINT) AT TIME ZONE 'UTC') as open_time_utc,
        (epoch_ms((obj ->> 'closeTime')::BIGINT) AT TIME ZONE 'UTC') as close_time_utc,
        (obj ->> 'firstId')::BIGINT as first_trade_id,
        (obj ->> 'lastId')::BIGINT as last_trade_id,
        (obj ->> 'count')::INT as trade_count
    from exploded
)

select *
from typed

{% if is_incremental() %}
where date(open_time_utc) > (
    select strptime(cast(max(record_date) as varchar), '%Y%m%d')
    from {{ this }}
)
{% endif %}
