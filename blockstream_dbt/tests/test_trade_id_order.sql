SELECT *
FROM {{ ref('stg_binance_us_24hr_ticker') }}
WHERE first_trade_id > last_trade_id