SELECT *
FROM {{ ref('stg__binance_us_24hr_ticker') }}
WHERE first_trade_id > last_trade_id