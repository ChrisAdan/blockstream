SELECT *
FROM {{ ref('stg_binance_us_24hr_ticker') }}
WHERE price_change != 0 AND volume = 0
