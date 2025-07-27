SELECT *
FROM {{ ref('stg__binance_us_24hr_ticker') }}
WHERE price_change != 0 AND volume = 0
