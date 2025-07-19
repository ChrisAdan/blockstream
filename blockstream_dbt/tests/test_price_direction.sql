SELECT *
FROM {{ ref('stg_binance_us_24hr_ticker') }}
WHERE
    high_price < low_price
    OR last_price < low_price
    OR last_price > high_price