SELECT *
FROM {{ ref('stg_binance_us_24hr_ticker') }}
WHERE open_time_utc >= close_time_utc