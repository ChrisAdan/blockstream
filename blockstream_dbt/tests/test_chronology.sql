SELECT *
FROM {{ ref('stg__binance_us_24hr_ticker') }}
WHERE open_time_utc >= close_time_utc