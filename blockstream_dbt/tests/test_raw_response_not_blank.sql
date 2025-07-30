SELECT
  id,
  raw_response
FROM {{ source('raw', 'raw_binance_us_24hr_ticker') }}
WHERE raw_response IS NULL
   OR TRIM(raw_response) = '[]'
   OR TRIM(raw_response) = ''
limit 10