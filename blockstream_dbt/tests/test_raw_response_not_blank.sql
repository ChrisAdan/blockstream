SELECT
  id,
  raw_response[i] as missing_field,
  i as index_in_array
from {{ source('raw', 'raw_binance_us_24hr_ticker') }},
     range(0, length(raw_response)) as t(i)
WHERE raw_response[i] IS NULL OR trim(raw_response[i]) = ''
limit 10
