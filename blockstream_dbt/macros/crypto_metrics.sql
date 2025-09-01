-- macros/crypto_metrics.sql
{% macro calculate_rsi(price_column, window=14, partition_by='symbol', order_by='record_date') %}
  -- Relative Strength Index calculation
  with price_changes as (
    select *,
      {{ price_column }} - lag({{ price_column }}) over (
        partition by {{ partition_by }}
        order by {{ order_by }}
      ) as price_change
    from {{ ref('stg__binance_us_24hr_ticker') }}
  ),
  gains_losses as (
    select *,
      case when price_change > 0 then price_change else 0 end as gain,
      case when price_change < 0 then abs(price_change) else 0 end as loss
    from price_changes
  ),
  avg_gains_losses as (
    select *,
      avg(gain) over (
        partition by {{ partition_by }}
        order by {{ order_by }}
        rows between {{ window - 1 }} preceding and current row
      ) as avg_gain,
      avg(loss) over (
        partition by {{ partition_by }}
        order by {{ order_by }}
        rows between {{ window - 1 }} preceding and current row
      ) as avg_loss
    from gains_losses
  )
  select *,
    case 
      when avg_loss = 0 then 100
      else 100 - (100 / (1 + (avg_gain / nullif(avg_loss, 0))))
    end as rsi_{{ window }}
  from avg_gains_losses
{% endmacro %}