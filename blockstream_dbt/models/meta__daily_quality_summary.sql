{{ config(
    materialized='table',
    post_hook=[
      "{{ log_data_quality_alerts() }}",
      "VACUUM {{ this }}"
    ]
) }}

-- meta__data_quality_summary
-- Comprehensive data quality monitoring and alerting model
-- Tracks pipeline health, data completeness, and anomaly detection

with daily_symbol_counts as (
    select
        strftime(open_time_utc, '%Y-%m-%d') as check_date,
        count(distinct symbol) as total_symbols,
        count(*) as total_records
    from {{ ref('stg__binance_us_24hr_ticker') }}
    group by 1
),

quality_metrics as (
    select
        strftime(open_time_utc, '%Y-%m-%d') as check_date,
        
        -- Data quality aggregations
        avg(data_quality_score) as avg_data_quality_score,
        min(data_quality_score) as min_data_quality_score,
        
        -- Anomaly counts
        sum(case when price_anomaly_flag then 1 else 0 end) as price_anomalies,
        sum(case when volume_anomaly_flag then 1 else 0 end) as volume_anomalies,
        sum(case when time_anomaly_flag then 1 else 0 end) as time_anomalies,
        sum(case when trade_id_anomaly_flag then 1 else 0 end) as trade_id_anomalies,
        
        -- Total anomaly records
        sum(case when (price_anomaly_flag or volume_anomaly_flag or 
                      time_anomaly_flag or trade_id_anomaly_flag) 
                 then 1 else 0 end) as records_with_anomalies,
        
        -- Price range validations
        sum(case when last_price <= 0 then 1 else 0 end) as invalid_prices,
        sum(case when high_price < low_price then 1 else 0 end) as ohlc_errors,
        
        -- Volume validations  
        sum(case when volume < 0 then 1 else 0 end) as negative_volumes,
        sum(case when volume = 0 and abs(price_change_percent) > 1 then 1 else 0 end) as zero_volume_price_moves,
        
        -- Missing data
        sum(case when last_price is null then 1 else 0 end) as missing_prices,
        sum(case when volume is null then 1 else 0 end) as missing_volumes
        
    from {{ ref('stg__binance_us_24hr_ticker') }}
    group by 1
),

expected_symbols as (
    -- Define expected major trading pairs that should always be present
    select unnest([
        'BTCUSD', 'ETHUSD', 'ADAUSD', 'SOLUSD', 'DOTUSD',
        'LINKUSD', 'AVAXUSD', 'MATICUSD', 'ATOMUSD', 'ALGOUSD'
    ]) as expected_symbol
),

missing_symbols_check as (
    select 
        stg.check_date,
        count(exp.expected_symbol) as total_expected,
        count(case when actual.symbol is not null then exp.expected_symbol end) as symbols_present,
        count(exp.expected_symbol) - count(case when actual.symbol is not null then exp.expected_symbol end) as missing_major_symbols
    from (select distinct strftime(open_time_utc, '%Y-%m-%d') as check_date from {{ ref('stg__binance_us_24hr_ticker') }}) stg
    cross join expected_symbols exp
    left join (
        select distinct 
            strftime(open_time_utc, '%Y-%m-%d') as check_date,
            symbol 
        from {{ ref('stg__binance_us_24hr_ticker') }}
    ) actual on stg.check_date = actual.check_date 
                and exp.expected_symbol = actual.symbol
    group by 1
),

pipeline_health as (
    select
        sc.check_date,
        sc.total_symbols,
        sc.total_records,
        
        -- Quality scores
        qm.avg_data_quality_score,
        qm.min_data_quality_score,
        
        -- Anomaly metrics
        qm.records_with_anomalies,
        qm.price_anomalies,
        qm.volume_anomalies,
        qm.time_anomalies,
        qm.trade_id_anomalies,
        
        -- Data validation errors
        qm.invalid_prices,
        qm.ohlc_errors,
        qm.negative_volumes,
        qm.zero_volume_price_moves,
        qm.missing_prices,
        qm.missing_volumes,
        
        -- Symbol completeness
        ms.missing_major_symbols,
        ms.symbols_present,
        ms.total_expected,
        
        -- Calculated health scores
        case when sc.total_records > 0 
             then 1.0 - (cast(qm.records_with_anomalies as float) / sc.total_records)
             else 0 
        end as data_integrity_score,
        
        case when ms.total_expected > 0
             then cast(ms.symbols_present as float) / ms.total_expected
             else 1
        end as symbol_completeness_score
        
    from daily_symbol_counts sc
    left join quality_metrics qm on sc.check_date = qm.check_date
    left join missing_symbols_check ms on sc.check_date = ms.check_date
),

final_with_status as (
    select
        *,
        
        -- Overall pipeline health determination
        case 
            when avg_data_quality_score >= 0.95 
                 and data_integrity_score >= 0.98 
                 and symbol_completeness_score >= 0.9
                 and missing_major_symbols = 0
            then 'Healthy'
            
            when avg_data_quality_score >= 0.85 
                 and data_integrity_score >= 0.9 
                 and symbol_completeness_score >= 0.8
            then 'Warning'
            
            else 'Error'
        end as pipeline_status,
        
        -- Alert flags
        case when total_symbols < 50 then true else false end as low_symbol_count_alert,
        case when avg_data_quality_score < 0.8 then true else false end as quality_degradation_alert,
        case when records_with_anomalies > (total_records * 0.1) then true else false end as high_anomaly_alert,
        case when missing_major_symbols > 0 then true else false end as missing_symbols_alert,
        
        -- Metadata
        current_timestamp as calculated_at
        
    from pipeline_health
)

select * from final_with_status