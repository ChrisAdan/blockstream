-- macros/log_data_quality_alerts.sql  
{% macro log_data_quality_alerts() %}
  {% if execute %}
    {% set alert_query %}
      SELECT 
        check_date,
        pipeline_status,
        avg_data_quality_score,
        records_with_anomalies,
        total_records,
        missing_major_symbols,
        low_symbol_count_alert,
        quality_degradation_alert,
        high_anomaly_alert,
        missing_symbols_alert
      FROM {{ this }}
      WHERE pipeline_status != 'Healthy'
         OR low_symbol_count_alert = true
         OR quality_degradation_alert = true  
         OR high_anomaly_alert = true
         OR missing_symbols_alert = true
      ORDER BY check_date DESC
      LIMIT 1
    {% endset %}
    
    {% set results = run_query(alert_query) %}
    
    {% if results %}
      {% for row in results %}
        {% set alert_msg = "🚨 DATA QUALITY ALERT: " ~ row[1] ~ " status on " ~ row[0] %}
        {% set details = "Quality Score: " ~ row[2] ~ ", Anomalies: " ~ row[3] ~ "/" ~ row[4] %}
        {{ log(alert_msg ~ " - " ~ details, info=true) }}
      {% endfor %}
    {% endif %}
  {% endif %}
{% endmacro %}
