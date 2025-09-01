-- macros/audit_run_results.sql
{% macro audit_run_results() %}
  {% if execute %}
    {% set run_results_query %}
      INSERT OR REPLACE INTO meta.dbt_run_audit (
        run_started_at,
        invocation_id, 
        models_run,
        models_passed,
        models_failed,
        tests_run,
        tests_passed,
        tests_failed,
        total_runtime_seconds
      )
      VALUES (
        '{{ run_started_at }}',
        '{{ invocation_id }}',
        {{ results | selectattr("status", "defined") | list | length }},
        {{ results | selectattr("status", "equalto", "success") | list | length }},
        {{ results | selectattr("status", "equalto", "error") | list | length }},
        {{ results | selectattr("resource_type", "equalto", "test") | list | length }},
        {{ results | selectattr("resource_type", "equalto", "test") | selectattr("status", "equalto", "pass") | list | length }},
        {{ results | selectattr("resource_type", "equalto", "test") | selectattr("status", "equalto", "fail") | list | length }},
        {{ (modules.datetime.datetime.now() - run_started_at).total_seconds() }}
      )
    {% endset %}
    
    {% do run_query(run_results_query) %}
    {{ log("Audit record inserted for run: " ~ invocation_id, info=true) }}
  {% endif %}
{% endmacro %}
