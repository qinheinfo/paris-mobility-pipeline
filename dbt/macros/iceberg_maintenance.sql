{% macro iceberg_maintenance() %}

  {% set tables = [
      'velib.station_status_bronze',
      'velib.station_status_silver',
      'velib.dim_station',
      'velib.mart_station_availability'
  ] %}

  {% set cutoff = (modules.datetime.datetime.utcnow() - modules.datetime.timedelta(minutes=30)).strftime('%Y-%m-%d %H:%M:%S') %}

  {% set rewrite_sql %}
    CALL demo.system.rewrite_data_files(table => 'velib.station_status_bronze')
  {% endset %}
  {% set rewrite_result = run_query(rewrite_sql) %}
  {% do log("rewrite_data_files on bronze: " ~ (rewrite_result.rows[0] | list), info=True) %}

  {% for t in tables %}
    {% set expire_sql %}
      CALL demo.system.expire_snapshots(
        table => '{{ t }}',
        older_than => TIMESTAMP '{{ cutoff }}',
        retain_last => 5,
        stream_results => true
      )
    {% endset %}
    {% set expire_result = run_query(expire_sql) %}
    {% do log("expire_snapshots on " ~ t ~ ": " ~ (expire_result.rows[0] | list), info=True) %}
  {% endfor %}

{% endmacro %}