{% macro filter_last_update() %}
    last_update > CURRENT_TIMESTAMP - INTERVAL '10' DAY
{% endmacro %}
