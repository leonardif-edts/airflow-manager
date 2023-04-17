SELECT
  {%- for col in dag.columns.ext %}
  {% if col.datatype == "text" or col.datatype == "varchar" %}REGEXP_REPLACE({{ col.name }}, E'[\\n\\r\\|"~]+', ' ', 'g' ) AS {{ col.name }}{% else %}{{ col.name }}{% endif %}{{ "," if not loop.last else ""}}
  {%- endfor %}
{%- raw %}
FROM {{ params.dataset_table_name }}
{% if (("operator" in dag_run.conf)  and  ("start_date" in dag_run.conf )) or (("start_date" in dag_run.conf ) and ("end_date" in dag_run.conf)) -%}
  {%- if ("operator" in dag_run.conf) -%}
    {%- if   (dag_run.conf["operator"] == "=")  and ("end_date" not in dag_run.conf) -%}
      WHERE {% endraw -%}{{ dag.partition.name }}{%- raw %} BETWEEN (TO_TIMESTAMP('{{ dag_run.conf["start_date"] }}', '%Y-%MM-%DD %HH24')) AND (TO_TIMESTAMP('{{ dag_run.conf["start_date"] }}', '%Y-%MM-%DD %HH24') + INTERVAL '1 DAY')
    {%- elif (dag_run.conf["operator"] == ">=") and ("end_date" not in dag_run.conf) -%}
      WHERE {% endraw -%}{{ dag.partition.name }}{%- raw %} >= TO_TIMESTAMP('{{ dag_run.conf["start_date"] }}', '%Y-%MM-%DD %HH24')
    {%- endif -%}
  {%- elif ("start_date" in dag_run.conf ) and ("end_date" in dag_run.conf ) -%}
    WHERE {% endraw -%}{{ dag.partition.name }}{%- raw %} BETWEEN (TO_TIMESTAMP('{{ dag_run.conf["start_date"] }}', '%Y-%MM-%DD %HH24')) AND (TO_TIMESTAMP('{{ dag_run.conf["end_date"] }}', '%Y-%MM-%DD %HH24') + INTERVAL '1 DAY')
  {%- else -%}
    WHERE {% endraw -%}{{ dag.partition.name }}{%- raw %} BETWEEN (CURRENT_DATE - INTERVAL '1 DAY') and CURRENT_DATE
  {%- endif %}
{%- else -%}
WHERE {% endraw -%}{{ dag.partition.name }}{%- raw %} BETWEEN (CURRENT_DATE - INTERVAL '1 DAY') and CURRENT_DATE
{%- endif %}
{%- if "limit" in dag_run.conf %}
LIMIT {{ dag_run.conf["limit"] }}
{%- endif -%};
{%- endraw -%}