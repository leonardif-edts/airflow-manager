-- SRC
{% if dag.columns.src -%}
CREATE OR REPLACE TABLE `src.{{ dag.bq_tablename }}` (
  {%- for col in dag.columns.src %}
  {{ col.name }} {{ col.datatype | upper }},
  {%- endfor %}
  job_date DATE,
  load_datetime DATETIME,
  job_id INT64,
  path_filename STRING,
  row STRING
)
PARTITION BY job_date
CLUSTER BY job_id;
{%- endif %}

-- STG
{% if dag.columns.src -%}
CREATE OR REPLACE TABLE `stg.{{ dag.bq_tablename }}` (
  {%- for col in dag.columns.stg %}
  {{ col.name }} {{ col.datatype | upper }},
  {%- endfor %}
  job_date DATE,
  load_datetime DATETIME,
  job_id INT64,
  path_filename STRING,
  row STRING
)
PARTITION BY job_date
CLUSTER BY job_id;
{%- endif %}

-- DW
{% if dag.columns.dw -%}
CREATE OR REPLACE TABLE `dw.{{ dag.bq_tablename }}` (
  {%- for col in dag.columns.dw %}
  {{ col.name }} {{ col.datatype | upper }},
  {%- endfor %}
  job_date DATE,
  load_datetime DATETIME,
  job_id INT64
)
{%- if dag.partition or dag.cluster %}
{%- if dag.partition %}
PARTITION BY {{ col.name }}
{%- endif -%}
{%- if dag.cluster %}
CLUSTER BY {% for col in dag.cluster -%} {{ col.name }}{{ ", " if not loop.last else ""}}{%- endfor -%}
{% endif %}
{%- endif -%}
;
{%- endif %}

-- REJ
CREATE OR REPLACE TABLE `rej.{{ dag.bq_tablename }}_rej` (
  `row` STRING,
  rej_reason STRING,
  job_date DATE,
  load_datetime DATETIME,
  job_id INT64,
  path_filename STRING
)
PARTITION BY  job_date
CLUSTER BY job_id;