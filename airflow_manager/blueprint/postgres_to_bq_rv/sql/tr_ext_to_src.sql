{%- raw -%}
DELETE FROM `{{ params.project_id }}.{{ params.target_dataset_tablename }}`
WHERE job_id = {{ job_id_bq(data_interval_end) }}
  AND job_date = PARSE_DATE("%Y%m%d", SUBSTR(CAST({{ job_id_bq(data_interval_end) }} AS STRING), 1, 8));

INSERT INTO `{{ params.project_id }}.{{ params.target_dataset_tablename }}` (
  {%- endraw %}
  {%- for col in dag.columns.src %}
  {{ col.name }},
  {%- endfor %}
  job_date,
  load_datetime,
  job_id,
  path_filename, 
  row
)
SELECT
  {%- for col in dag.columns.src %}
  TRIM({{ col.transformation }}, '" ') AS `{{col.name}}`,
  {%- endfor %}
  {% raw -%}
  PARSE_DATE("%Y%m%d", SUBSTR(CAST({{ job_id_bq(data_interval_end) }} AS STRING), 1, 8)) AS `job_date`,
  CURRENT_DATETIME("Asia/Jakarta") AS `load_datetime`,
  {{ job_id_bq(data_interval_end) }} AS `job_id`,
  _FILE_NAME  AS `path_filename`,
  `row`
FROM`{{ params.project_id }}.{{ params.source_dataset_tablename }}`
WHERE LOWER(`row`) NOT LIKE "{{ params.header_key }}%"
  AND LENGTH(`row`) - LENGTH(REGEXP_REPLACE(`row`, r"\|", "")) = {{ params.delimiter_count }};
{%- endraw -%}