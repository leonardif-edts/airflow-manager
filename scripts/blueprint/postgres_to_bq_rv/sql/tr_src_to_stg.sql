{% raw %}
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
  {%- for col in dag.columns.stg %}
  (CASE WHEN {{col.source }} <> "" AND {{col.source }} IS NOT NULL THEN {{ col.transformation }} END) AS `{{ col.name }}`,
  {%- endfor %}
  {% raw -%}
  `job_date`,
  CURRENT_DATETIME("Asia/Jakarta") AS `load_datetime`,
  `job_id`,
  `path_filename`, 
  `row`
FROM `{{ params.project_id }}.{{ params.source_dataset_tablename }}`
WHERE job_id = {{ job_id_bq(data_interval_end) }}
  AND job_date = PARSE_DATE("%Y%m%d", SUBSTR(CAST({{ job_id_bq(data_interval_end) }} AS STRING), 1, 8));
{%- endraw -%}