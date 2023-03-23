{%- raw -%}
DELETE FROM `{{ params.project_id }}.{{ params.target_dataset_tablename }}`
WHERE job_id = {{ job_id_bq(data_interval_end) }}
  AND job_date = PARSE_DATE("%Y%m%d", SUBSTR(CAST({{ job_id_bq(data_interval_end) }} AS STRING), 1, 8))
  AND rej_reason = "HEADER";

INSERT INTO `{{ params.project_id }}.{{ params.target_dataset_tablename }}` (
  `row`,
  rej_reason,
  job_date,
  load_datetime,
  job_id,
  path_filename
)
SELECT 
  `row`,
  "HEADER" AS `rej_reason`,
  PARSE_DATE('%Y%m%d', SUBSTR(CAST({{ job_id_bq(data_interval_end) }} AS STRING), 1, 8)) AS `job_date`,
  CURRENT_DATETIME("Asia/Jakarta") AS `load_datetime`,
  {{ job_id_bq(data_interval_end) }} AS `job_id`,
  _FILE_NAME AS `path_filename`
FROM `{{ params.project_id }}.{{ params.source_dataset_tablename }}`
WHERE LOWER(`row`) LIKE "{{ params.header_key }}%"
   OR `row` IS NULL;
{%- endraw -%}