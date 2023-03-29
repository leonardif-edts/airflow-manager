{%- raw -%}
DELETE FROM `{{ params.project_id }}.{{ params.target_dataset_tablename }}`
WHERE job_id = {{ job_id_bq(data_interval_end) }}
  AND job_date = PARSE_DATE("%Y%m%d", SUBSTR(CAST({{ job_id_bq(data_interval_end) }} AS STRING), 1, 8))
  AND rej_reason = "DUPLICATE RECORD";

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
  "DUPLICATE RECORD" AS `rej_reason`,
  PARSE_DATE("%Y%m%d", SUBSTR(CAST({{ job_id_bq(data_interval_end) }} AS STRING), 1, 8)) AS `job_date`,
  CURRENT_DATETIME("Asia/Jakarta") AS `load_datetime`,
  {{ job_id_bq(data_interval_end) }} AS `job_id`,
  `path_filename`
{%- endraw %}
FROM (
  SELECT r.*
  FROM (
    SELECT *, 
      ROW_NUMBER() OVER (
        PARTITION BY {% if dag.unique | length == 1 -%}{{dag.unique[0].name}}{% else %}
          {%- for col in dag.unique %}
          {{ col.name }}{{ "," if not loop.last else "" }}
          {%- endfor %}
        {%- endif %}
      ORDER BY path_filename DESC
    ) AS `rownum`
    {%- raw %}
    FROM `{{ params.project_id }}.{{ params.source_dataset_tablename }}`
    WHERE job_id = {{ job_id_bq(data_interval_end) }}
      AND job_date = PARSE_DATE("%Y%m%d", SUBSTR(CAST({{ job_id_bq(data_interval_end) }} AS STRING), 1, 8))
  ) r
  WHERE rownum != 1
);
{%- endraw -%}