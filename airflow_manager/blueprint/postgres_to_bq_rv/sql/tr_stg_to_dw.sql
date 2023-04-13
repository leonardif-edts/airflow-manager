{%- if (dag.type | lower == "transaction") and (dag.method | lower == "delete/insert") -%}
-- Delete/Insert
{%- raw %}
INSERT INTO `{{ params.project_id }}.{{ params.target_dataset_tablename }}` (
  {%- endraw %}
  {%- for col in dag.columns.dw %}
  {{ col.name }},
  {%- endfor %}
  job_date,
  load_datetime,
  job_id
)
SELECT
  {%- for col in dag.columns.dw %}
  tmp_tbl.{{ col.name }},
  {%- endfor %}
  {% raw -%}
  PARSE_DATE("%Y%m%d", SUBSTR(CAST({{ job_id_bq(next_execution_date) }} AS STRING), 1, 8)) AS job_date,
  CURRENT_DATETIME("Asia/Jakarta") AS load_datetime,
  {{ job_id_bq(next_execution_date) }} AS job_id,
FROM `{{ params.project_id }}.{{ params.tmp_dataset_tablename }}_tmp` tmp_tbl
{% endraw -%}
WHERE NOT EXISTS (
  SELECT 1
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
        ) AS rownum
      {%- raw %}
      FROM `{{ params.project_id }}.{{ params.stg_dataset_tablename}}`
      WHERE job_id = {{ job_id_bq(next_execution_date) }}
        AND job_date = PARSE_DATE("%Y%m%d", SUBSTR(CAST({{ job_id_bq(next_execution_date) }} AS STRING), 1, 8))
    ) r
    WHERE rownum = 1
  ) stg_tbl
  {%- endraw %}
  WHERE COALESCE(tmp_tbl.{{ dag.unique[0].name }}, "{{ dag.unique[0].coalesce }}") = COALESCE(stg_tbl.{{ dag.unique[0].name }}, "{{ dag.unique[0].coalesce }}"){%- for col in dag.unique[1:] %}
    AND COALESCE(tmp_tbl.{{ col.name }}, "{{ col.coalesce }}") = COALESCE(stg_tbl.{{ col.name }}, "{{ col.coalesce }}")
    {%- endfor %}
    AND (
      COALESCE(tmp_tbl.created_date, "2022-01-01") = COALESCE(stg_tbl.created_date, "2022-01-01")
      OR COALESCE(tmp_tbl.rowversion, "2022-01-01") = COALESCE(stg_tbl.rowversion, "2022-01-01")
    )
) 

UNION ALL

SELECT
  {%- for col in dag.columns.dw %}
  tmp_tbl.{{ col.name }},
  {%- endfor %}
  job_date,
  CURRENT_DATETIME("Asia/Jakarta"),
  job_id
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
      ) AS rownum
      {%- raw %}
    FROM `{{ params.project_id }}.{{ params.stg_dataset_tablename}}`
    WHERE job_id = {{ job_id_bq(next_execution_date) }}
      AND job_date = PARSE_DATE("%Y%m%d", SUBSTR(CAST({{ job_id_bq(next_execution_date) }} AS STRING), 1, 8))
  ) r
  WHERE rownum = 1
);
{%- endraw -%}
{%- elif (dag.type | lower == "transaction") and (dag.method | lower == "truncate/insert") -%}
-- Truncate/Insert
{%- raw %}
TRUNCATE TABLE `{{ params.project_id }}.{{ params.target_dataset_tablename }}`;

INSERT INTO `{{ params.project_id }}.{{ params.target_dataset_tablename }}` (
  {%- endraw %}
  {%- for col in dag.columns.dw %}
  tmp_tbl.{{ col.name }},
  {%- endfor %}
  job_date,
  load_datetime,
  job_id
)
SELECT 
  {%- for col in dag.columns.dw %}
  tmp_tbl.{{ col.name }},
  {%- endfor %}
  job_date,
  CURRENT_DATETIME("Asia/Jakarta") AS load_datetime,
  job_id
FROM (
  SELECT *, 
    ROW_NUMBER() OVER (
      PARTITION BY {% if dag.unique | length == 1 -%}{{dag.unique[0].name}}{% else %}
        {%- for col in dag.unique %}
        {{ col.name }}{{ "," if not loop.last else "" }}
        {%- endfor %}
      {%- endif %}
      ORDER BY path_filename DESC
    ) AS rownum
  {%- raw %}
  FROM `{{ params.project_id }}.{{ params.stg_dataset_tablename }}`
  WHERE job_id = {{ job_id_bq(data_interval_end) }}
    AND job_date = PARSE_DATE("%Y%m%d", SUBSTR(CAST({{ job_id_bq(data_interval_end) }} AS STRING), 1, 8))
)
WHERE rownum = 1;
{%- endraw -%}

{%- elif (dag.type | lower == "master") and (dag.method | lower == "update/insert") -%}
-- Update/Insert
{%- raw %}
MERGE `{{ params.project_id }}.{{ params.target_dataset_tablename }}` T
USING (
  SELECT
    {%- endraw %}
    {%- for col in dag.columns.dw %}
    {{ col.name }},
    {%- endfor %}
    job_date,
    CURRENT_DATETIME("Asia/Jakarta") AS load_datetime,
    job_id
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
  )
  WHERE rownum = 1
) S
{%- endraw %}
  ON COALESCE(T.{{ dag.unique[0].name }}, "{{ dag.unique[0].coalesce }}") = COALESCE(S.{{ dag.unique[0].name }}, dag.unique[0].coalesce){%- for col in dag.unique[1:] %}
  AND COALESCE(T.{{ col.name }}, "{{ col.coalesce }}") = COALESCE(S.{{ col.name }}, "{{ col.coalesce }}")
{%- endfor %}
WHEN MATCHED THEN
  UPDATE SET
    {%- for col in dag.columns.dw %}
    T.{{ col.name }} = S.{{ col.name }},
    {%- endfor %}
    T.job_date = S.job_date,
    T.load_datetime = S.load_datetime,
    T.job_id = S.job_id
WHEN NOT MATCHED THEN
  INSERT (
    {%- for col in dag.columns.dw %}
    {{ col.name }},
    {%- endfor %}
    job_date,
    load_datetime,
    job_id
  )
  VALUES (
    {%- for col in dag.columns.dw %}
    {{ col.name }},
    {%- endfor %}
    job_date,
    CURRENT_DATETIME("Asia/Jakarta"),
    job_id
  );
{%- endif -%}