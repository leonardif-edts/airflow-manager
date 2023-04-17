{%- if (dag.method | lower == "delete/insert") -%}
-- Delete/Insert
{%- raw %}
DELETE FROM `{{ params.project_id }}.{{ params.target_dataset_tablename }}`{%- endraw %}
WHERE {{ dag.partition.name }} IN (
  SELECT DISTINCT {{ dag.partition.name }}{%- raw %}
  FROM `{{ params.project_id }}.{{ params.source_dataset_tablename }}`
  WHERE job_id = {{ job_id_bq(next_execution_date) }}
    AND job_date = PARSE_DATE("%Y%m%d", SUBSTR(CAST({{ job_id_bq(next_execution_date) }} AS STRING), 1, 8))
);

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
  {{ col.name }},
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
  FROM `{{ params.project_id }}.{{ params.source_dataset_tablename }}`
  WHERE job_id = {{ job_id_bq(data_interval_end) }}
    AND job_date = PARSE_DATE("%Y%m%d", SUBSTR(CAST({{ job_id_bq(data_interval_end) }} AS STRING), 1, 8))
)
WHERE rownum = 1;
{%- endraw -%}

{%- elif (dag.method | lower == "truncate/insert") -%}
-- Truncate/Insert
{%- raw %}
TRUNCATE TABLE `{{ params.project_id }}.{{ params.target_dataset_tablename }}`;

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
  {{ col.name }},
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
  FROM `{{ params.project_id }}.{{ params.source_dataset_tablename }}`
  WHERE job_id = {{ job_id_bq(data_interval_end) }}
    AND job_date = PARSE_DATE("%Y%m%d", SUBSTR(CAST({{ job_id_bq(data_interval_end) }} AS STRING), 1, 8))
)
WHERE rownum = 1;
{%- endraw -%}

{%- elif (dag.method | lower == "update/insert") -%}
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
  ON COALESCE(T.{{ dag.unique[0].name }}, {% if dag.unique[0].coalesce is integer%}{{ dag.unique[0].coalesce }}{% else %}"{{ dag.unique[0].coalesce }}"{% endif %}) = COALESCE(S.{{ dag.unique[0].name }}, {% if dag.unique[0].coalesce is integer %}{{ dag.unique[0].coalesce }}{% else %}"{{ dag.unique[0].coalesce }}"{% endif %}){%- for col in dag.unique[1:] %}
  AND COALESCE(T.{{ col.name }}, {%- if col.coalesce is integer %}{{ col.coalesce }}{%- else %}"{{ col.coalesce }}"{%- endif %}) = COALESCE(S.{{ col.name }}, {% if col.coalesce is integer %}{{ col.coalesce }}{% else -%}"{{ col.coalesce }}"{% endif %})
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