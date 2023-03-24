BLUEPRINT_CODE = {
    "Postgres to BQ RV": "postgres_to_bq_rv"
}

COALESCE_VALUE = {
    "int64": 0,
    "float64": 0.0,
    "string": " ",
    "datetime": "1990-01-01 00:00:00",
    "date": "1990-01-01"
}

INDEX_COLUMNS = (
    "no",
    "bq_tablename",
    "type",
    "method",
    "dag_type",
    "dag_id",
    "dag_schedule",
    "dag_retries_count",
    "dag_retries_delay",
    "dag_concurrency",
    "dag_max_active_runs",
    "source_type",
    "source_database",
    "source_tablename",
    "source_filename",
    "source_delimiter",
    "source_connection",
    "description",
    "ops_email_dev",
    "ops_email_prd"
)

INDEX_COLUMNS_ARRAY = (
    "ops_email_dev",
    "ops_email_prd"
)

EXCLUDE_TABLE_COLUMNS = {
    "job_date",
    "load_datetime",
    "job_id",
    "path_filename",
    "row"
}

PLAN_LOG_COLUMNS = [
    "id",
    "create_ts",
    "source_type",
    "source_id",
    "total_dags"
]