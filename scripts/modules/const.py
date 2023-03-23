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

INDEX_ARRAY_COLUMNS = (
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

