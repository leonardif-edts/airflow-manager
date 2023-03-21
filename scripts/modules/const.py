PARAMS_KEY_MAPPER = {
    "Project ID": "project_id",
    "GCS Bucket": "gcs_bucket"
}

INDEX_COLUMNS = (
    "no",
    "bq_tablename",
    "type",
    "method",
    "source_type",
    "source_database",
    "source_tablename",
    "source_filename",
    "source_connection",
    "dag_type",
    "dag_id"
)

EXCLUDE_COLUMNS = {
    "job_date",
    "load_datetime",
    "job_id",
    "path_filename",
    "row"
}
