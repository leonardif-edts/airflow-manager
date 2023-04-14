from datetime import datetime, timedelta
import pendulum
import json
import os

from airflow import DAG
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from composer_idm_klik_apollo_prd.dags.shared.custom_postgres_to_gcs import (
    CustomPostgresToGCSOperator,
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
    BigQueryDeleteTableOperator,
    BigQueryInsertJobOperator,
)

from composer_idm_klik_apollo_prd.dags.shared import (
    log_insert, 
    macro, 
    callback{%- if (dag.method | lower == "delete/insert") %},
    custom_delete_insert{%- endif %}
)

# Load default global config
ENV           = Variable.get("ENVIRONMENT")
OPS_TASK_DIR  = Variable.get("IDM_KLIK_APOLLO_SHARED_DIRECTORY")

local_tz      = pendulum.timezone("Asia/Jakarta")
script_dir    = os.path.dirname(__file__)
abs_file_path = os.path.join(script_dir, f"config/config_{ENV}.json")
config_json   = json.load(open(abs_file_path))

# Load environment config
config_env = config_json["env"]
SOURCE     = config_env["source"]
BQ         = config_env["bq"]
GCS        = config_env["gcs"]

# Load job config
JOB        = config_json["job"]
START_DATE = list(map(int, JOB["start_date"].split("-")))


# Custom Python function to query from ssh tunnel
def select_from_tunnel_db(JOB_ID: int, SQL: str, **kwargs):
    # Query from database
    postgres_to_gcs = CustomPostgresToGCSOperator(
        task_id             = 'postgres_to_gcs',
        sql                 = SQL,
        bucket              = GCS["bucket"],
        field_delimiter     = "|",
        filename            = f"new/{GCS['filename']}-{JOB_ID}.csv",
        export_format       = "CSV",
        postgres_conn_id    = SOURCE["postgres_conn_id"],
        dag                 = dag
    )
    postgres_to_gcs.execute(dict())


# Define default arguments
default_args = {
    "owner"                 : JOB["owner"],
    "depends_on_past"       : False,
    "email_on_failure"      : True,
    "email_on_retry"        : False,
    "email"                 : JOB["email"],
    "retries"               : JOB["retries"],
    "retry_delay"           : timedelta(seconds=JOB["retry_delay"]),
    "on_failure_callback"   : callback.on_failure_callback_fact(config_json, ENV)
}

# Define dag
{% raw %}
with DAG(
    JOB["job_name"],
    access_control          = {f"{JOB['tags'][0]}-ops": {"can_read", "can_edit"}},
    start_date              = datetime(*START_DATE, tzinfo=local_tz),
    schedule_interval       = JOB["schedule"],
    concurrency             = JOB["concurrency"],
    max_active_runs         = JOB["max_active_runs"],
    default_args            = default_args,
    tags                    = JOB["tags"],
    template_searchpath     = [OPS_TASK_DIR],
    catchup                 = False,
    is_paused_upon_creation = True,
    user_defined_macros = {
        "job_id_bq": macro.job_id_bq,
        "yesterday": macro.yesterday
    }
) as dag:

    job_start = DummyOperator(
        task_id = "job_start"
    )
    
    with TaskGroup("source_to_ext") as source_to_ext:
        ssh_query_from_db = PythonOperator(
            task_id         = "query_from_db",
            python_callable = select_from_tunnel_db,
            op_kwargs       = {
                "JOB_ID": "{{ yesterday(data_interval_end) }}",
                "SQL": "{% include './sql/query_db.sql' %}"
            },
            params          = {
                "dataset_table_name": SOURCE["dataset_table_name"]
            }
        )

        get_process_file = BashOperator(
            task_id         = "get_process_file",
            bash_command    = "script/get_process_file.sh",
            params          = {
                "project_id"  : JOB["project_id"],
                "gcs_bucket"  : GCS["bucket"],
                "gcs_folder"  : GCS["folder"],
                "gcs_filename": GCS["filename"],
                "type"        : SOURCE["type"],
                "database"    : GCS["database"]
            }
        )

        del_prev_ext = BigQueryDeleteTableOperator(
            task_id                 = "del_prev_ext",
            ignore_if_missing       = True,
            deletion_dataset_table  = f"{JOB['project_id']}.{BQ['tmp_dataset_tablename']}"
        )

        create_ext = BigQueryCreateExternalTableOperator(
            task_id                 = "create_ext",
            bucket                  = GCS["bucket"],
            source_objects          = [f"prc_jobid/{SOURCE['type']}/{GCS['database']}/{GCS['folder']}/{{{{ job_id_bq(data_interval_end) }}}}/*"],
            field_delimiter         = "~",
            allow_quoted_newlines   = True,
            destination_project_dataset_table = f"{JOB['project_id']}.{BQ['tmp_dataset_tablename']}",
            skip_leading_rows       = 0,
            schema_fields           = [
                {"name": "row", "type": "STRING"}
            ]
        )

        # Orchestration
        ssh_query_from_db >> get_process_file >> del_prev_ext >> create_ext


    with TaskGroup("ext_to_src") as ext_to_src:
        tr_ext_to_src = BigQueryInsertJobOperator(
            task_id       = "tr_ext_to_src",
            project_id    = JOB["project_id"],
            configuration = {
                "query": {
                    "query"       : "{% include './sql/tr_ext_to_src.sql' %}",
                    "useLegacySql": False
                }
            },
            params        = {
                "project_id"              : JOB["project_id"],
                "source_dataset_tablename": BQ["tmp_dataset_tablename"],
                "target_dataset_tablename": BQ["src_dataset_tablename"],
                "delimiter_count"         : SOURCE["delimiter_count"],
                "header_key"              : SOURCE["header_key"]
            },
        )

        rej_header = BigQueryInsertJobOperator(
            task_id       = "rej_header",
            project_id    = JOB["project_id"],
            configuration = {
                "query": {
                    "query"       : "{% include './sql/rej_header.sql' %}",
                    "useLegacySql": False
                }
            },
            params={
                "project_id"              : JOB["project_id"],
                "source_dataset_tablename": BQ["tmp_dataset_tablename"],
                "target_dataset_tablename": BQ["rej_dataset_tablename"],
                "header_key"              : SOURCE["header_key"]
            }
        )

        rej_column_mismatch = BigQueryInsertJobOperator(
            task_id         = "rej_column_mismatch",
            project_id      = JOB["project_id"],
            configuration   = {
                "query": {
                    "query"       : "{% include './sql/rej_column_mismatch.sql' %}",
                    "useLegacySql": False
                }
            },
            params          = {
                "project_id"              : JOB["project_id"],
                "source_dataset_tablename": BQ["tmp_dataset_tablename"],
                "target_dataset_tablename": BQ["rej_dataset_tablename"],
                "header_key"              : SOURCE["header_key"],
                "delimiter_count"         : SOURCE["delimiter_count"]
            }
        )

    
    with TaskGroup("src_to_stg") as src_to_stg:
        tr_src_to_stg = BigQueryInsertJobOperator(
            task_id       = "tr_src_to_stg",
            project_id    = JOB["project_id"],
            configuration = {
                "query": {
                    "query"       : "{% include './sql/tr_src_to_stg.sql' %}",
                    "useLegacySql": False
                }
            },
            params        = {
                "project_id"              : JOB["project_id"],
                "source_dataset_tablename": BQ["src_dataset_tablename"],
                "target_dataset_tablename": BQ["stg_dataset_tablename"]
            }
        )
    

    {%- endraw %}
    {% if (dag.method | lower == "delete/insert") %}
    
    delete_exist_dw = custom_delete_insert.KlikIDMApolloDeleteInsertUsingTMPTable(
        task_id                 = "delete_exist_dw",
        project_id              = JOB["project_id"],
        tmp_dataset_tablename   = BQ["tmp_dataset_tablename"],
        stg_dataset_tablename   = BQ["stg_dataset_tablename"],
        dw_dataset_tablename    = BQ["dw_dataset_tablename"],
        partition_column        = BQ["partition_column"]
    )
    {% endif -%}
    {% raw %}

    with TaskGroup("stg_to_dw") as stg_to_dw:
        tr_stg_to_dw = BigQueryInsertJobOperator(
            task_id       = "tr_stg_to_dw",
            project_id    = JOB["project_id"],
            configuration = {
                "query": {
                    "query"       : "{% include './sql/tr_stg_to_dw.sql' %}",
                    "useLegacySql": False
                }
            },
            params        = {
                "project_id"              : JOB["project_id"],
                "target_dataset_tablename": BQ["dw_dataset_tablename"],
                "source_dataset_tablename": BQ["stg_dataset_tablename"],
                {%- endraw %}{%- if (dag.type | lower == "transaction") and (dag.method | lower == "delete/insert") %}
                "tmp_dataset_tablename"   : BQ["tmp_dataset_tablename"]
                {%- endif %}{%- raw %}
            }
        )

        rej_duplicate = BigQueryInsertJobOperator(
                task_id       = "rej_duplicate",
                project_id    = JOB["project_id"],
                configuration = {
                    "query": {
                        "query"       : "{% include './sql/rej_duplicate.sql' %}",
                        "useLegacySql": False
                    }
                },
                params        = {
                    "project_id"              : JOB["project_id"],
                    "source_dataset_tablename": BQ["stg_dataset_tablename"],
                    "target_dataset_tablename": BQ["rej_dataset_tablename"]
                }
            )


    {%- endraw %}
    {% if (dag.method | lower == "delete/insert") %}
    
    delete_tmp = BigQueryDeleteTableOperator(
        task_id                 = "delete_tmp",
        ignore_if_missing       = True,
        deletion_dataset_table  = f"{JOB['project_id']}.{BQ['tmp_dataset_tablename']}_tmp"
    )
    {% endif -%}
    {% raw %}

    seq_log_etl = log_insert.KlikSequenceLogInsert(
        project_id = JOB["project_id"],
        job_name   = JOB["job_name"],
        bq_config  = BQ
    ).init_sequence_gcs()


    rm_process_file = BashOperator(
        task_id = "rm_process_file",
        params  = {
            "gcs_bucket": GCS["bucket"],
            "gcs_folder": GCS["folder"],
            "type"      : SOURCE["type"],
            "database"  : GCS["database"]
        },
        bash_command = "script/rm_process_file.sh"
    )


    job_finish = DummyOperator(
        task_id = "job_finish"
    )


    # Define task dependencies
    (        
        job_start
        >> source_to_ext
        >> ext_to_src
        >> src_to_stg
        {%- endraw %}{% if (dag.method | lower == "delete/insert") %}
        >> delete_exist_dw
        {%- endif %}{%- raw %}
        >> stg_to_dw
        {%- endraw %}{% if (dag.method | lower == "delete/insert") %}
        >> delete_tmp
        {%- endif %}{%- raw %}
        >> seq_log_etl
        >> rm_process_file
        >> job_finish
    )
{% endraw %}