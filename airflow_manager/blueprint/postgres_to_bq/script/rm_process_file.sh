{%- raw -%}
gsutil -m rm -r gs://{{ params.gcs_bucket }}/prc_jobid/{{ params.type }}/{{ params.database }}/{{ params.gcs_folder }}/{{ job_id_bq(data_interval_end) }}/*
{%- endraw -%}