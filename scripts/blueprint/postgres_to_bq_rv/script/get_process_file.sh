{%- raw -%}
FILENAME="{{ dag_run.conf.get('filename', params.gcs_filename) }}"
DATE_DIGIT_SEQ=($(seq 1 1 {{ dag_run.conf.get('date_digit', 8) }}))
GCS_FILENAME="$FILENAME-$(printf "?%0.s" ${DATE_DIGIT_SEQ[@]}).csv"

echo "Project ID: {{ params.project_id }}"
echo "GCS Bucket: {{ params.gcs_bucket }}"
echo "GCS Folder: {{ params.gcs_folder }}"
echo "GCS Filename: $GCS_FILENAME"
echo "Job ID: {{ job_id_bq(data_interval_end) }}"

gsutil -m mv    gs://{{ params.gcs_bucket }}/new/$GCS_FILENAME gs://{{ params.gcs_bucket }}/prc_jobid/{{ params.type }}/{{ params.database }}/{{ params.gcs_folder }}/{{ job_id_bq(data_interval_end) }}/
gsutil -m cp -r gs://{{ params.gcs_bucket }}/prc_jobid/{{ params.type }}/{{ params.database }}/{{ params.gcs_folder }}/{{ job_id_bq(data_interval_end) }}/* gs://{{ params.gcs_bucket }}/bkp/{{ params.type }}/{{ params.database }}/{{ params.gcs_folder }}/{{ job_id_bq(data_interval_end)[0:4] }}/
gsutil -m cp -r gs://{{ params.gcs_bucket }}/prc_jobid/{{ params.type }}/{{ params.database }}/{{ params.gcs_folder }}/{{ job_id_bq(data_interval_end) }}/* gs://{{ params.gcs_bucket }}/bkp_jobid/{{ params.type }}/{{ params.database }}/{{ params.gcs_folder }}/{{ job_id_bq(data_interval_end) }}/
{%- endraw -%}