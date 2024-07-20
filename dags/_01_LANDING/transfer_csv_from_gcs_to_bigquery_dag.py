from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# Define constants for BigQuery and GCS
BQ_PROJECT_ID = 'ready-data-engineering-p24'
BQ_LANDING_DATASET_NAME = 'landing_01'
GCS_BUCKET = 'ready-project-dataset'
FILENAME = 'cars-com_dataset/*.csv'
TABLE_NAME = 'gcs_used_cars'

# Schema definition for the destination table
gcs_used_cars_schema_fields = [
    {'name': 'brand', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'model', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'year', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'mileage', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'engine', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'engine_size', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'transmission', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'automatic_transmission', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'fuel_type', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'drivetrain', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'min_mpg', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'max_mpg', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'damaged', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'first_owner', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'personal_using', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'turbo', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'alloy_wheels', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'adaptive_cruise_control', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'navigation_system', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'power_liftgate', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'backup_camera', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'keyless_start', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'remote_start', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'sunroof_or_moonroof', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'automatic_emergency_braking', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'stability_control', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'leather_seats', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'memory_seat', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'third_row_seating', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'apple_car_play_or_android_auto', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'bluetooth', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'usb_port', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'heated_seats', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'interior_color', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'exterior_color', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'price', 'type': 'FLOAT', 'mode': 'NULLABLE'}]


extract_load_csv_dag = DAG(
    dag_id="transfer_csv_from_gcs_to_bigquery",
    schedule="@daily",
    start_date=datetime(2024, 7, 5),
    catchup=False,
)

start_task = EmptyOperator(task_id="empty_start_task", dag = extract_load_csv_dag)


# Define the GCS to BigQuery transfer task
extract_load_csv_op = GCSToBigQueryOperator(
    task_id="gcs_to_bigquery_csv",
    bucket=GCS_BUCKET,
    source_objects=FILENAME,
    destination_project_dataset_table=f"{BQ_PROJECT_ID}.{BQ_LANDING_DATASET_NAME}.{TABLE_NAME}",
    dag=extract_load_csv_dag,
    field_delimiter=',',
    schema_fields = gcs_used_cars_schema_fields,
    ignore_unknown_values=True,
    skip_leading_rows=1,
    source_format='CSV',
    autodetect = False,
    max_bad_records=50,
    write_disposition = "WRITE_TRUNCATE"
)


end_task = EmptyOperator(task_id="empty_end_task", dag=extract_load_csv_dag)

start_task >> extract_load_csv_op >> end_task
