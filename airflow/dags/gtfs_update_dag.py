import datetime
import airflow
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.contrib.sensors.file_sensor import FileSensor


from helpers import (
    find_zip_gtfs,find_zip_name_gtfs,extract_zip_gtfs
)


default_args_dict = {
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': None,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=5),
}

dag = airflow.DAG(
    dag_id='gtfs_update_dag',
    default_args=default_args_dict,
    catchup=False,
    template_searchpath=['/opt/airflow/dags/']
)

# Nodes
start = DummyOperator(
    task_id='start', 
    dag=dag,
    trigger_rule='all_success',
)

# Add a FileSensor to wait for new files in /zip_gtfs directory
file_sensor = FileSensor(
    task_id='file_sensor',
    filepath='/zip_gtfs',
    poke_interval=10,  # Check for new files every 10 seconds
    timeout=3600,  # Timeout after 1 hour of waiting
    dag=dag,
)

generate_find_zip = PythonOperator(
    task_id = 'find_zip',
    python_callable = find_zip_gtfs,
    dag=dag,
    depends_on_past=False,
    trigger_rule='all_success',
)

find_zip = PostgresOperator(
        task_id='find_zip',
        dag=dag,
        postgres_conn_id='postgres_default',
        sql='find_zip.sql',
        trigger_rule='none_failed',
        autocommit=True
    )

find_zip_name = PythonOperator(
    task_id='extract_zip_name',
    python_callable=find_zip_name_gtfs,
    provide_context=True,
    dag=dag
)

open_zip = PythonOperator(
    task_id = 'extract_zip',
    python_callable = extract_zip_gtfs,
    provide_context=True,
    dag=dag,
    depends_on_past=False,
    trigger_rule='all_success',
)

end = DummyOperator(
    task_id='end', 
    dag=dag,
    trigger_rule='all_done',
)



start >> file_sensor >> generate_find_zip >> find_zip >> find_zip_name >> end
