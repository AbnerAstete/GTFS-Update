import os
import pandas as pd
import zipfile

BASE_DIR = '/opt/airflow/'
DATA_DIR = os.path.join(BASE_DIR, 'data')

ZIP_DIR = '/opt/airflow/zip_gtfs'


def find_zip_gtfs():
    with open("/opt/airflow/dags/sql/find_zip.sql", "w") as f:
        f.write(
       "SELECT nombre_zip FROM carga_gtfs WHERE id_update = (SELECT MAX(id_update) FROM carga_gtfs);"
    )
    
def find_zip_name_gtfs(task_instance, **kwargs):
    # Obtener el resultado de la ejecución de la consulta
    ti = kwargs['ti']
    result = ti.xcom_pull(task_ids='find_zip')

    if result:
        # Si hay un resultado, guardar el valor de zip_name en una variable
        zip_name = result[0][0]
        print("ZIP Name:", zip_name)
        return zip_name

def extract_zip_gtfs(zip_name, **kwargs):
     with zipfile.ZipFile(ZIP_DIR + zip_name, 'r') as archivo_zip:
        archivo_zip.extractall('txts_gtfs/')