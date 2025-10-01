import uuid
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)
from airflow.utils.dates import days_ago
from airflow.models import Variable

PROJECT_ID = Variable.get("project_id")                                     # VARIABLE QUE SE OBTIENE DE COMPOSER
REGION = Variable.get("region_name")                                        # VARIABLE QUE SE OBTIENE DE COMPOSER
BUCKET = Variable.get("dataproc_code_bucket")                               # VARIABLE QUE SE OBTIENE DE COMPOSER
PYTHON_FILE_LOCATION = f"gs://{BUCKET}/ruta_job_spark"                      # RUTA AL JOB DE SPARK EN EL BUCKET DE GCS
SQL_SCRIPT_LOCATION = f"gs://{BUCKET}/sparksql/script_sparksql.sql"         # RUTA AL SCRIPT DE SPARK SQL EN EL BUCKET DE GCS (ESTO SE PENSÓ PARA JOBS DE SPARK SQL)
DEPENDENCIES_FILE_LOCATION = f"gs://{BUCKET}/dependencies/utils.zip"        # RUTA AL ZIP CON LAS LIBRERIAS INSTALADAS EN EL BUCKET DE GCS
JDBC_JAR = f"gs://{BUCKET}/jars/ojdbc8.jar"                                 # RUTA AL JAR DE CONEXION JDBC EN EL BUCKET DE GCS
SERVICE_ACCOUNT = Variable.get("service_account")                           # CUENTA DE SERVICIO CON PERMISOS PARA EJECUTAR EL JOB
SUBNET_URI = Variable.get("subnet_uri")                                     # URI DE LA SUBRED DONDE SE
BATCH_PYSPARK = f"pyspark-batch-{uuid.uuid4()}"                             # NOMBRE UNICO PARA EL JOB DE SPARK
BATCH_SPARKSQL = f"sparksql-batch-{uuid.uuid4()}"                           # NOMBRE UNICO PARA EL JOB DE SPARK SQL
labels = {
    "direccion":"xxxxxxx",
    "empresa":"xxxxxxx",
    "gerencia":"xxxxxxx",    
    "proyecto":"xxxxxxx",
    "region":"xxxxxxx"
    }


# Configuración por defecto del DAG
default_args = {
    "owner": "Airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    'email': ['xxxxxxxxxxx'],                                                 # AGREGAR MAIL DE NOTIFICACIÓN
    'email_on_failure': True,
    'email_on_retry': True,
    #"retry_delay": timedelta(minutes=5),
    #"retry_exponential_backoff": True,
    #"max_retry_delay": timedelta(minutes=30),
}

with DAG(
    dag_id="xxxxxxx",                                                         # NOMBRE DEL DAG
    default_args=default_args,
    description="Grupo xx x - Ejecuta un batch de Dataproc Serverless",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["dataproc", "pyspark", "batch"],
) as dag:
    batch_pyspark = DataprocCreateBatchOperator(
        task_id="create_serverless_batch",
        project_id=PROJECT_ID,
        region=REGION,
        batch_id=BATCH_PYSPARK,
        asynchronous=True,
        batch={
            "pyspark_batch": {
                "main_python_file_uri": PYTHON_FILE_LOCATION,
                "python_file_uris": [DEPENDENCIES_FILE_LOCATION],
                "jar_file_uris": [JDBC_JAR],
                "args": [                                                      # ARGUMENTOS QUE SE LE PASAN AL JOB DE SPARK
                    PROJECT_ID,
                    "xxxxxxxxxx",                                              # GRUPO AL QUE PERTENECE EL JOB (VER CONFIG_FILE.JSON)
                    "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",               # BUCKET DONDE SE GUARDAN LOS LOGS DE DATAPROC
                    "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",               # BUCKET DONDE ESTÁ EL CÓDIGO DE LOS JOBS
                    "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"                # RUTA DEL ARCHIVO DE CONFIGURACIÓN DENTRO DEL BUCKET DE CÓDIGO
                ]
            },
            "runtime_config": {
                "version": "2.2",
            },
            "environment_config": {
                "execution_config": {
                    "service_account": SERVICE_ACCOUNT,
                    "subnetwork_uri": SUBNET_URI,
                }
            },
            "labels": labels
        },
    )

    batch_pyspark