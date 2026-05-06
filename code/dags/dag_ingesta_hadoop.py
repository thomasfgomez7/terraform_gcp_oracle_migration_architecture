from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from datetime import datetime, timedelta
from airflow.models import Variable
#v12
default_args = {
    "owner": "corebi",
    "start_date": datetime(2023, 5, 29),
    "retries": 0,
    "weight_rule": "absolute",
}

PROJECT_ID = Variable.get("project_id")
REGION = Variable.get("region_name")
SERVICE_ACCOUNT = Variable.get("service_account")
SUBNET_URI = Variable.get("subnet_p2_uri")
GCS_CODE_BUCKET = Variable.get("dataproc_code_bucket")
ENV = "prd"

RUNTIME_VERSION = "2.2"

SPARK_PROPERTIES = {
    "spark.executor.memory": "8g",
    "spark.driver.memory": "8g",
    "spark.executor.cores": "4",
    "spark.dataproc.driver.disk.size": "250g",
    "spark.dataproc.executor.disk.size": "250g",
}


JARS_BY_PREFIX = {
    "MYSQL": [
        "gs://bucket-path/jars/mysql-connector-java-8.0.28.jar"
    ],
    "MDB": [
        "gs://bucket-path/jars/mongo-spark-connector_2.13-10.3.0.jar",
        "gs://bucket-path/jars/mongodb-driver-sync-4.11.1.jar",
        "gs://bucket-path/jars/mongodb-driver-core-4.11.1.jar",
        "gs://bucket-path/jars/bson-4.11.1.jar",
    ],
    "ORCL": [
        "gs://bucket-path/jars/ojdbc8.jar"
    ],
}

ARCHIVES_BY_PREFIX = {
    "FTP": [
        "gs://bucket-path/dependencies/site-packages.zip#site-packages"
    ],
}

def get_archive_uris(proceso):
    for prefix, archives in ARCHIVES_BY_PREFIX.items():
        if proceso.startswith(prefix + "_"):
            return archives
    return []

def get_jar_file_uris(proceso):
    for prefix, jars in JARS_BY_PREFIX.items():
        if proceso.startswith(prefix + "_"):
            return jars
    return []


def get_properties(proceso):
    props = SPARK_PROPERTIES.copy()
    if proceso.startswith("PGSQL_"):
        props["spark.jars.packages"] = "org.postgresql:postgresql:42.7.9"
    return props


def clean_batch_id(value):
    return value.lower().replace("_", "-")[:35]


def dataproc_task(
    dag,
    task_group,
    task_id,
    relative_path,
    args=None,
    properties=None,
    jar_file_uris=None,
    archive_uris=None,
    retries=0,
    retry_delay_seconds=20,
):
    return DataprocCreateBatchOperator(
        task_id=task_id,
        task_group=task_group,
        project_id=PROJECT_ID,
        region=REGION,
        batch={
            "pyspark_batch": {
                "main_python_file_uri": f"gs://{GCS_CODE_BUCKET}/{relative_path}",
                "args": args or [],
                "jar_file_uris": jar_file_uris or [],
                "archive_uris": archive_uris or [],
            },
            "runtime_config": {
                "version": RUNTIME_VERSION,
                "properties": properties or SPARK_PROPERTIES,
            },
            "environment_config": {
                "execution_config": {
                    "service_account": SERVICE_ACCOUNT,
                    "subnetwork_uri": SUBNET_URI,
                }
            },
            "labels": {
                "env": ENV,
                "orchestrator": "airflow",
                "engine": "dataproc",
            },
        },
        batch_id=f"{clean_batch_id(task_id)}-{{{{ ts_nodash | lower }}}}-try{{{{ ti.try_number }}}}",
        retries=retries,
        retry_delay=timedelta(seconds=retry_delay_seconds),
        dag=dag,
    )


with DAG(
    dag_id="MAIN_LOAD_HADOOP_GCP",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["nivel_0", "nivel_1", "nivel_2", "DWH", "DATAPROC", "MAIN"],
) as dag:

    start_dag = EmptyOperator(task_id="Start")
    end_dag = EmptyOperator(task_id="End", trigger_rule="none_failed")

    process_mains = TaskGroup(group_id="MAINS")
    P1 = TaskGroup(group_id="P1", parent_group=process_mains)

    PROCESOS_P1 = [
        # Lista de procesos 
        "PROCESO_EJEMPLO_1",
        "PROCESO_EJEMPLO_2",
    ]

    tasks = {}
    for proceso in PROCESOS_P1:
        tasks[proceso] = dataproc_task(
            dag=dag,
            task_group=P1,
            task_id=proceso,
            relative_path=f"pyspark/migracion_hadoop/P1/{proceso}/python/main.py",
            args=["--execution_mode", "daily"],
            properties=get_properties(proceso),
            jar_file_uris=get_jar_file_uris(proceso),
            archive_uris=get_archive_uris(proceso),
        )

    def get(name):
        return tasks.get(name)

    def chain_existing(*names):
        existing = [get(name) for name in names if get(name)]
        for upstream, downstream in zip(existing, existing[1:]):
            upstream >> downstream
        return existing

    video_start = EmptyOperator(task_id="VIDEO_P1_START", task_group=P1)
    video_end = EmptyOperator(task_id="VIDEO_P1_END", task_group=P1)

    internet_start = EmptyOperator(task_id="INTERNET_P1_START", task_group=P1)
    internet_end = EmptyOperator(task_id="INTERNET_P1_END", task_group=P1)

    comercial_start = EmptyOperator(task_id="COMERCIAL_P1_START", task_group=P1)
    comercial_end = EmptyOperator(task_id="COMERCIAL_P1_END", task_group=P1)

    wifi_start = EmptyOperator(task_id="WIFI_P1_START", task_group=P1)
    wifi_end = EmptyOperator(task_id="WIFI_P1_END", task_group=P1)

    omni_start = EmptyOperator(task_id="OMNICANALIDAD_P1_START", task_group=P1)
    api_qm_end = EmptyOperator(task_id="API_QM_END", task_group=P1)
    omni_mysql_end = EmptyOperator(task_id="MYSQL_OMNI_END", task_group=P1)
    omni_end = EmptyOperator(task_id="OMNICANALIDAD_P1_END", task_group=P1)

    hiperactivos_end = EmptyOperator(task_id="HIPERACTIVOS_END", task_group=P1)
    cliente360_end = EmptyOperator(task_id="CLIENTE360_END", task_group=P1)
    model_end = EmptyOperator(task_id="MODEL_P1_END", task_group=P1)
    wifi_backend_end = EmptyOperator(task_id="WIFI_BACKEND_END", task_group=P1)
    conciliaciones_end = EmptyOperator(task_id="CONCILIACIONES_P1_END", task_group=P1)

    chain_existing(
        "PROCESO_EJEMPLO_1",
        "PROCESO_EJEMPLO_2"        
    )

    chain_existing(
        "PROCESO_EJEMPLO_1",
        "PROCESO_EJEMPLO_2"
    )

    video_roots = [
        "VIDEO_TASK_1",
        "VIDEO_TASK_2",
    ]

    video_finals = [        
        "VIDEO_TASK_3",
        "VIDEO_TASK_4",
    ]

    video_start >> [get(t) for t in video_roots if get(t)]
    [get(t) for t in video_finals if get(t)] >> video_end

    internet_tasks = [
        "INTERNET_TASK_1",
        "INTERNET_TASK_2",
    ]

    internet_existing = [get(t) for t in internet_tasks if get(t)]
    internet_start >> internet_existing >> internet_end

    chain_existing(
        "PROCESO_EJEMPLO_1",
        "PROCESO_EJEMPLO_2"
    )

    chain_existing(
        "PROCESO_EJEMPLO_1",
        "PROCESO_EJEMPLO_2"
    )

    chain_existing(
        "PROCESO_EJEMPLO_1",
        "PROCESO_EJEMPLO_2"
    )

    comercial_roots = [
        "COMERCIAL_TASK_1",
        "COMERCIAL_TASK_2"
    ]

    comercial_finals = [
        "COMERCIAL_TASK_3",
        "COMERCIAL_TASK_4"
    ]

    comercial_start >> [get(t) for t in comercial_roots if get(t)]
    [get(t) for t in comercial_finals if get(t)] >> comercial_end

    wifi_tasks = [
        "WIFI_TASK_1",
        "WIFI_TASK_2"
    ]

    wifi_existing = [get(t) for t in wifi_tasks if get(t)]
    wifi_start >> wifi_existing >> wifi_end

    api_qm_tasks = [
        "API_QM_TASK_1",
        "API_QM_TASK_2"
    ]

    omni_start >> [get(t) for t in api_qm_tasks if get(t)] >> api_qm_end

    chain_existing(
        "PROCESO_EJEMPLO_1",
        "PROCESO_EJEMPLO_2"
    )

    chain_existing(
        "PROCESO_EJEMPLO_1",
        "PROCESO_EJEMPLO_2"
    )

    chain_existing(
        "PROCESO_EJEMPLO_1",
        "PROCESO_EJEMPLO_2"
    )

    mysql_omni_roots = [
        "MYSQL_OMNI_TASK_1",
        "MYSQL_OMNI_TASK_2"
    ]

    mysql_omni_finals = [
        "MYSQL_OMNI_TASK_3",
        "MYSQL_OMNI_TASK_4"
    ]

    api_qm_end >> [get(t) for t in mysql_omni_roots if get(t)]
    [get(t) for t in mysql_omni_finals if get(t)] >> omni_mysql_end

    yoizen_tasks = [
        "YOIZEN_TASK_1",
        "YOIZEN_TASK_2"
    ]

    omni_mysql_end >> [get(t) for t in yoizen_tasks if get(t)] >> omni_end

    if get("HDFS_TASK_1"):
        omni_end >> get("HDFS_TASK_1") >> hiperactivos_end
    else:
        omni_end >> hiperactivos_end

    if get("HDFS_TASK_2"):
        hiperactivos_end >> get("HDFS_TASK_2") >> cliente360_end
    else:
        hiperactivos_end >> cliente360_end

    if get("HDFS_TASK_3"):
        cliente360_end >> get("HDFS_TASK_3")
        cliente360_bajas_final = get("HDFS_TASK_4")
    else:
        cliente360_bajas_final = cliente360_end

    model_chain = chain_existing(
        "MODEL_TASK_1",
        "MODEL_TASK_2"
    )

    if model_chain:
        cliente360_end >> model_chain[0]
        model_chain[-1] >> model_end
    else:
        cliente360_end >> model_end

    wifi_backend_chain = chain_existing(
        "WIFI_BACKEND_TASK_1",
        "WIFI_BACKEND_TASK_2"
    )

    if wifi_backend_chain:
        model_end >> wifi_backend_chain[0]
        wifi_backend_chain[-1] >> wifi_backend_end
    else:
        model_end >> wifi_backend_end

    if get("HDFS_TASK_5"):
        [cliente360_bajas_final, wifi_backend_end] >> get("HDFS_TASK_5") >> conciliaciones_end
    else:
        [cliente360_bajas_final, wifi_backend_end] >> conciliaciones_end

    start_dag >> video_start
    video_end >> internet_start
    internet_end >> comercial_start
    comercial_end >> wifi_start
    wifi_end >> omni_start
    conciliaciones_end >> end_dag