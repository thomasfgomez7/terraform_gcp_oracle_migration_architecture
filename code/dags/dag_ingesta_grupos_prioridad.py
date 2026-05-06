import uuid
import json
import pendulum
from datetime import timedelta

from airflow.utils.trigger_rule import TriggerRule
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.python import PythonSensor
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.models import TaskInstance
from airflow.utils.state import State
from airflow.utils.session import provide_session
from google.cloud import storage


PROJECT_ID = Variable.get("project_id")
REGION = Variable.get("region_name")
BUCKET = Variable.get("dataproc_code_bucket")

PYTHON_FILE_LIGHT = f"gs://{BUCKET}/pyspark_daily_job_light.py"
PYTHON_FILE_HEAVY = f"gs://{BUCKET}/pyspark_daily_job_heavy.py"

JDBC_JAR = f"gs://{BUCKET}/jars/ojdbc8.jar"
SERVICE_ACCOUNT = Variable.get("service_account")
SUBNET_URI = Variable.get("SUBNET_URI")
CONFIG_URI = "PATH_TO_CONFIG_JSON"

TIMEZONE = "America/Argentina/Buenos_Aires"

labels = {
    "env": "dev",
}

default_args = {"owner": "developer", "retries": 0}


with DAG(
    dag_id="dag_ingesta_prioridad",
    default_args=default_args,
    description="Dataproc Serverless por grupo",
    start_date=pendulum.datetime(2025, 10, 1, tz=TIMEZONE),
    schedule="0 0 * * *",
    catchup=False,
    tags=["dataproc", "pyspark", "batch"],
) as dag:

    @task
    def load_config():
        client = storage.Client()
        bucket_name, blob_path = CONFIG_URI.replace("gs://", "").split("/", 1)
        blob = client.bucket(bucket_name).blob(blob_path)
        return json.loads(blob.download_as_text())

    load_config_task = load_config()

    DEFAULT_SPARK_PROPERTIES = {
        "spark.dynamicAllocation.enabled": "true",
        "spark.network.timeout": "800s",
        "spark.executor.heartbeatInterval": "60s",
        "spark.speculation": "false",
        "spark.sql.adaptive.enabled": "false",
    }

    def mk_batch(
        task_id: str,
        batch_prefix: str,
        config_key: str,
        ttl: str = "36000s",
        flavor: str = "light",
    ):
        main_py = PYTHON_FILE_HEAVY if flavor == "heavy" else PYTHON_FILE_LIGHT

        runtime_cfg = {"version": "2.2"}
        if DEFAULT_SPARK_PROPERTIES:
            runtime_cfg["properties"] = DEFAULT_SPARK_PROPERTIES

        return DataprocCreateBatchOperator(
            task_id=task_id,
            project_id=PROJECT_ID,
            region=REGION,
            batch_id=f"{batch_prefix}-{str(uuid.uuid4())[:8]}",
            asynchronous=False,
            trigger_rule=TriggerRule.ALL_DONE,
            pool="dataproc_light",
            batch={
                "pyspark_batch": {
                    "main_python_file_uri": main_py,
                    "jar_file_uris": [JDBC_JAR],
                    "args": [
                        PROJECT_ID,
                        f'{{{{ ti.xcom_pull(task_ids="load_config")["{config_key}"]["tables"] }}}}',
                        "bucket-logs",
                        "bucket-code",
                    ],
                },
                "runtime_config": runtime_cfg,
                "environment_config": {
                    "execution_config": {
                        "service_account": SERVICE_ACCOUNT,
                        "subnetwork_uri": SUBNET_URI,
                        "ttl": ttl,
                    }
                },
                "labels": labels,
            },
        )

    p1_g1 = mk_batch("grupo_1_oracle_crm_prioridad_1", "daily-grupo-1-oracle-crm-prioridad-1", "grupo-1-oracle-crm-prioridad-1", flavor="light")
    p1_g2 = mk_batch("grupo_2_oracle_crm_prioridad_1", "daily-grupo-2-oracle-crm-prioridad-1", "grupo-2-oracle-crm-prioridad-1", flavor="heavy")
    p1_g3 = mk_batch("grupo_3_oracle_crm_prioridad_1", "daily-grupo-3-oracle-crm-prioridad-1", "grupo-3-oracle-crm-prioridad-1", flavor="light")
    p1_g4 = mk_batch("grupo_4_oracle_crm_prioridad_1", "daily-grupo-4-oracle-crm-prioridad-1", "grupo-4-oracle-crm-prioridad-1", flavor="light")
    p1_g5 = mk_batch("grupo_5_oracle_crm_prioridad_1", "daily-grupo-5-oracle-crm-prioridad-1", "grupo-5-oracle-crm-prioridad-1", flavor="heavy")
    p1_g6 = mk_batch("grupo_6_oracle_crm_prioridad_1", "daily-grupo-6-oracle-crm-prioridad-1", "grupo-6-oracle-crm-prioridad-1", flavor="light")

    p2_g1 = mk_batch("grupo_1_oracle_crm_prioridad_2", "daily-grupo-1-oracle-crm-prioridad-2", "grupo-1-oracle-crm-prioridad-2", ttl="7200s", flavor="heavy")
    p2_g2 = mk_batch("grupo_2_oracle_crm_prioridad_2", "daily-grupo-2-oracle-crm-prioridad-2", "grupo-2-oracle-crm-prioridad-2", flavor="light")
    p2_g3 = mk_batch("grupo_3_oracle_crm_prioridad_2", "daily-grupo-3-oracle-crm-prioridad-2", "grupo-3-oracle-crm-prioridad-2", flavor="light")
    p2_g4 = mk_batch("grupo_4_oracle_crm_prioridad_2", "daily-grupo-4-oracle-crm-prioridad-2", "grupo-4-oracle-crm-prioridad-2", flavor="light")
    p2_g5 = mk_batch("grupo_5_oracle_crm_prioridad_2", "daily-grupo-5-oracle-crm-prioridad-2", "grupo-5-oracle-crm-prioridad-2", flavor="light")
    p2_g6 = mk_batch("grupo_6_oracle_crm_prioridad_2", "daily-grupo-6-oracle-crm-prioridad-2", "grupo-6-oracle-crm-prioridad-2", flavor="heavy")

    p2_g7 = mk_batch("grupo_7_oracle_crm_prioridad_2", "daily-grupo-7-oracle-crm-prioridad-2", "grupo-7-oracle-crm-prioridad-2", flavor="light")
    p2_g8 = mk_batch("grupo_8_oracle_crm_prioridad_2", "daily-grupo-8-oracle-crm-prioridad-2", "grupo-8-oracle-crm-prioridad-2", flavor="light")
    p2_g9 = mk_batch("grupo_9_oracle_crm_prioridad_2", "daily-grupo-9-oracle-crm-prioridad-2", "grupo-9-oracle-crm-prioridad-2", flavor="light")
    p2_g10 = mk_batch("grupo_10_oracle_crm_prioridad_2", "daily-grupo-10-oracle-crm-prioridad-2", "grupo-10-oracle-crm-prioridad-2", flavor="light")
    p2_g11 = mk_batch("grupo_11_oracle_crm_prioridad_2", "daily-grupo-11-oracle-crm-prioridad-2", "grupo-11-oracle-crm-prioridad-2", flavor="light")
    p2_g12 = mk_batch("grupo_12_oracle_crm_prioridad_2", "daily-grupo-12-oracle-crm-prioridad-2", "grupo-12-oracle-crm-prioridad-2", flavor="light")
    p2_g13 = mk_batch("grupo_13_oracle_crm_prioridad_2", "daily-grupo-13-oracle-crm-prioridad-2", "grupo-13-oracle-crm-prioridad-2", flavor="light")
    p2_g14 = mk_batch("grupo_14_oracle_crm_prioridad_2", "daily-grupo-14-oracle-crm-prioridad-2", "grupo-14-oracle-crm-prioridad-2", flavor="light")
    p2_g15 = mk_batch("grupo_15_oracle_crm_prioridad_2", "daily-grupo-15-oracle-crm-prioridad-2", "grupo-15-oracle-crm-prioridad-2", flavor="light")
    p2_g16 = mk_batch("grupo_16_oracle_crm_prioridad_2", "daily-grupo-16-oracle-crm-prioridad-2", "grupo-16-oracle-crm-prioridad-2", flavor="heavy")
    p2_g17 = mk_batch("grupo_17_oracle_crm_prioridad_2", "daily-grupo-17-oracle-crm-prioridad-2", "grupo-17-oracle-crm-prioridad-2", flavor="light")
    p2_g18 = mk_batch("grupo_18_oracle_crm_prioridad_2", "daily-grupo-18-oracle-crm-prioridad-2", "grupo-18-oracle-crm-prioridad-2", flavor="light")
    p2_g19 = mk_batch("grupo_19_oracle_crm_prioridad_2", "daily-grupo-19-oracle-crm-prioridad-2", "grupo-19-oracle-crm-prioridad-2", flavor="light")
    p2_g20 = mk_batch("grupo_20_oracle_crm_prioridad_2", "daily-grupo-20-oracle-crm-prioridad-2", "grupo-20-oracle-crm-prioridad-2", flavor="light")

    def chunk_list(lst, size):
        return [lst[i:i + size] for i in range(0, len(lst), size)]

    p1_wave1_done = EmptyOperator(task_id="p1_wave1_done", trigger_rule=TriggerRule.ALL_DONE)
    p1_wave2_done = EmptyOperator(task_id="p1_wave2_done", trigger_rule=TriggerRule.ALL_DONE)
    p1_wave3_done = EmptyOperator(task_id="p1_wave3_done", trigger_rule=TriggerRule.ALL_DONE)

    load_config_task >> p1_g2
    p1_g2 >> p1_wave1_done

    p1_wave1_done >> p1_g5
    p1_g5 >> p1_wave2_done

    p1_wave2_done >> [p1_g1, p1_g3, p1_g4, p1_g6]
    [p1_g1, p1_g3, p1_g4, p1_g6] >> p1_wave3_done

    trigger_call_bq_routine_dag = TriggerDagRunOperator(
        task_id="trigger_call_bq_routine_dag",
        trigger_dag_id="call_bq_routines_mixed",
        trigger_rule=TriggerRule.ALL_SUCCESS,
        wait_for_completion=False,
        reset_dag_run=False,
        conf={"source_dag_id": "dag_ingesta_prioridad_test", "source_run_id": "{{ run_id }}"},
    )

    [p1_g1, p1_g2, p1_g3, p1_g4, p1_g5, p1_g6] >> trigger_call_bq_routine_dag

    continuar_prioridad_2 = EmptyOperator(task_id="continuar_prioridad_2", trigger_rule=TriggerRule.ALL_DONE)
    trigger_call_bq_routine_dag >> continuar_prioridad_2

    p2_wave1_done = EmptyOperator(task_id="p2_wave1_done", trigger_rule=TriggerRule.ALL_DONE)
    p2_wave2_done = EmptyOperator(task_id="p2_wave2_done", trigger_rule=TriggerRule.ALL_DONE)
    
    continuar_prioridad_2 >> p2_g1
    p2_g1 >> p2_g16
    p2_g16 >> p2_wave1_done
    
    p2_wave1_done >> p2_g6
    p2_g6 >> p2_wave2_done


    p2_rest = [p2_g2, p2_g3, p2_g4, p2_g5, p2_g7, p2_g8, p2_g9, p2_g10, p2_g11, p2_g12, p2_g13, p2_g14, p2_g15, p2_g17, p2_g18, p2_g19, p2_g20]
    p2_waves = chunk_list(p2_rest, 5)

    @provide_session
    def _wave_done(task_ids, session=None, **_):
        from airflow.operators.python import get_current_context
        ctx = get_current_context()
        dag_id = ctx["dag"].dag_id
        run_id = ctx["run_id"]

        tis = (
            session.query(TaskInstance.task_id, TaskInstance.state)
            .filter(TaskInstance.dag_id == dag_id)
            .filter(TaskInstance.run_id == run_id)
            .filter(TaskInstance.task_id.in_(task_ids))
            .all()
        )
        states = {tid: st for tid, st in tis}
        done_states = {State.SUCCESS, State.FAILED, State.SKIPPED, State.UPSTREAM_FAILED}
        return all(states.get(tid) in done_states for tid in task_ids)

    if p2_waves:
        for t in p2_waves[0]:
            p2_wave2_done >> t
            t.trigger_rule = TriggerRule.ALL_DONE

        prev_gate_or = p2_wave2_done

        for i in range(1, len(p2_waves)):
            prev_tasks = p2_waves[i - 1]
            prev_ids = [t.task_id for t in prev_tasks]

            wave_start = EmptyOperator(
                task_id=f"p2_wave_{i}_start",
                trigger_rule=TriggerRule.ALL_DONE,
            )
            prev_gate_or >> wave_start

            wave_timer = TimeDeltaSensor(
                task_id=f"timer_p2_wave_{i}",
                delta=timedelta(hours=1),
                mode="reschedule",
                poke_interval=60,
            )
            wave_start >> wave_timer

            wave_done = PythonSensor(
                task_id=f"done_p2_wave_{i}",
                python_callable=_wave_done,
                op_kwargs={"task_ids": prev_ids},
                poke_interval=60,
                timeout=60 * 60 * 24,
                mode="reschedule",
                soft_fail=False,
            )

            for tprev in prev_tasks:
                tprev >> wave_done

            gate_or = EmptyOperator(
                task_id=f"gate_or_p2_wave_{i+1}",
                trigger_rule=TriggerRule.ONE_SUCCESS,
            )
            wave_done >> gate_or
            wave_timer >> gate_or

            for tnext in p2_waves[i]:
                gate_or >> tnext
                tnext.trigger_rule = TriggerRule.ALL_DONE

            prev_gate_or = gate_or

    @task(trigger_rule=TriggerRule.ALL_DONE, retries=0)
    def resumen_ejecucion(batch_task_ids: list):
        import json as _json
        from datetime import datetime as _dt
        from airflow.operators.python import get_current_context
        from google.cloud import storage as _storage

        ctx = get_current_context()
        dag_id = ctx["dag"].dag_id
        run_id = ctx["run_id"]
        execution_date = ctx["execution_date"].isoformat()

        @provide_session
        def _get_states(session=None):
            states = {}
            for task_id in batch_task_ids:
                ti = (
                    session.query(TaskInstance)
                    .filter(TaskInstance.dag_id == dag_id, TaskInstance.run_id == run_id, TaskInstance.task_id == task_id)
                    .order_by(TaskInstance.try_number.desc())
                    .first()
                )
                states[task_id] = ti.state if ti else "no_ti"
            return states

        states = _get_states()

        ok = [k for k, v in states.items() if v == State.SUCCESS]
        failed = [k for k, v in states.items() if v == State.FAILED]
        skipped = [k for k, v in states.items() if v == State.SKIPPED]

        resumen = {
            "dag_id": dag_id,
            "run_id": run_id,
            "execution_date": execution_date,
            "states": states,
            "ok": ok,
            "failed": failed,
            "skipped": skipped,
            "generated_at": _dt.utcnow().isoformat(),
        }

        client = _storage.Client()
        bucket = client.bucket("bucket-logs")
        blob_path = f"resumen_ejecucion/{dag_id}/{run_id}.json"
        bucket.blob(blob_path).upload_from_string(_json.dumps(resumen, indent=2), content_type="application/json")
        return resumen

    all_batches = [
        p1_g1, p1_g2, p1_g3, p1_g4, p1_g5, p1_g6,
        p2_g1, p2_g2, p2_g3, p2_g4, p2_g5, p2_g6, p2_g7, p2_g8, p2_g9, p2_g10,
        p2_g11, p2_g12, p2_g13, p2_g14, p2_g15, p2_g16, p2_g17, p2_g18, p2_g19, p2_g20
    ]

    resumen_task = resumen_ejecucion([t.task_id for t in all_batches])
    for t in all_batches:
        t >> resumen_task
