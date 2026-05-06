import sys
import time
from datetime import datetime, timedelta
import ast
import json

from google.cloud import storage
from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType
from google.cloud import secretmanager
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from pyspark import StorageLevel


def access_secret(project_id, secret_id, version_id="latest"):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

def get_oracle_connection_string(ip, port, service):
    return f"jdbc:oracle:thin:@{ip}:{port}/{service}"

def setPrecisionScale(df):
    for field in df.schema.fields:
        if isinstance(field.dataType, DecimalType):
            p = min(field.dataType.precision, 28)
            s = min(field.dataType.scale, 9)
            df = df.withColumn(field.name, df[field.name].cast(DecimalType(p, s)))
    return df

def build_oracle_query_incremental(query_string, date_column, start_date, yesterday, bq_client, bq_table, reload=False):
    if reload:
        return query_string, False
    if not date_column:
        return query_string, False
    if start_date is not None and str(start_date).strip() != "":
        final_query = (
            query_string
            + f" WHERE {date_column} > TO_DATE('{start_date}', 'YYYY-MM-DD') "
            + f"AND {date_column} < TO_DATE('{yesterday}', 'YYYY-MM-DD')"
        )
        return final_query, True
    last = None
    try:
        last = next(
            bq_client.query(
                f"SELECT MAX(DATE({date_column})) d FROM `{bq_table}`"
            ).result()
        ).d
    except NotFound:
        last = None
    except Exception:
        last = None
    if last:
        final_query = (
            query_string
            + f" WHERE {date_column} > TO_DATE('{last}', 'YYYY-MM-DD') "
            + f"AND {date_column} < TO_DATE('{yesterday}', 'YYYY-MM-DD') + 1"
        )
        return final_query, True
    print(
        f"[WARN] start_date vacio y no pude obtener last_date desde BigQuery "
        f"(tabla inexistente o sin datos). Se ejecuta FULL para {bq_table}."
    )
    return query_string, False

def get_dynamic_bounds(spark, base_query, oracle_conn, oracle_user, oracle_password, partition_col, table=None, retries=2, sleep_secs=5,):
    if table == "TABLE_EXAMPLE":
        bounds_sql = (
            "SELECT "
            "MAX(CASE WHEN tipo = 'LO' THEN val END) AS LO, "
            "MAX(CASE WHEN tipo = 'HI' THEN val END) AS HI "
            "FROM ("
            "  SELECT /*+ INDEX_ASC(t TABLE_EXAMPLE_PK1) */ "
            f"         'LO' AS tipo, t.PQ_DETALLEID AS val "
            "  FROM SCHEMA.TABLE_EXAMPLE t "
            "  WHERE ROWNUM = 1 "
            "  UNION ALL "
            "  SELECT /*+ INDEX_DESC(t TABLE_EXAMPLE_PK1) */ "
            f"         'HI' AS tipo, t.PQ_DETALLEID AS val "
            "  FROM SCHEMA.TABLE_EXAMPLE t "
            "  WHERE ROWNUM = 1 "
            ")"
        )
    else:
        bounds_sql = (
            "SELECT "
            f"MIN({partition_col}) AS LO, "
            f"MAX({partition_col}) AS HI "
            f"FROM ({base_query}) q"
        )

    last_err = None
    for attempt in range(1, retries + 2):
        try:
            print(f"[INFO] Bounds Oracle MIN/MAX (attempt {attempt}/{retries+1})")
            df_bounds = (
                spark.read.format("jdbc")
                .option("url", oracle_conn)
                .option("driver", "oracle.jdbc.OracleDriver")
                .option("user", oracle_user)
                .option("password", oracle_password)
                .option("query", bounds_sql)
                .option("fetchsize", "1")
                .option("oracle.net.CONNECT_TIMEOUT", "300000")
                .option("oracle.jdbc.ReadTimeout", "7200000")
                .option("oracle.jdbc.QueryTimeout", "7200")
                .load()
            )
            row = df_bounds.collect()[0]
            lo = row["LO"]
            hi = row["HI"]
            if lo is None or hi is None:
                return None, None
            try:
                lo = int(lo)
            except Exception:
                lo = int(float(lo))
            try:
                hi = int(hi)
            except Exception:
                hi = int(float(hi))
            return lo, hi
        except Exception as e:
            last_err = e
            msg = str(e)
            is_closed_conn = ("ORA-17008" in msg) or ("Closed connection" in msg)
            if attempt <= retries and is_closed_conn:
                print(f"[WARN] ORA-17008 en bounds (attempt {attempt}/{retries+1}). Reintento en {sleep_secs}s...")
                time.sleep(sleep_secs)
                continue
            raise
    raise last_err


def load_data_parallel_by_id(spark, base_query, oracle_conn, oracle_user, oracle_password, partition_col, lower_bound, upper_bound, num_partitions):
    reader = (
        spark.read.format("jdbc")
        .option("url", oracle_conn)
        .option("driver", "oracle.jdbc.OracleDriver")
        .option("user", oracle_user)
        .option("password", oracle_password)
        .option("dbtable", f"({base_query}) q")
        .option("fetchsize", "10000")
        .option("oracle.net.CONNECT_TIMEOUT", "120000")
        .option("oracle.jdbc.ReadTimeout", "43200000")  # 12h
    )
    print(
        f"[INFO] JDBC partitioning: {partition_col} "
        f"BETWEEN {lower_bound} AND {upper_bound} "
        f"({num_partitions} partitions)"
    )
    return (
        reader
        .option("partitionColumn", partition_col)
        .option("lowerBound", str(lower_bound))
        .option("upperBound", str(upper_bound))
        .option("numPartitions", str(num_partitions))
        .load()
    )

def get_pk_column_oracle(spark, oracle_conn, oracle_user, oracle_password, owner, table):
    sql = f"""
    SELECT acc.column_name
    FROM all_constraints ac
    JOIN all_cons_columns acc
      ON ac.owner = acc.owner
     AND ac.constraint_name = acc.constraint_name
    WHERE ac.constraint_type = 'P'
      AND ac.owner = '{owner}'
      AND ac.table_name = '{table}'
    ORDER BY acc.position
    """
    df_pk = (
        spark.read.format("jdbc")
        .option("url", oracle_conn)
        .option("driver", "oracle.jdbc.OracleDriver")
        .option("user", oracle_user)
        .option("password", oracle_password)
        .option("query", sql)
        .option("fetchsize", "50")
        .load()
    )

    rows = df_pk.collect()
    if not rows:
        return None
    return rows[0][0]

# =========================================================
# MAIN
# =========================================================
def main():
    start_time = time.time()
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    PROJECT_ID = sys.argv[1]
    TABLES_ARG = sys.argv[2]
    LOGS_BUCKET = sys.argv[3]
    CONFIG_BUCKET = sys.argv[4]

    tables_list = ast.literal_eval(TABLES_ARG)

    oracle_ip = access_secret(PROJECT_ID, "secret-id")
    oracle_port = access_secret(PROJECT_ID, "secret-id")
    oracle_service = access_secret(PROJECT_ID, "secret-id")
    oracle_user = access_secret(PROJECT_ID, "secret-id")
    oracle_password = access_secret(PROJECT_ID, "secret-id")

    DEFAULT_PARTITION_COLUMN = "COLUMN_PARTITION_ID"
    NUM_PARTITIONS = 1
    WRITE_PAR = 32
    spark = (
        SparkSession.builder
        .appName("Oracle to BigQuery Migration (Dynamic PK + Dynamic Bounds + Incremental)")
        .config("spark.executor.instances", "6")
        .config("spark.executor.cores", "2")
        .config("spark.executor.memory", "8g")
        .config("spark.executor.memoryOverhead", "3g")
        .config("spark.driver.memory", "10g")
        .config("spark.driver.memoryOverhead", "2g")
        .config("spark.network.timeout", "1200s")
        .config("spark.executor.heartbeatInterval", "60s")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.shuffle.partitions", str(WRITE_PAR))
        .config("spark.dynamicAllocation.enabled", "false")
        .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .getOrCreate()
    )
    oracle_conn = get_oracle_connection_string(oracle_ip, oracle_port, oracle_service)
    bq_client = bigquery.Client(project=PROJECT_ID)
    PARTITIONED_JSON_URI = "gs://bucket-path/tablas_particionadas.json"

    storage_client = storage.Client(project=PROJECT_ID)
    bucket_name = PARTITIONED_JSON_URI.replace("gs://", "").split("/", 1)[0]
    blob_name = PARTITIONED_JSON_URI.replace("gs://", "").split("/", 1)[1]
    
    partitioned_tables = set(
        json.loads(storage_client.bucket(bucket_name).blob(blob_name).download_as_text())
    )
    
    print(f"[INFO] Tablas particionadas (desde JSON): {len(partitioned_tables)}")
    PARTITION_OVERRIDE_MONTH = {
        "SCHEMA.EXAMPLE_TABLE": "PARTITION_COLUMN",
    }
    for table_conf in tables_list:
        oracle_table = table_conf["oracle_name"]
        bq_table = table_conf["bq_name"]
        query_string = table_conf["query"]
        date_column = table_conf.get("date_column")
        start_date = table_conf.get("start_date")
        reload = table_conf.get("reload", False)
        print(f"\n=== {oracle_table} -> {bq_table} ===")
        owner = oracle_table.split(".")[0].upper()
        table = oracle_table.split(".")[1].upper()
        partition_col = None
        pk_found = False
        if table == "EXAMPLE_TABLE":
            NUM_PARTITIONS = 32
        try:
            partition_col = get_pk_column_oracle(spark, oracle_conn, oracle_user, oracle_password, owner, table)
        except Exception as e:
            print(f"[WARN] No pude obtener PK desde Oracle para {oracle_table}: {e}")
            partition_col = None
        if not partition_col:
            pk_found = False
            partition_col = DEFAULT_PARTITION_COLUMN
        else:
            pk_found = True
            print(f"[INFO] PK detectada. partition_col={partition_col}")
        query_string, is_incremental = build_oracle_query_incremental(query_string=query_string, date_column=date_column, start_date=start_date, yesterday=yesterday, bq_client=bq_client, bq_table=bq_table, reload=reload,)
        print("[DEBUG] Oracle query final:")
        print(query_string)
        print(
            f"[INFO] incremental={is_incremental} | date_column={date_column} | "
            f"start_date='{start_date}' | reload={reload}"
        )
        print("[INFO] Calculando bounds (MIN/MAX) en Oracle...")
        if not pk_found:
            print("[WARN] PK no encontrada. Lectura single partition")
            lower_bound, upper_bound = None, None
            effective_partitions = 1
        else:
            lower_bound, upper_bound = get_dynamic_bounds(spark, query_string, oracle_conn, oracle_user, oracle_password, partition_col, table)
            if lower_bound is None or upper_bound is None:
                print("[WARN] No se encontraron bounds (tabla vacia o ID null). Salteo esta tabla.")
                continue
            if lower_bound >= upper_bound:
                print(f"[WARN] Bounds degenerados lo={lower_bound} hi={upper_bound}. Fuerzo numPartitions=1.")
                effective_partitions = 1
            else:
                effective_partitions = NUM_PARTITIONS
        if pk_found:
            print(f"[INFO] lo={lower_bound} hi={upper_bound} partitions={effective_partitions}")
        else:
            print(f"[INFO] partitions={effective_partitions}")
        print("Leyendo Oracle...")
        if not pk_found:
            df = (
                spark.read.format("jdbc")
                .option("url", oracle_conn)
                .option("driver", "oracle.jdbc.OracleDriver")
                .option("user", oracle_user)
                .option("password", oracle_password)
                .option("dbtable", f"({query_string}) q")
                .option("fetchsize", "10000")
                .option("oracle.net.CONNECT_TIMEOUT", "120000")
                .option("oracle.jdbc.ReadTimeout", "43200000")
                .load()
            )
        else:
            df = load_data_parallel_by_id(spark, query_string, oracle_conn, oracle_user, oracle_password, partition_col, lower_bound, upper_bound, effective_partitions)
        df = setPrecisionScale(df)
        if table != "EXAMPLE_TABLE":
            df = df.persist(StorageLevel.DISK_ONLY)
            print("[DEBUG] count() para materializar lectura...")
            row_count = df.count()
            print(f"[DEBUG] count={row_count}")
            df = df.repartition(WRITE_PAR)
            print(f"[DEBUG] numPartitions={df.rdd.getNumPartitions()}")
        if reload:
            full_table_id = f"{PROJECT_ID}.{bq_table}"
            print(f"[INFO] reload=True => TRUNCATE TABLE `{full_table_id}`")
            try:
                bq_client.query(f"TRUNCATE TABLE `{full_table_id}`").result()
                print("[INFO] TRUNCATE OK")
            except NotFound:
                print("[WARN] Tabla no existe en BigQuery, no se trunca")

        print("Escribiendo BigQuery (INDIRECT)...")
        
        writer = (
            df.write.format("bigquery")
            .option("table", bq_table)
            .option("writeMethod", "indirect")
            .option("temporaryGcsBucket", LOGS_BUCKET)
            .option("intermediateFormat", "parquet")
            .option("maxParallelism", str(WRITE_PAR))
            .option("allowFieldAddition", "true")
            .option("allowFieldRelaxation", "true")
            .mode("append")
        )
        
        if oracle_table in PARTITION_OVERRIDE_MONTH:
            special_col = PARTITION_OVERRIDE_MONTH[oracle_table]
            if special_col in df.columns:
                print(f"[INFO] Override particion MONTH para {bq_table} por {special_col}")
                writer = (
                    writer
                    .option("partitionField", special_col)
                    .option("partitionType", "MONTH")
                )
            else:
                print(f"[WARN] Override MONTH: columna {special_col} no existe en DF, escribo sin particion.")
        else:
            if oracle_table in partitioned_tables:
                if not date_column:
                    print(f"[WARN] {oracle_table} esta en tablas_particionadas.json pero no tiene date_column, se escribe sin particion.")
                elif date_column not in df.columns:
                    print(f"[WARN] date_column={date_column} no existe en DF para {oracle_table}, se escribe sin particion.")
                else:
                    print(f"[INFO] Escribiendo {bq_table} particionada por {date_column} (DAY default)")
                    writer = writer.option("partitionField", date_column)
        
        writer.save()
        df.unpersist()
        print("OK")
    print(f"\nTiempo total: {round(time.time() - start_time, 2)}s")
    spark.stop()


if __name__ == "__main__":
    main()
