import sys
import time
import traceback
from datetime import datetime, timedelta
import ast
import json

from google.cloud import storage
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, LongType, DecimalType, DoubleType
from google.cloud import secretmanager, bigquery
from google.cloud.exceptions import NotFound


def access_secret(project_id, secret_id, version_id="latest"):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    return client.access_secret_version(request={"name": name}).payload.data.decode("UTF-8")


def get_oracle_connection_string(ip, port, service):
    return f"jdbc:oracle:thin:@{ip}:{port}/{service}"


def get_oracle_num_rows_stats(spark, conn, user, pwd, owner, table):
    sql = (
        "SELECT NUM_ROWS, LAST_ANALYZED "
        "FROM ALL_TAB_STATISTICS "
        f"WHERE OWNER = '{owner}' "
        f"AND TABLE_NAME = '{table}' "
        "AND PARTITION_NAME IS NULL"
    )
    try:
        df = (
            spark.read.format("jdbc")
            .option("url", conn)
            .option("driver", "oracle.jdbc.OracleDriver")
            .option("user", user)
            .option("password", pwd)
            .option("query", sql)
            .load()
        )
        r = df.collect()
        if not r or r[0]["NUM_ROWS"] is None:
            return None
        return int(float(r[0]["NUM_ROWS"]))
    except Exception:
        return None


def pick_hash_candidate(spark, conn, user, pwd, table):
    try:
        df = (
            spark.read.format("jdbc")
            .option("url", conn)
            .option("driver", "oracle.jdbc.OracleDriver")
            .option("user", user)
            .option("password", pwd)
            .option("query", f"SELECT * FROM {table} WHERE 1=0")
            .load()
        )
        for f in df.schema.fields:
            if isinstance(f.dataType, (IntegerType, LongType, DecimalType, DoubleType)):
                if any(k in f.name.lower() for k in ["id", "cod", "num", "nro", "doc"]):
                    return f.name
        for f in df.schema.fields:
            if isinstance(f.dataType, (IntegerType, LongType, DecimalType, DoubleType)):
                return f.name
    except Exception:
        pass
    return None


def hash_expr(col, buckets):
    return f"ORA_HASH(NVL(TO_CHAR({col}), 'NULL'), {buckets - 1})"


def subq_hash(query, hash_sql):
    return f"SELECT q.*, {hash_sql} AS HASH_BUCKET FROM ({query}) q"


def choose_num_buckets(rows):
    if rows is None or rows <= 10_000_000:
        return 8
    if rows <= 200_000_000:
        return 16
    return 32


def build_spark():
    """
    SparkSession
    """
    return (
        SparkSession.builder
        .appName("Oracle to BQ")
        .config("spark.dynamicAllocation.enabled", "false")
        .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .getOrCreate()
    )


def setPrecisionScale(df):
    """
    Ajusta columnas Decimal para que no superen NUMERIC(28, 9)
    """
    from pyspark.sql.types import DecimalType

    decimal_columns = [
        (field.name, field.dataType.precision, field.dataType.scale)
        for field in df.schema.fields
        if isinstance(field.dataType, DecimalType)
    ]
    print(f"Columnas decimales exediendo precision: {decimal_columns}")

    for col_name, precision, scale in decimal_columns:
        new_precision = 28 if precision > 28 else precision
        new_scale = 9 if scale > 9 else scale
        df = df.withColumn(col_name, df[col_name].cast(DecimalType(new_precision, new_scale)))
        print(f"Columna ajustada: {col_name}, Precision: {new_precision}, Escala: {new_scale}")

    return df


def main():
    PROJECT_ID, TABLES_ARG, LOGS_BUCKET, _ = sys.argv[1:5]
    tables = ast.literal_eval(TABLES_ARG)
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    print("\n------------------------------------------------------")
    print(f"Tablas a migrar: {len(tables)}")
    for i, t in enumerate(tables, start=1):
        print(f"{i}. {t['oracle_name']} -> {t['bq_name']}")
    print("------------------------------------------------------\n")

    oracle_ip = access_secret(PROJECT_ID, "secret-id")
    oracle_port = access_secret(PROJECT_ID, "secret-id")
    oracle_service = access_secret(PROJECT_ID, "secret-id")
    oracle_user = access_secret(PROJECT_ID, "secret-id")
    oracle_pwd = access_secret(PROJECT_ID, "secret-id")

    conn = get_oracle_connection_string(oracle_ip, oracle_port, oracle_service)
    bq = bigquery.Client(project=PROJECT_ID)
    PARTITIONED_JSON_URI = "gs://bucket-path/tablas_particionadas.json"

    storage_client = storage.Client(project=PROJECT_ID)
    bucket_name = PARTITIONED_JSON_URI.replace("gs://", "").split("/", 1)[0]
    blob_name = PARTITIONED_JSON_URI.replace("gs://", "").split("/", 1)[1]
    
    partitioned_tables = set(
        json.loads(storage_client.bucket(bucket_name).blob(blob_name).download_as_text())
    )
    print(f"[INFO] Tablas particionadas (desde JSON): {len(partitioned_tables)}")

    spark = build_spark()

    rows_by_table = {}
    for t in tables:
        owner, table = t["oracle_name"].split(".", 1)
        rows = get_oracle_num_rows_stats(spark, conn, oracle_user, oracle_pwd, owner, table)
        rows_by_table[t["oracle_name"]] = rows

    failed_tables = []
    table_times = {}

    # LOOP PRINCIPAL
    for t in tables:
        oracle_table = t["oracle_name"]
        bq_table = t["bq_name"]
        table_start_time = time.time()

        try:
            query = t["query"]
            date_col = t.get("date_column")
            reload = t.get("reload", False)

            if reload:
                try:
                    truncate_query = f"TRUNCATE TABLE `{bq_table}`"
                    start_truncate = time.time()
                    print(f"[INFO] RELOAD activo -> Ejecutando: {truncate_query}")
                    bq.query(truncate_query).result()
                    print(f"[INFO] Tabla {bq_table} truncada OK")
                    print(f"[INFO] Tiempo truncate bq: {round(time.time() - start_truncate, 2)} s")
                except NotFound:
                    print(f"[INFO] Tabla {bq_table} no existe, no se trunca.")

            # Incremental
            if not reload and date_col:
                start_date = (t.get("start_date") or "").strip()
                last = None
            
                try:
                    last = next(
                        bq.query(f"SELECT MAX(DATE({date_col})) d FROM `{bq_table}`").result()
                    ).d
                except NotFound:
                    last = None
            
                if last:
                    query += (
                        f" WHERE {date_col} > TO_DATE('{last}', 'YYYY-MM-DD') "
                        f"AND {date_col} < TO_DATE('{yesterday}', 'YYYY-MM-DD') + 1"
                    )
                elif start_date:
                    query += (
                        f" WHERE {date_col} >= TO_DATE('{start_date}', 'YYYY-MM-DD') "
                        f"AND {date_col} < TO_DATE('{yesterday}', 'YYYY-MM-DD') + 1"
                    )
            is_incremental = (" WHERE " in query.upper()) and (date_col is not None)
            rows = rows_by_table.get(oracle_table)
            buckets = 8 if is_incremental else choose_num_buckets(rows)

            hash_col = pick_hash_candidate(spark, conn, oracle_user, oracle_pwd, oracle_table) or "1"
            final_query = subq_hash(query, hash_expr(hash_col, buckets))

            print(f"[INFO] {oracle_table} rows={rows} incremental={is_incremental} buckets={buckets}")
            print("[DEBUG] Query final enviada a Oracle:")
            print(final_query)

            df = (
                spark.read.format("jdbc")
                .option("url", conn)
                .option("driver", "oracle.jdbc.OracleDriver")
                .option("user", oracle_user)
                .option("password", oracle_pwd)
                .option("dbtable", f"({final_query}) q")
                .option("partitionColumn", "HASH_BUCKET")
                .option("lowerBound", 0)
                .option("upperBound", buckets - 1)
                .option("numPartitions", buckets)
                .option("fetchsize", "10000")
                .load()
                .drop("HASH_BUCKET")
            )

            df = setPrecisionScale(df)

            writer = (
                df.write.format("bigquery")
                .option("table", bq_table)
                .option("writeMethod", "indirect")
                .option("temporaryGcsBucket", LOGS_BUCKET)
                .mode("append")
            )
            
            if oracle_table in partitioned_tables:
                if not date_col:
                    print(f"[WARN] {oracle_table} esta en tablas_particionadas.json pero no tiene date_column, se escribe sin particion.")
                elif date_col not in df.columns:
                    print(f"[WARN] date_column={date_col} no existe en DF para {oracle_table}, se escribe sin particion.")
                else:
                    print(f"[INFO] Escribiendo {bq_table} particionada por {date_col}")
                    writer = writer.option("partitionField", date_col)
            
            writer.save()

            elapsed = round(time.time() - table_start_time, 2)
            table_times[oracle_table] = elapsed
            print(f"[INFO] Tiempo total tabla {oracle_table}: {elapsed}")

        except Exception:
            elapsed = round(time.time() - table_start_time, 2)
            table_times[oracle_table] = elapsed
            print(f"[ERROR] Fallo la tabla {oracle_table} -> {bq_table}, tiempo {elapsed})")
            traceback.print_exc()
            failed_tables.append(f"{oracle_table} -> {bq_table}")

    spark.stop()

    print("\n------------------------------------------------------")
    print("Tiempos por tabla:")
    for tbl, t_sec in table_times.items():
        print(f"- {tbl}: {t_sec} s")

    if failed_tables:
        print("\nTablas que FALLARON:")
        for t in failed_tables:
            print(f"- {t}")
    else:
        print("\nTodas las tablas se migraron correctamente.")


if __name__ == "__main__":
    main()
