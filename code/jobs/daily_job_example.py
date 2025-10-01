from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql.types import StringType, IntegerType
import sys
import socket
from google.cloud import secretmanager
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import NotFound
import json
from datetime import datetime, timedelta

# Load environment variables
import os
PROJECT_ID = sys.argv[1]
GRUPO = sys.argv[2]
LOGS_BUCKET = sys.argv[3]
CONFIG_BUCKET = sys.argv[4]
CONFIG_FILE = sys.argv[5]

def check_connection(ip, port, timeout=2):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        sock.connect((ip, port))
        sock.close()
        print(f"Conexión exitosa a {ip}:{port}")
        return True
    except socket.error as e:
        print(f"No se pudo conectar a {ip}:{port}. Error: {e}")
        return False


def load_data(spark, query, oracle_connection_string, ORACLE_USUARIO, ORACLE_PASSWORD):
    print(f"Query to execute: {query}")
    df = spark.read.format('jdbc') \
        .option('url', oracle_connection_string) \
        .option('driver', 'oracle.jdbc.OracleDriver') \
        .option('user', ORACLE_USUARIO) \
        .option('password', ORACLE_PASSWORD) \
        .option('query', query) \
        .load()
    df.show(5)
    return df


def get_oracle_connection_string(ORACLE_IP, ORACLE_PORT, ORACLE_SERVICE):
    connection_string = 'jdbc:oracle:thin:@{}:{}/{}'.format(
        ORACLE_IP, ORACLE_PORT, ORACLE_SERVICE)
    return connection_string


def truncate_existent_table(spark, db_name, table_name):
    spark.sql(f'TRUNCATE TABLE {db_name}.{table_name}')


def access_secret(project_id, secret_id, version_id="latest"):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")


def get_file_text_from_gcs(bucket_name, blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    file_text = blob.download_as_text()
    print(f"Archivo {blob_name} leído desde el bucket {bucket_name}.")
    return json.loads(file_text)


def build_oracle_query(query_string, date_column, yesterday=None, start_date=None, last_date=None):
    if last_date is not None:
        # Modify the query to filter by last_date
        query_string += f" WHERE TRUNC({date_column}) >= TO_DATE('{last_date}', 'YYYY-MM-DD') and {date_column} < TO_DATE('{yesterday}', 'YYYY-MM-DD')"
        print(f"Modified query with last date filter: {query_string}")
        return query_string
    else:
        print("No previous data found in BigQuery, loading all data.")
        if start_date is not None:
            query_string += f" WHERE TRUNC({date_column}) >= TO_DATE('{start_date}', 'YYYY-MM-DD') and {date_column} < TO_DATE('{yesterday}', 'YYYY-MM-DD')"
            print(f"Modified query with start date: {query_string}")
            return query_string
        else:
            print("No start date provided, loading all data.")
            return query_string


def main():
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    project_id = PROJECT_ID
    group_id = GRUPO
    config_file_text = get_file_text_from_gcs(
       CONFIG_BUCKET, CONFIG_FILE)
    # Read secrets from GCP Secret Manager
    HDFS_IP = access_secret(
        project_id, 'inssert-secret-id')
    HDFS_PORT = access_secret(
        project_id, 'inssert-secret-id')
    ORACLE_IP = access_secret(
        project_id, 'inssert-secret-id')
    ORACLE_PORT = access_secret(
        project_id, 'inssert-secret-id')
    ORACLE_SERVICE = access_secret(
        project_id, 'inssert-secret-id')
    ORACLE_USUARIO = access_secret(
        project_id, 'inssert-secret-id')
    ORACLE_PASSWORD = access_secret(
        project_id, 'inssert-secret-id')

    print("------------- Probando Socket a HDFS -------------")
    if not check_connection(HDFS_IP, int(HDFS_PORT)):
        print(
            f"No se pudo conectar a HDFS en {HDFS_IP}:{HDFS_PORT}. Verifique la conexión.")
        sys.exit(1)

    print("------------- Probando Socket a Oracle CRM -------------")
    if not check_connection(HDFS_IP, int(HDFS_PORT)):
        print(
            f"No se pudo conectar a HDFS en {ORACLE_IP}:{ORACLE_PORT}. Verifique la conexión.")
        sys.exit(1)

    print("------------- Creando Spark Session -------------")
    try:
        spark = SparkSession.builder \
            .appName('ORCL_CRM_CLIENTES_HDFS_BCH') \
            .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
            .config("hive.metastore.uris", f"thrift://{HDFS_IP}:9083") \
            .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED") \
            .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED") \
            .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED") \
            .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
            .enableHiveSupport() \
            .getOrCreate()
        print("Spark session created successfully.")
    except Exception as e:
        print(f"Error creating Spark session: {e}")
        sys.exit(1)

    print("------------- Creando conexion a Oracle -------------")
    oracle_connection_string = get_oracle_connection_string(
        ORACLE_IP, ORACLE_PORT, ORACLE_SERVICE)
    print(f"Oracle connection string: {oracle_connection_string}")

    print("------------- Leyendo datos desde Oracle -------------")
    try:
        for table in config_file_text[group_id]['tables']:
            print(f"Processing table: {table['oracle_name']}")
            query_string = table['query']
            bq_client = bigquery.Client(project=project_id)
            try:
                bq_table = bq_client.get_table(table['bq_name'])
                if table['reload'] == False:
                    # Get the last date from the BigQuery table
                    query = f"SELECT MAX(EXTRACT(DATE FROM {table['date_column']})) as last_date FROM `{table['bq_name']}`"
                    query_job = bq_client.query(query)
                    result = query_job.result()
                    last_date = None
                    for row in result:
                        last_date = row.last_date
                    query_string = build_oracle_query(
                        query_string, table['date_column'], yesterday, table.get('start_date'), last_date)
                else:
                    print("Reloading all data as 'reload' is set to True.")
                    bigquery.Client(project=project_id).query_and_wait(f"TRUNCATE TABLE {table['bq_name']}")
                df = load_data(
                spark, query_string, oracle_connection_string, ORACLE_USUARIO, ORACLE_PASSWORD)
                try:
                    df.write.format('bigquery') \
                        .option('table', f'{project_id}.{table['bq_name']}') \
                        .option("writeMethod", "direct") \
                        .mode('append') \
                        .save()
                except:
                    df.write.format('bigquery') \
                        .option('table', f'{project_id}.{table['bq_name']}') \
                        .option("temporaryGcsBucket", LOGS_BUCKET) \
                        .mode('append') \
                        .save()
            except NotFound:
                print(
                    f"Table {table['bq_name']} does not exist in BigQuery. Creating table.")
                # Create the table if it does not exist
                if table['reload'] == False:
                    query_string = build_oracle_query(
                    query_string, table['date_column'], yesterday, table.get('start_date'))
                else:
                    print("Reloading all data as 'reload' is set to True.")
                df = load_data(
                    spark, query_string, oracle_connection_string, ORACLE_USUARIO, ORACLE_PASSWORD)
                # schema = [
                #     bigquery.SchemaField(field.name, field.dataType.simpleString().upper())
                #     for field in df.schema.fields
                # ]
                # bq_table = bigquery.Table(f"{project_id}.{table['bq_name']}", schema=schema)
                # bq_table = bq_client.create_table(bq_table)
                # print(f"Table {table['bq_name']} created successfully.")
                print(f"Data leída desde Oracle con éxito.")
                try:
                    df.write.format('bigquery') \
                        .option('table', f'{project_id}.{table['bq_name']}') \
                        .option("writeMethod", "direct") \
                        .mode('overwrite') \
                        .save()
                except:
                    df.write.format('bigquery') \
                        .option('table', f'{project_id}.{table['bq_name']}') \
                        .option("temporaryGcsBucket", LOGS_BUCKET) \
                        .mode('overwrite') \
                        .save()
        print("Data loaded to BigQuery successfully.")
    except Exception as e:
        print(f"Error loading data: {e}")
        sys.exit(1)
    spark.stop()


if __name__ == '__main__':
    main()
