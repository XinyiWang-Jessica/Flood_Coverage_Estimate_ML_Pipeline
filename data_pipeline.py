import airflow
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from user_definition import *
from gee_call import *


def _download_gee_data():
    blob_name = f'{today}/b11.csv'
    df = retrieve_gee_data(toke_path, start_string, end_string)
    write_csv_to_gcs(bucket_name, blob_name, gcs_token, df)
    
with DAG(
    dag_id = 'msds697-group8',
    schedule_interval='@daily' # need to update
    start_date=datetime(2023, 1, 1),
    catchup=False
) as dag:
    
        create_insert_aggregate = SparkSubmitOperator(
        task_id="aggregate_creation",
        packages="com.google.cloud.bigdataoss:gcs-connector:hadoop2-1.9.17,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1",
        exclude_packages="javax.jms:jms,com.sun.jdmk:jmxtools,com.sun.jmx:jmxri",
        conf={"spark.driver.userClassPathFirst":True,
             "spark.executor.userClassPathFirst":True,
            #  "spark.hadoop.fs.gs.impl":"com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
            #  "spark.hadoop.fs.AbstractFileSystem.gs.impl":"com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
            #  "spark.hadoop.fs.gs.auth.service.account.enable":True,
            #  "google.cloud.auth.service.account.json.keyfile":service_account_key_file,
             },
        verbose=True,
        application='aggregates_to_mongo.py'
    )
    
    download_gee_data = PythonOperator(task_id = "download_gee_data",
                                       python_callable = _download_gee_data,
                                                    dag=dag)
    
    download_gee_data #>> create_insert_aggregate