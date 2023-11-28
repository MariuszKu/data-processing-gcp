from datetime import datetime
import polars as pl
import datetime
import gcsfs
import app.convert as con

from airflow.models.variable import Variable
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow import DAG
from airflow.decorators import task


PROJECT = Variable.get("PROJECT")
fs = gcsfs.GCSFileSystem()

@task
def branch_data():
    con.convert_branch_to_parquet()

@task
def trans_data():
    con.convert_to_parquet_card_operations()

@task
def application_data():
    con.convert_to_parquet_client_applications()    

@task
def client_data():
    con.convert_to_parquet_clients()

with DAG(
    dag_id="task_example",
    schedule=None,
    start_date=datetime.datetime.today(),
    catchup=False,
    tags=["staging"],
) as dag:
    
    dbt_dag = CloudRunExecuteJobOperator(
        task_id='dbt',
        project_id=PROJECT,
        region="europe-central2",
        job_name="dbt",
        dag=dag,
        deferrable=False,
    )

    [branch_data(), trans_data(), application_data(), client_data()] >> dbt_dag