import datetime

from airflow import models
from airflow.models.variable import Variable
from airflow.providers.google.cloud.operators.dataflow import  DataflowStartFlexTemplateOperator
from airflow.operators import empty
from airflow.providers.google.cloud.operators.bigquery import  BigQueryInsertJobOperator

yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
PROJECT = Variable.get("PROJECT")
REGION = Variable.get("GCP_REGION")
LANDING = Variable.get("LANDING")
SILVER = Variable.get("LANDING")

default_args = {
  'owner': 'airflow',
  'start_date': yesterday,
  'depends_on_past': False,
  'email': [''],
  'email_on_failure': False,
  'email_on_retry': False,
  'retries': 1,
  'retry_delay': datetime.timedelta(minutes=5),
  
}

dataflow_environment = {
    'workerZone': "europe-west1-b",

    'stagingLocation': "gs://LANDING",
    'tempLocation': "gs://LANDING" + "/tmp",
}


with models.DAG(
    'data_pipeline_dag',
    default_args=default_args,
    tags=["v1"],
    schedule_interval=None) as dag:


    start = empty.EmptyOperator(
        task_id='start',
        trigger_rule='all_success'
    )


    card_operations = DataflowStartFlexTemplateOperator(
        task_id='dataflow_transactions',
        project_id=PROJECT,
        location="europe-west1",
        body={
            'launchParameter': {
                'jobName': f'dataflow-tran-import-tran',
                "containerSpecGcsPath":"gs://dataflow-templates-europe-west1/latest/flex/File_Format_Conversion",
                'parameters': {
                    "inputFileFormat":"csv",
                    "outputFileFormat":"parquet",
                    "inputFileSpec":f"gs://{LANDING}/card_operations.csv",
                    "outputBucket":f"gs://{SILVER}/df_transactions/",
                    "schema":f"gs://{LANDING}/transactions.avsc",
                }
            }
        })
    
    client_applications = DataflowStartFlexTemplateOperator(
        task_id='dataflow_applications',
        project_id=PROJECT,
        location="europe-west1",
        body={
            'launchParameter': {
                'jobName': f'dataflow-tran-import-app',
                "containerSpecGcsPath":"gs://dataflow-templates-europe-west1/latest/flex/File_Format_Conversion",
                'parameters': {
                    "inputFileFormat":"csv",
                    "outputFileFormat":"parquet",
                    "inputFileSpec":f"gs://{LANDING}/client_applications.csv",
                    "outputBucket":f"gs://{SILVER}/df_client_applications/",
                    "schema":f"gs://{LANDING}/applications.avsc",
                }
            }
        })
    
    clients = DataflowStartFlexTemplateOperator(
        task_id='dataflow_clients',
        project_id=PROJECT,
        location="europe-west1",
        body={
            'launchParameter': {
                'jobName': f'dataflow-tran-import-client',
                "containerSpecGcsPath":"gs://dataflow-templates-europe-west1/latest/flex/File_Format_Conversion",
                'parameters': {
                    "inputFileFormat":"csv",
                    "outputFileFormat":"parquet",
                    "inputFileSpec":f"gs://{LANDING}/clients.csv",
                    "outputBucket":f"gs://{SILVER}/df_clients/",
                    "schema":f"gs://{LANDING}/clients.avsc",
                }
            }
        })
    



    start >> [card_operations, client_applications, clients] 