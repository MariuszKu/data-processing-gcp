from datetime import datetime
import datetime
from airflow.models.variable import Variable

from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.operators.bash_operator import BashOperator


PROJECT = Variable.get("PROJECT")

with DAG(
    dag_id="oprtators_example",
    schedule=None,
    start_date=datetime.datetime.today(),
    catchup=False,
    tags=["staging"],
) as dag:
    

    client_dag = CloudRunExecuteJobOperator(
        task_id='clients',
        project_id=PROJECT,
        region="europe-central2",
        job_name="clients",
        dag=dag,
        deferrable=False,
    )

    app_dag = CloudRunExecuteJobOperator(
        task_id='app',
        project_id=PROJECT,
        region="europe-central2",
        job_name="applicatons",
        dag=dag,
        deferrable=False,
    )

    tran_dag = CloudRunExecuteJobOperator(
        task_id='transation',
        project_id=PROJECT,
        region="europe-central2",
        job_name="trans",
        dag=dag,
        deferrable=False,
    )

    dbt_dag = CloudRunExecuteJobOperator(
        task_id='dbt',
        project_id=PROJECT,
        region="europe-central2",
        job_name="dbt",
        dag=dag,
        deferrable=False,
    )

    [client_dag >> app_dag >> tran_dag] >> dbt_dag