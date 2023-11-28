from datetime import datetime
import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.operators.bash_operator import BashOperator


args={
    'retries': 0,
    'catchup': False,
}

with DAG(
    dag_id="process_data1",
    schedule=None,
    start_date=datetime.datetime.today(),
    catchup=False,
    tags=["example"],
    default_args=args
) as dag:
    

    clients = BashOperator(
        task_id='clients',
        bash_command='gcloud beta run jobs execute worker-clients --region=europe-west1 --wait --args="python" --args="app/convert.py" --args="-f" --args="clients"',
        dag=dag
    )

    app = BashOperator(
        task_id='applications',
        bash_command='gcloud beta run jobs execute worker-clients --region=europe-west1 --wait --args="python" --args="app/convert.py" --args="-f" --args="applications"',
        dag=dag
    )

    tran = BashOperator(
        task_id='transactions',
        bash_command='gcloud beta run jobs execute worker-clients --region=europe-west1 --wait --args="python" --args="app/convert.py" --args="-f" --args="transactions"',
        dag=dag
    )
        
    branch = BashOperator(
        task_id='branch',
        bash_command='gcloud beta run jobs execute worker-clients --region=europe-west1 --wait --args="python" --args="app/convert.py" --args="-f" --args="branch"',
        dag=dag
    )

    dbt = BashOperator(
        task_id='dbt',
        bash_command='gcloud beta run jobs execute dbt --region=europe-west1 --wait',
        dag=dag
    )

    report = BashOperator(
        task_id='reports',
        bash_command='gcloud beta run jobs execute report-trans --region=europe-west1 --wait ',
        dag=dag
    )

    [clients, app, tran, branch ] >> dbt >> report



