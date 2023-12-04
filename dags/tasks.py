from airflow import DAG
from airflow.operators.python_operator import PythonOperator
#from airflow.operators.bash import BashOperator

import subprocess


def collection_integration():
    subprocess.run('python3 /../collection-integration.py', shell=True)
    #print('ok')


def detection_fraude():
    subprocess.run('python3 /../detection-fraude.py', shell=True)
    #print('ok')

# Define default arguments for the DAG
default_args = {
    'owner': 'NADIR Hicham',
    'start_date': datetime(2023, 12, 02),
    'retry_dely': timedelta(seconds=10),
    'end_date':None, 
}

# Create a DAG
with DAG('detection_fraude', default_args=default_args, schedule_interval="@once") as dag:#*/1 * * * *
    # Create PythonOperator tasks
    task_1 = PythonOperator(
        task_id='collection_integration',
        python_callable=collection_integration,
    )

    task_2 = PythonOperator(
        task_id='detection_fraude',
        python_callable=detection_fraude,
        dag=dag
    )
    
    # tasks
    task_1 >> task_2

if __name__ == "__main__":
    dag.cli()