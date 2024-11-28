from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from breweries_medallion import Stage1, Stage2, Stage3

def alert_on_failure(context):
   pass


with DAG(dag_id="breweries_stages", 
        start_date=datetime(2024,1,1), 
        schedule_interval='@daily',  
        catchup=False,
        default_args={"retries": 2,
                      'retry_delay': timedelta(minutes=5),
                      'email_on_failure': True,
                      'email_on_retry': False,
                      'email': ['your-email@example.com']
                      }
        ) as dag:
    
    stage_raw=PythonOperator(
        task_id="stage1",
        python_callable=Stage1.stage1, 
        on_failure_callback=alert_on_failure
    )
    stage_partition=PythonOperator(
        task_id="stage2",
        python_callable=Stage2.stage2, 
        on_failure_callback=alert_on_failure
    )
    stage_group=PythonOperator(
        task_id="stage3",
        python_callable=Stage3.stage3, 
        on_failure_callback=alert_on_failure
    )

stage_raw >> stage_partition >> stage_group

