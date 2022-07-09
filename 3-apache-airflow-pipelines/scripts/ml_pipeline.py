#!/usr/bin/env python
import time
import uuid
import json
import boto3
import sagemaker

# Import config file.
from config import config
from datetime import timedelta
import airflow
from airflow import DAG
from airflow.models import DAG

# airflow operators
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

# airflow sagemaker operators
from airflow.providers.amazon.aws.operators.sagemaker_training import SageMakerTrainingOperator
from airflow.providers.amazon.aws.operators.sagemaker_model import SageMakerModelOperator

# airflow sagemaker configuration
from sagemaker.amazon.amazon_estimator import get_image_uri
from sagemaker.estimator import Estimator
from sagemaker.workflow.airflow import training_config

# airflow Data Wrangler operator
from SMDataWranglerOperator import SageMakerDataWranglerOperator

# airflow dummy operator
from airflow.operators.dummy import DummyOperator


  
default_args = {  
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'provide_context': True,
    'email': ['airflow@iloveairflow.com'],
    'email_on_failure': False,
    'email_on_retry': False
}
ts                = f"{time.strftime('%d-%H-%M-%S', time.gmtime())}-{str(uuid.uuid4())[:8]}"
DAG_NAME          = f"ml-pipeline"

#-------
### Start creating DAGs
#-------

dag = DAG(  
            DAG_NAME,
            default_args=default_args,
            dagrun_timeout=timedelta(hours=2),
            # Cron expression to auto run workflow on specified interval
            # schedule_interval='0 3 * * *'
            schedule_interval=None
        )

#-------
# Task to create configurations
#-------

config_task = PythonOperator(
        task_id = 'Start',
        python_callable=config,
        op_kwargs={
            'training_job_name': f"XGBoost-training-{ts}",
            's3_prefix': 'data-wrangler-pipeline',
            'role_name': 'AmazonSageMaker-ExecutionRole-20201030T135016'},
        provide_context=True,
        dag=dag
    )

#-------
# Task with SageMakerDataWranglerOperator operator for Data Wrangler Processing Job.
#-------

def datawrangler(**context):
    config = context['ti'].xcom_pull(task_ids='Start',key='return_value')
    preprocess_task = SageMakerDataWranglerOperator(
                            task_id='DataWrangler_Processing_StepNew',
                            dag=dag,
                            flow_file_s3uri="$flow_uri",
                            processing_instance_count=2,
                            instance_type='ml.m5.4xlarge',
                            aws_conn_id="aws_default",
                            config= config["data_wrangler_config"]
                    )
    preprocess_task.execute(context)

datawrangler_task = PythonOperator(
        task_id = 'SageMaker_DataWrangler_step',
        python_callable=datawrangler,
        provide_context=True,
        dag=dag
    )

#-------
# Task with SageMaker training operator to train the xgboost model
#-------

def trainmodel(**context):
    config = context['ti'].xcom_pull(task_ids='Start',key='return_value')
    trainmodel_task = SageMakerTrainingOperator(
                task_id='Training_Step',
                config= config['train_config'],
                aws_conn_id='aws-sagemaker',
                wait_for_completion=True,
                check_interval=30
            )
    trainmodel_task.execute(context)

train_model_task = PythonOperator(
        task_id = 'SageMaker_training_step',
        python_callable=trainmodel,
        provide_context=True,
        dag=dag
    )

#-------
# Task with SageMaker Model operator to create the xgboost model from artifacts
#-------

def createmodel(**context):
    config = context['ti'].xcom_pull(task_ids='Start',key='return_value')
    createmodel_task= SageMakerModelOperator(
            task_id='Create_Model',
            config= config['model_config'],
            aws_conn_id='aws-sagemaker',
        )
    createmodel_task.execute(context)
    
create_model_task = PythonOperator(
        task_id = 'SageMaker_create_model_step',
        python_callable=createmodel,
        provide_context=True,
        dag=dag
    )

#------
# Last step
#------
end_task = DummyOperator(task_id='End', dag=dag)

# Create task dependencies

config_task >> datawrangler_task >> train_model_task >> create_model_task >> end_task
