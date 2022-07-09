#!/usr/bin/env python
import time
import uuid
import sagemaker
import json
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from sagemaker.amazon.amazon_estimator import get_image_uri

def config(**opts):
    
    region_name=opts['region_name'] if 'region_name' in opts else sagemaker.Session().boto_region_name
    
    # Hook
    hook = AwsBaseHook(aws_conn_id='airflow-sagemaker', resource_type="sagemaker")
    boto_session = hook.get_session(region_name=region_name)
    sagemaker_session =  sagemaker.session.Session(boto_session=boto_session)
    

    training_job_name=opts['training_job_name']
    bucket = opts['bucket'] if 'bucket' in opts else sagemaker_session.default_bucket() #"sagemaker-us-east-2-965425568475"
    s3_prefix = opts['s3_prefix']
    # Get the xgboost container uri
    container = get_image_uri(region_name, 'xgboost', repo_version='1.0-1')
    
    ts = f"{time.strftime('%d-%H-%M-%S', time.gmtime())}-{str(uuid.uuid4())[:8]}"   
    config = {}
    
    config["data_wrangler_config"] = {        
        "sagemaker_role":             opts['role_name'],
        #"s3_data_type"              :    defaults to "S3Prefix" 
        #"s3_input_mode"             :    defaults to "File", 
        #"s3_data_distribution_type" :    defaults to "FullyReplicated",         
        #"kms_key"                   :    defaults to None, 
        #"volume_size_in_gb"         :    defaults to 30,
        #"enable_network_isolation"  :    defaults to False, 
        #"wait_for_processing"       :    defaults to True, 
        #"container_uri"             :    defaults to "415577184552.dkr.ecr.us-east-2.amazonaws.com/sagemaker-data-wrangler-container:1.x", 
        #"container_uri_pinned"      :    defaults to "415577184552.dkr.ecr.us-east-2.amazonaws.com/sagemaker-data-wrangler-container:1.12.0",  
        "outputConfig": {
              #"s3_output_upload_mode":     #defaults to EndOfJob
              #"output_content_type":       #defaults to CSV
              #"output_bucket":             #defaults to SageMaker Default bucket
              "output_prefix": s3_prefix     #prefix within bucket where output will be written, default is generated automatically
        }
    }

    config["train_config"]={
        "AlgorithmSpecification": {
            "TrainingImage": container,
            "TrainingInputMode": "File"
        },
        "HyperParameters": {
            "max_depth": "5",
            "num_round": "10",
            "objective": "reg:squarederror"
        },
        "InputDataConfig": [
            {
                "ChannelName": "train",
                "ContentType": "csv",
                "DataSource": {
                    "S3DataSource": {
                        "S3DataDistributionType": "FullyReplicated",
                        "S3DataType": "S3Prefix",
                        "S3Uri": f"s3://{bucket}/{s3_prefix}/train"
                    }
                }
            }
        ],
        "OutputDataConfig": {
            "S3OutputPath": f"s3://{bucket}/{s3_prefix}/xgboost"
        },
        "ResourceConfig": {
            "InstanceCount": 1,
            "InstanceType": "ml.m5.2xlarge",
            "VolumeSizeInGB": 5
        },
        "RoleArn": opts['role_name'],
        "StoppingCondition": {
            "MaxRuntimeInSeconds": 86400
        },
        "TrainingJobName": training_job_name
    }
    
    config["model_config"]={
           "ExecutionRoleArn": opts['role_name'],
           "ModelName": f"XGBoost-Fraud-Detector-{ts}",
           "PrimaryContainer": { 
              "Mode": "SingleModel",
              "Image": container,
              "ModelDataUrl": f"s3://{bucket}/{s3_prefix}/xgboost/{training_job_name}/output/model.tar.gz"
           },
        }
    
    return config
