#!/usr/bin/env python
import time
import uuid
import sagemaker
import json
from sagemaker.processing import ProcessingInput, ProcessingOutput
from sagemaker.processing import Processor
from sagemaker.network import NetworkConfig
from airflow.models.baseoperator import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class SageMakerDataWranglerOperator(BaseOperator):
    template_fields = ["config","iam_role"]
    
    def __init__(self, 
                 flow_file_s3uri: str, 
                 processing_instance_count: int, 
                 instance_type: str,
                 aws_conn_id: str,
                 config: str,
                 **kwargs) -> None:
        super().__init__(**kwargs)
        sess = sagemaker.Session()
        
        self.config = config
        self.sagemaker_session         = sess
        self.basedir                   = "/opt/ml/processing"                                                     
        self.flow_file_s3uri           = flow_file_s3uri                                      # full form S3 URI example: s3://bucket/prefix/object
        self.processing_instance_count = processing_instance_count
        self.instance_type             = instance_type
        self.aws_conn_id               = aws_conn_id if aws_conn_id is not None else "aws_default"     # Uses the default MWAA AWS Connection
        self.s3_data_type              = self.get_config("s3_data_type", "S3Prefix", config)
        self.s3_input_mode             = self.get_config("s3_input_mode", "File", config)
        self.s3_data_distribution_type = self.get_config("s3_data_distribution_type", "FullyReplicated", config) 
        self.kms_key                   = self.get_config("kms_key", None, config)
        self.volume_size_in_gb         = self.get_config("volume_size_in_gb", 30, config)          # Defaults to 30Gb EBS volume
        self.enable_network_isolation  = self.get_config("enable_network_isolation",False, config)
        self.wait_for_processing       = self.get_config("wait_for_processing", True, config)
        self.container_uri             = self.get_config("container_uri", 
                                                    "415577184552.dkr.ecr.us-east-2.amazonaws.com/sagemaker-data-wrangler-container:1.x", config)
        self.container_uri_pinned      = self.get_config("container_uri_pinned",
                                                    "415577184552.dkr.ecr.us-east-2.amazonaws.com/sagemaker-data-wrangler-container:1.12.0", config)        
        self.s3_output_upload_mode     = self.get_config("s3_output_upload_mode", "EndOfJob", config)
        self.output_content_type       = self.get_config("output_content_type", "CSV", config["outputConfig"])                                 # CSV/PARQUET
        self.output_bucket             = self.get_config("output_bucket", sess.default_bucket(), config["outputConfig"])
        self.output_prefix             = self.get_config("output_prefix", None, config["outputConfig"])            
    
    def expand_role(self) -> None:
        if 'sagemaker_role' in self.config:
            hook = AwsBaseHook(self.aws_conn_id, client_type='iam')
            self.iam_role = hook.expand_role(self.config['sagemaker_role'])
            
    def get_config(self,flag, default, opts):
        var = opts[flag] if flag in opts else default
        return var
    
    def parse_s3_uri(self, s3_uri):
        path_parts=s3_uri.replace("s3://","").split("/")
        s3_bucket=path_parts.pop(0)
        key="/".join(path_parts)
        
        return s3_bucket, key
    
    def get_data_sources(self, data):
        # Initialize variables from .flow file
        output_node = data['nodes'][-1]['node_id']
        output_path = data['nodes'][-1]['outputs'][0]['name']
        input_source_names = [node['parameters']['dataset_definition']['name'] for node in data['nodes'] if node['type']=="SOURCE"]
        input_source_uris = [node['parameters']['dataset_definition']['s3ExecutionContext']['s3Uri'] for node in data['nodes'] if node['type']=="SOURCE"]
        
        output_name = f"{output_node}.{output_path}"
        
        data_sources = []        
        
        # Intialize data sources from .flow file
        for i in range(0,len(input_source_uris)):
            data_sources.append(ProcessingInput(
                source=input_source_uris[i],
                destination=f"{self.basedir}/{input_source_names[i]}",
                input_name=input_source_names[i],
                s3_data_type=self.s3_data_type,
                s3_input_mode=self.s3_input_mode,
                s3_data_distribution_type=self.s3_data_distribution_type
            ))
        
        return output_name, data_sources
    
    def get_processor(self):        
        # Create Processing Job
        # To launch a Processing Job, you will use the SageMaker Python SDK to create a Processor function.
        processor = Processor(
            role=self.iam_role,
            image_uri=self.container_uri,
            instance_count=self.processing_instance_count,
            instance_type=self.instance_type,
            volume_size_in_gb=self.volume_size_in_gb,
            network_config=NetworkConfig(enable_network_isolation=self.enable_network_isolation),
            sagemaker_session=self.sagemaker_session,
            output_kms_key=self.kms_key
        )
        
        return processor
        
    def execute(self, context):
        
        self.expand_role()
        
        print(f'SageMaker Data Wrangler Operator initialized with {context}...')
        # Time marker
        ts = f"{time.strftime('%d-%H-%M-%S', time.gmtime())}-{str(uuid.uuid4())[:8]}"        
        # Establish connection to S3 with S3Hook
        s3 = S3Hook(aws_conn_id=self.aws_conn_id)
        s3.get_conn()
        
        # Read the .flow file
        s3_bucket, key = self.parse_s3_uri(self.flow_file_s3uri)
        file_content = s3.read_key(key=key, bucket_name=s3_bucket)
        data = json.loads(file_content)
        
        output_name, data_sources = self.get_data_sources(data)        
                    
        # Configure Output for the SageMaker processing job
        prefix = self.output_prefix if self.output_prefix is not None else ts
        s3_output_path = f"s3://{self.output_bucket}/{prefix}"
        
        processing_job_output = ProcessingOutput(
            output_name=output_name,
            source=f"{self.basedir}/output",
            destination=s3_output_path,
            s3_upload_mode=self.s3_output_upload_mode
        )
        
        # The Data Wrangler Flow is provided to the Processing Job as an input source which we configure below.
        # Input - Flow file
        flow_input = ProcessingInput(
            source=self.flow_file_s3uri,
            destination=f"{self.basedir}/flow",
            input_name="flow",
            s3_data_type=self.s3_data_type,
            s3_input_mode=self.s3_input_mode,
            s3_data_distribution_type=self.s3_data_distribution_type
        )
        
        # Output configuration used as processing job container arguments 
        output_config = {
            output_name: {
                "content_type": self.output_content_type
            }
        }
        
        # Create a SageMaker processing Processor
        processor = self.get_processor()
        
        # Unique processing job name. Give a unique name every time you re-execute processing jobs
        processing_job_name = f"data-wrangler-flow-processing-{ts}"
        
        print(f'Starting SageMaker Data Wrangler processing job {processing_job_name} with {self.processing_instance_count} instances of {self.instance_type} and {self.volume_size_in_gb}Gb disk...')
        
        # Start Job
        processor.run(
            inputs=[flow_input] + data_sources, 
            outputs=[processing_job_output],
            arguments=[f"--output-config '{json.dumps(output_config)}'"],
            wait=self.wait_for_processing,
            logs=True,
            job_name=processing_job_name
        )
        
        print(f'SageMaker Data Wrangler processing job for flow file {self.flow_file_s3uri} complete...')
        
        # We will copy the files generated by Data Wrangler to a well known S3 location so that
        # the location can be used in our training job Task in the Airflow DAG.
        # This is because the prefix generated by Data Wrangler is dynamic.
        # We will use the default connection named 'aws_default' [Found in Airflow Admin UI > Admin Menu > Connections]
        
        print(f'Processing output files...')
        
        key_list = s3.list_keys(self.output_bucket, prefix=f"{prefix}/{processing_job_name}")
        for index, key in enumerate(key_list):
            s3.copy_object(source_bucket_key=f"s3://{self.output_bucket}/{key}",dest_bucket_key=f"{s3_output_path}/train/train-data-{index}.csv")
            
        # Delete the original file(s) since they have been moved to the well known S3 location
        s3.delete_objects(bucket=self.output_bucket, keys=key_list)
        
        data_output_path = f"{s3_output_path}/train"
        
        print(f'Saved output files at {data_output_path}...')
        return data_output_path