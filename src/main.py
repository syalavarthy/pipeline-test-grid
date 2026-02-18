"""
Poisson Grid Image Generator Pipeline

This pipeline generates a large 2D grid of numbers from a Poisson distribution,
converts it to a black and white JPEG image, and uploads it to S3.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from datetime import datetime
import logging
import docker
import base64
import json

# Fetch AWS credentials for Docker containers
hook = AwsBaseHook(aws_conn_id='aws_default')
aws_creds = hook.get_credentials()


def get_image_uri():
    """Fetch Docker image URI from AWS Secrets Manager."""
    hook = AwsBaseHook(
        aws_conn_id='aws_default',
        client_type='secretsmanager',
        region_name='us-east-1'
    )
    client = hook.get_client_type('secretsmanager')
    
    try:
        secret_id = 'pipelines/image-uris/pipeline-269e7c31'
        response = client.get_secret_value(SecretId=secret_id)
        secret_data = json.loads(response['SecretString'])
        image_uri = secret_data.get('image_uri')
        
        logging.info(f"Retrieved image URI: {image_uri}")
        return image_uri
    except Exception as e:
        logging.error(f"Error retrieving image URI: {str(e)}")
        raise


def ecr_login_and_pull(**context):
    """Authenticate with ECR and pull the Docker image."""
    hook = AwsBaseHook(
        aws_conn_id='aws_default',
        client_type='ecr',
        region_name='us-east-1'
    )
    ecr_client = hook.get_client_type('ecr')
    
    try:
        # Get ECR authorization token
        response = ecr_client.get_authorization_token()
        token = response['authorizationData'][0]['authorizationToken']
        proxy_endpoint = response['authorizationData'][0]['proxyEndpoint']
        
        # Decode token
        decoded_token = base64.b64decode(token).decode('utf-8')
        username, password = decoded_token.split(':')
        
        # Docker login
        registry = '069729019296.dkr.ecr.us-east-1.amazonaws.com'
        docker_client = docker.DockerClient(base_url='unix://var/run/docker.sock')
        docker_client.login(username=username, password=password, registry=registry)
        
        # Pull image
        image_uri = context['task_instance'].xcom_pull(task_ids='get_image')
        logging.info(f"Pulling image: {image_uri}")
        docker_client.images.pull(image_uri)
        logging.info(f"Successfully pulled image: {image_uri}")
        
    except Exception as e:
        logging.error(f"Error during ECR login and pull: {str(e)}")
        raise


# Define the DAG
with DAG(
    dag_id='poisson_grid_image_generator',
    description='Generate a Poisson-distributed grid and convert to JPEG image on S3',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['image-processing', 'poisson', 's3'],
    default_args={
        'retries': 2,
        'owner': 'airflow',
    }
) as dag:
    
    # Infrastructure tasks
    get_image = PythonOperator(
        task_id='get_image',
        python_callable=get_image_uri
    )
    
    ecr_auth = PythonOperator(
        task_id='ecr_login',
        python_callable=ecr_login_and_pull
    )
    
    # User workflow tasks
    generate_grid = DockerOperator(
        task_id='generate_poisson_grid',
        image="{{ task_instance.xcom_pull(task_ids='get_image') }}",
        command='python src/pipeline_logic.py',
        environment={
            'TASK_NAME': 'generate_poisson_grid',
            'RUN_ID': '{{ run_id }}',
            'S3_DATA_BUCKET': 'pipelines-data-flow',
            'AWS_ACCESS_KEY_ID': aws_creds.access_key,
            'AWS_SECRET_ACCESS_KEY': aws_creds.secret_key,
            'AWS_DEFAULT_REGION': 'us-east-1'
        },
        force_pull=False,
        docker_url='unix://var/run/docker.sock',
        auto_remove='success',
        api_version='auto',
        mount_tmp_dir=False
    )
    
    create_image = DockerOperator(
        task_id='create_jpeg_image',
        image="{{ task_instance.xcom_pull(task_ids='get_image') }}",
        command='python src/pipeline_logic.py',
        environment={
            'TASK_NAME': 'create_jpeg_image',
            'RUN_ID': '{{ run_id }}',
            'S3_DATA_BUCKET': 'pipelines-data-flow',
            'AWS_ACCESS_KEY_ID': aws_creds.access_key,
            'AWS_SECRET_ACCESS_KEY': aws_creds.secret_key,
            'AWS_DEFAULT_REGION': 'us-east-1'
        },
        force_pull=False,
        docker_url='unix://var/run/docker.sock',
        auto_remove='success',
        api_version='auto',
        mount_tmp_dir=False
    )
    
    upload_to_s3 = DockerOperator(
        task_id='upload_image_to_s3',
        image="{{ task_instance.xcom_pull(task_ids='get_image') }}",
        command='python src/pipeline_logic.py',
        environment={
            'TASK_NAME': 'upload_image_to_s3',
            'RUN_ID': '{{ run_id }}',
            'S3_DATA_BUCKET': 'pipelines-data-flow',
            'AWS_ACCESS_KEY_ID': aws_creds.access_key,
            'AWS_SECRET_ACCESS_KEY': aws_creds.secret_key,
            'AWS_DEFAULT_REGION': 'us-east-1',
            'OUTPUT_S3_BUCKET': 'pipelines-data-flow'  # Change this to your target bucket
        },
        force_pull=False,
        docker_url='unix://var/run/docker.sock',
        auto_remove='success',
        api_version='auto',
        mount_tmp_dir=False
    )
    
    # Task dependencies
    get_image >> ecr_auth >> generate_grid >> create_image >> upload_to_s3
