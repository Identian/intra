import os
import json
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    try:
        client = boto3.client('glue')
        glue_job_name = os.environ['JOB_NAME']
        response = client.start_job_run(JobName = glue_job_name)
        job_run_id = response["JobRunId"]
        logger.info(f"Se lanzó el Glue - Compare Curves Intradia con el id {job_run_id}.")
        return {
            'statusCode': 200,
            'body': json.dumps('Se lanzaron el/los Glue correctamente')
        }
    except Exception:
        logger.error("Error iniciando el Glue para componente intradia - Compare Curves")
        raise
