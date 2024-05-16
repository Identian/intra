import os
import json
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    try:
        client = boto3.client('glue')
        portfolio_track_intra_glue_job_name = os.environ['JOB_NAME']
        response = client.start_job_run(JobName = portfolio_track_intra_glue_job_name)
        job_run_id = response["JobRunId"]
        logger.info(f"Se lanzó el Glue para la transformación de isines en pantalla intradia para fin de dia - Componente 3: JOB ID = {job_run_id}.")
        return {
            'statusCode': 200,
            'body': json.dumps('Se lanzaron el/los Glue correctamente')
        }
    except Exception:
        logger.error("Error iniciando el Glue para la inicializacion de la información de versiones - Aplicación intradia")
        raise
