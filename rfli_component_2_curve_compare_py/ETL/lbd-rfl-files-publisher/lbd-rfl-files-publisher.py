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
        glue_sync_job_name = os.environ['SYNC_JOB_NAME']
        # glue_eod_job_name = os.environ['EOD_JOB_NAME']
        files_id = ",".join(event['files'])
        tables_id = ",".join(list(set(event['sync_tables']+event['files'])))
        response = client.start_job_run(JobName = glue_job_name, Arguments = {'--VALUATION_DATE': event['valuation_date'], '--FILES_ID': files_id})
        job_run_id = response["JobRunId"]
        logger.info(f"Se lanzó el Glue para la publicación de archivos con el id {job_run_id}")
        response = client.start_job_run(JobName = glue_sync_job_name, Arguments = {'--VALUATION_DATE': event['valuation_date'], '--TABLES_ID': tables_id})
        job_run_id = response["JobRunId"]
        logger.info(f"Se lanzó el Glue para la sincronización de archivos con el id {job_run_id}")
        send_sms(str({'VALUATION_DATE': event['valuation_date'], 'FILES_ID': tables_id}) )
        return {
            'statusCode': 200,
            'body': json.dumps('Se lanzaron el/los Glue correctamente')
        }
    except Exception:
        logger.error("Error iniciando el Glue publicador de archivos")
        raise

def send_sms(message):
    sns = boto3.client('sns', region_name='us-east-1')
    response = sns.publish(
        TopicArn=str(os.environ['RFLI_START_EOD_TOPIC_ARN']),
        Message=str(message),
    )
    logger.info(f"Se publicó el mensaje con message_id {response['MessageId']}")
