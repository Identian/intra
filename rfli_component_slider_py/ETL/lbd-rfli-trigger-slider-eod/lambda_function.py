import os
import json
import boto3
import logging
import ast
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    try:
        client = boto3.client('glue')
        glue_job_name = os.environ['JOB_NAME']
        message = event['Records'][0]['Sns']['Message']
        print(f"El mensaje de SNS es: {message}")
        message_dictionary = ast.literal_eval(message)
        response = client.start_job_run(JobName = glue_job_name, Arguments = {'--VALUATION_DATE': message_dictionary['VALUATION_DATE']})
        job_run_id = response["JobRunId"]
        logger.info(f"Se lanzó el Glue - Slider EOD con el id {job_run_id}.")
        return {
            'statusCode': 200,
            'body': json.dumps('Se lanzaron el/los Glue correctamente')
        }
    except Exception:
        logger.error("Error iniciando el Glue para componente fin de dia - Slider")
        raise
