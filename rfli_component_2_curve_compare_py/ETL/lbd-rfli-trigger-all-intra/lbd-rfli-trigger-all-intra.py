import os
import json
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    try:
        sns = boto3.client('sns', region_name='us-east-1')
        response = sns.publish(
            TopicArn=str(os.environ['RFLI_START_INTRA_TOPIC_ARN']),
            Message="Iniciando componentes Intradia."
        )
        logger.info(f"Se publicó el mensaje con message_id {response['MessageId']}")
        return {
            'statusCode': 200,
            'body': json.dumps('Se publicó el mensaje por SNS correctamente.')
        }
    except Exception:
        logger.error("Error enviando el mensaje para inicia los componentes de Intradia.")
        raise