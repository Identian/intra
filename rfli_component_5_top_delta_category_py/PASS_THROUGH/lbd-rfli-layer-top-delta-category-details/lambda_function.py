import json
import boto3
import os
import logging 
import decimal
import datetime
import time

logger = logging.getLogger()
logger.setLevel(logging.INFO)

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal): 
            return float(str(o))
        return super(DecimalEncoder, self).default(o)

TOP_CATEGORY_DETAILS_TABLE = "dnb-rfli-top-delta-category-details"


def lambda_handler(event, context):
    logger.info(f'Event de entrada {event}')

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(TOP_CATEGORY_DETAILS_TABLE)
    ranking_index = int(event["queryStringParameters"].get("ranking_index",0))
    
    logger.info(f'[lambda_handler] Se busca información para el category_id:\n{ranking_index}')
    version_table     = dynamodb.Table('dnb-rfli-data-version-intra')
    
    
    response = table.get_item(Key = {'ranking_index': ranking_index})
        
    if 'Item' not in response:
        return {'headers':{'Access-Control-Allow-Headers': '*', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': '*'},
                'statusCode': 400,
                'body': f'No se encontró detalle para la categoria en la posición de ranking {ranking_index}.'}
    
    data = response['Item']
    
    
    version_response  = version_table.get_item(Key = {'component': 'top-delta-category'})
    if 'Item' not in version_response:
        logger.info('No se encontro version en la tabla')
        return {
                'headers':{ 'Access-Control-Allow-Headers': '*',
                            'Access-Control-Allow-Origin' : '*', 
                            'Access-Control-Allow-Methods': '*'},
                'statusCode': 500,
                'body': {
                    'message': 'No hay data de versión en la tabla'}
            }
    version_data = version_response['Item']
    current_version = version_data['version']
    
    logger.info(f"\ncurrent_version: {current_version}\n")
    
    final_object = {
                    'version': current_version,
                    'next_update': None,
                    'next_status': None,
                    'data': data['data']}
    
    final_response = {
        'headers':{ 
                'Access-Control-Allow-Headers': '*', 
                'Access-Control-Allow-Origin' : '*', 
                'Access-Control-Allow-Methods': '*'  }, 
                
        'body' : json.dumps(final_object, cls=DecimalEncoder)
       }
     
    logger.info(f'Respuesta de lambda {final_response}')
       
    return final_response