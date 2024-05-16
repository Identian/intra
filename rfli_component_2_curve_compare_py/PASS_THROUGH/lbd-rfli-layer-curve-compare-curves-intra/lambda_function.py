import json
import boto3
import os
import logging
import decimal
import datetime
import time

import sys
from boto3.dynamodb.conditions import Key, Attr

#Conversion de Decimal a Float
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal): 
            return float(str(o))
        return super(DecimalEncoder, self).default(o)
        

logger = logging.getLogger()
logger.setLevel(logging.INFO)  

dynamodb = boto3.resource("dynamodb")
    
intra_table_name = "dnb-rfli-curve-compare-curves-intra" #** cambiar data_table_name
version_table_name = "dnb-rfli-data-version-intra" #** cambiar version_table_name
    
def lambda_handler(event, context):

    table = dynamodb.Table(intra_table_name) #tabla de curvas intra
    version_table = dynamodb.Table(version_table_name) #tabla de versiones
    
    logger.info('[lambda_handler] event de entrada:%s',event)
    
    curve = event["queryStringParameters"]["cc_curve"]
    
    version_request = event["queryStringParameters"]["version"]
    
    #response = None
    data = None
    #final_object = None

    version_response = version_table.query(KeyConditionExpression=Key('component').eq('compare-curves'), Limit = 1, ScanIndexForward = False)
    
    if len(version_response['Items'])==0:
        return {
                'headers':{ 'Access-Control-Allow-Headers': '*', 
                            'Access-Control-Allow-Origin': '*', 
                            'Access-Control-Allow-Methods': '*'},
                'statusCode': 400,
                'body': 'No se encontró el elemento'
            };
            
            
    print(version_response)
    
    if len(version_response['Items'])==0:
        logger.error('No se encontro version en la tabla')
        return {
                'headers':{'Access-Control-Allow-Headers': '*', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': '*'},
                'statusCode': 500,
                'body': {
                    'message': 'Error interno'}
                };
        
    version_data = version_response['Items'][0]
    current_version = version_data['version']
    next_update = version_data['next_update']
    next_status = version_data['next_status']
    
    #present_date = datetime.datetime.now()
    #now  = datetime.datetime.timestamp(present_date)*1000
    now = time.time()
    
    print(now)
    
    time_to_wait = float(next_update) - now;

    if next_status != 'intraday':
        time_to_wait = 0
        
    
    if float(version_request) != float(current_version):
        
        try:
        
            response = table.get_item(
                Key={
                    'cc_curve': curve
                }
            )

        
            data = response['Item']
        
            final_object = {
                'version':current_version,
                'next_update': time_to_wait,
                'next_status': next_status,
                'data':data['data']
            }
        
        except (Exception, ):
            logger.error('No se encontro la curva en la tabla %s', intra_table_name)
            
            return {
                    'headers':{ 'Access-Control-Allow-Headers': '*', 
                                'Access-Control-Allow-Origin': '*', 
                                'Access-Control-Allow-Methods': '*'},
                    'statusCode': 400,
                    'body': 'No se encontro el elemento'
                    }
    
    #24 - pre_eod --version se mantiene    
    else:
        
        final_object = {
                'version':current_version,
                'next_update': None,
                'next_status': next_status,
                'data': None
            }
        
    #logger.info(response)
    
    
    final_response = {
        'statusCode': 200,
        'headers':{'Access-Control-Allow-Headers': '*', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': '*'}, 
        'body' : json.dumps(final_object, cls=DecimalEncoder)
       }
    logger.info(f'{final_response}')
    return final_response 