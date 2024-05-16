import json
import base64
import logging  
import sys  
import jwt  
import requests
from cryptography.x509 import load_pem_x509_certificate
from cryptography.hazmat.backends import default_backend
import boto3
import decimal
from boto3.dynamodb.conditions import Key, Attr
import time
import datetime 
import os

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal): 
            return float(str(o))
        return super(DecimalEncoder, self).default(o)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

GENERIC_HEADERS = { 'Access-Control-Allow-Headers': '*', 
                    'Access-Control-Allow-Origin' : '*', 
                    'Access-Control-Allow-Methods': '*'  }
                    
DEFAULT_USER = 'default'

USER_ISINES_TABLE = 'dnb-rfli-isin-track-user-isines'
DATA_VERSION_TABLE = 'dnb-rfli-data-version-intra'


def get_user(token):     
    payload = token.split('.')[1]   
    payload_decoded = base64.urlsafe_b64decode(payload + '==')  
    payload_decoded = json.loads(payload_decoded)
    
    user = payload_decoded['unique_name']
    return user


def lambda_handler(event, context):
    
    dynamodb = boto3.resource("dynamodb")
    param_store = boto3.client("ssm")
    
    
    
    #obtener usuario a partir del token
    intradiapp_user = get_user(event['headers']['Authorization'])
  
    market_rate_env    = os.environ['MARKET_RATE']
    market_rate = int(param_store.get_parameter(Name=market_rate_env, WithDecryption=True)['Parameter']['Value'])
    
    user_isines_table = dynamodb.Table(USER_ISINES_TABLE)
    version_table     = dynamodb.Table(DATA_VERSION_TABLE)
    
    market_rate_env    = os.environ['MARKET_RATE']
    
    market_rate = int(param_store.get_parameter(Name=market_rate_env, WithDecryption=True)['Parameter']['Value'])
    
    version_response  = version_table.get_item(
        Key = {
                'component': 'isin-track'
            }
        )

    if 'Item' not in version_response:
        logger.info('No se encontro version en la tabla')
        return {
                'headers':{ 'Access-Control-Allow-Headers': '*',
                            'Access-Control-Allow-Origin' : '*', 
                            'Access-Control-Allow-Methods': '*'},
                'statusCode': 500,
                'body': {
                    'message': 'No hay data de versión en la tabla'}
            };
    
    version_data = version_response['Item']
    
    
    #if next_status == 'intraday':
    
    current_version = version_data['version']
    next_status = version_data['next_status']
    next_update = version_data['next_update']
    
    aux_date = datetime.datetime.now()
    now_time = time.mktime(aux_date.timetuple())
    
    
    d_parts  = str(aux_date).split()[0].split('-')
    
    zero_based_date = datetime.datetime(int(d_parts[0]), int(d_parts[1]), int(d_parts[2])).timetuple()
    zero_based_time = time.mktime(zero_based_date)
    
    next_update_options = { 
        'intraday' : next_update - int(now_time),
        'pre_eod'  : next_update - int(now_time) if (next_update - int(now_time) >= 0) else market_rate , 
        'final_eod': next_update - int(now_time)
    }    
     
    time_to_wait = next_update_options[next_status]
        
    logger.info(f"\ncurrent_version: {current_version}\nnext_update: {time_to_wait}\nnext_status: {next_status}");
    
    
    if intradiapp_user != None:
        
        response = user_isines_table.get_item(
            Key = {
                'user_id': intradiapp_user
            }
        )
        
        if 'Item' not in response:
            
            logger.info('No se encontró parametrización para el usuario, se usa default')
            
            response = user_isines_table.get_item( 
                Key = {
                    'user_id': DEFAULT_USER
                    }
                )
                
            if 'Item' not in response:
                
                logger.info('No se encontró información para el usuario default')
                
                return {
                    'headers': GENERIC_HEADERS,
                    'statusCode': 404,
                    'body': 'Error al realizar la consulta'
                }; 
                
                
            isines = response['Item']['isines']
          
            final_object = {
                'version': current_version,
                'next_update': time_to_wait,
                'next_status': next_status,
                'data': isines,
            }
            
            
        else:
        
            isines = response['Item']['isines']
            
            final_object = {
                'version': current_version,
                'next_update': time_to_wait,
                'next_status': next_status,
                'data': isines
            }
        
    else: 
        
        logger.info("No fue posible obtener el usuario");
        
        return {
            'headers': GENERIC_HEADERS,
            'statusCode': 400,
            'body': 'Error al realizar la consulta'
        }; 
        
        
    final_response = {
        'headers': GENERIC_HEADERS, 
        'body' : json.dumps(final_object, cls=DecimalEncoder)
       }
    
    return final_response