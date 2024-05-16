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
import os
from boto3.dynamodb.conditions import Key, Attr
import time
import datetime  
from decimal import Decimal 

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

USER_PARAMS_TABLE  = 'dnb-rfli-isin-track-user-params'
USER_ISINES_TABLE  = 'dnb-rfli-isin-track-user-isines' 
ALL_ISINES_TABLE   = 'dnb-rfli-isin-track-all-isines'
DATA_VERSION_TABLE = 'dnb-rfli-data-version-intra'


dynamodb = boto3.resource("dynamodb")

param_store = boto3.client("ssm")

def get_user(token):     
    payload = token.split('.')[1]   
    payload_decoded = base64.urlsafe_b64decode(payload + '==')  
    payload_decoded = json.loads(payload_decoded)
    
    user = payload_decoded['unique_name']
    return user


def get_items_from_tab(items, table):
    
    isines = []
   
    for item in items:
        
        temp_key = {
            "isin": item
        }
        
        temp_item = table.get_item(Key = temp_key)
        
        if('Item' in temp_item):
            isines.append(temp_item['Item'])
            
    return isines

def lambda_handler(event, context):
    # TODO implement
    logger.info(f"event {event}")
    logger.info("entrando a lambda")
    
    user_params_table  = dynamodb.Table(USER_PARAMS_TABLE)
    all_isines_table   = dynamodb.Table(ALL_ISINES_TABLE)
    user_isines_table  = dynamodb.Table(USER_ISINES_TABLE)
    version_table      = dynamodb.Table(DATA_VERSION_TABLE)
    
    market_rate_env    = os.environ['MARKET_RATE']
    market_rate = int(param_store.get_parameter(Name=market_rate_env, WithDecryption=True)['Parameter']['Value'])

    #body_object = json.loads(event['body'], parse_float=Decimal)
    
    body_object = json.loads(event['body'])
    print(body_object)  
    
    request_isines = body_object["isines"]
    
       
    
    logger.info(f"isines de entrada {request_isines}")
     
    
    intradiapp_user = get_user(event['headers']['Authorization'])
    
    logger.info(str(event))
    
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
        
    if intradiapp_user != None:
        
        user_params = {
            'user_id': intradiapp_user,
            'isines': request_isines
        }
        
        logger.info('Isines solicitados: '+str(request_isines))
        
        #arreglo de keys para hacer request a dynamo
        batch_keys = []
        #se llena el arreglo de keys
        for isin in request_isines:
            batch_keys.append({
                'isin': isin
            })
        
        #make a batch get to ask dynamo for the requested isines
        '''get_response_isines = dynamodb.batch_get_item(
            RequestItems={
                'dnb-rfli-isin-track-all-isines': {
                    'Keys': batch_keys
                    }
                }
            )'''
            
        logger.info('removiendo isines duplicados')
        
        request_isines = list(set(request_isines))
        get_response_isines =  get_items_from_tab(request_isines, all_isines_table)
            
        
        logger.info(get_response_isines)
        
            
        #Extraer isines obtenidos de la respuesta
        #response_isines = get_response_isines['Responses']['dnb-rfli-isin-track-all-isines']  
        response_isines = get_response_isines
        
        logger.info(f"Isines obtenidos: {response_isines} ")
        print(json.dumps(response_isines, cls=DecimalEncoder))
        
        #Compare amount of request isines vs response isines
        if len(response_isines) != len(request_isines):
            
            logger.info('no se encontraron los isines de entrada')
            
            if len(response_isines) ==0:
                return {
                'headers': GENERIC_HEADERS,
                'statusCode': 404,
                'body': 'No se encontró ningún ISIN',
                'message': 'No fue posible encontrar ninguno de los isines especificados.'
                }; 
                
            
            def get_isin_name(isin):
                return isin['isin']
             
            obtained_isines = list(map(get_isin_name, response_isines))
            wrong_isines = [x for x in request_isines if x not in obtained_isines]
            
            user_params = {
                'user_id': intradiapp_user,
                'isines': obtained_isines
            }
            
            put_user_param = user_params_table.put_item(
                Item = user_params
            )
            
            user_isines = {
                'user_id': intradiapp_user,
                'isines': response_isines
            }
            
            put_all_isines = user_isines_table.put_item(
                Item = user_isines
            )
            
            final_object = {
                'version': current_version,
                'next_update': time_to_wait,
                'next_status': next_status,
                'message': f"Por favor corregir los siguientes isines: {wrong_isines}",
                'data': response_isines
                }
                
            final_response = {
            'headers': GENERIC_HEADERS, 
            'body' : json.dumps(final_object, cls=DecimalEncoder)
            }
            
            return final_response
            
        
        put_user_param = user_params_table.put_item(
            Item = user_params
        )
        
        user_isines = {
            'user_id': intradiapp_user,
            'isines': response_isines
        }
        
        put_all_isines = user_isines_table.put_item(
                Item = user_isines
            )
       
        #time_to_wait = next_update_options[next_status] - now_time
        
        final_object = {
                'version': current_version,
                'next_update': time_to_wait,
                'next_status': next_status,
                'data': response_isines
            }
       
       
      
        final_response = {
            'headers': GENERIC_HEADERS, 
            'body' : json.dumps(final_object, cls=DecimalEncoder)
        }
        
        
    else:
        logger.info("No fue posible obtener el usuario");
        
        return {
            'headers': GENERIC_HEADERS,
            'statusCode': 400,
            'body': 'Error al realizar la consulta'
        }; 
    
   
    
    return final_response

