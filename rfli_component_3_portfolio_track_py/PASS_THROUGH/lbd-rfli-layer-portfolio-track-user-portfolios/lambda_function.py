from dynamodb_encryption_sdk.encrypted import CryptoConfig
from dynamodb_encryption_sdk.encrypted.item import decrypt_python_item, encrypt_python_item
from dynamodb_encryption_sdk.identifiers import CryptoAction
from dynamodb_encryption_sdk.material_providers.aws_kms import AwsKmsCryptographicMaterialsProvider
from dynamodb_encryption_sdk.structures import AttributeActions, EncryptionContext, TableInfo
from dynamodb_encryption_sdk.transform import dict_to_ddb

import decimal
from decimal import Decimal
import boto3
import json
import logging
import os
import base64
import time
from datetime import datetime 
from boto3.dynamodb.conditions import Key, Attr
from dynamo_encryption_utils import encrypt_portf, decrypt_item

GENERIC_HEADERS = { 'Access-Control-Allow-Headers': '*', 
                    'Access-Control-Allow-Origin' : '*', 
                    'Access-Control-Allow-Methods': '*'  }
                    
DEFAULT_USER = 'default'

DEFAULT_USER                = 'default'
PORTFOLIO_PARAMS_TABLE      = 'dnb-rfli-portfolio-track-params-portfolios'
PORTFOLIO_PARAMS_HIST_TABLE = 'dnb-rfli-portfolio-track-params-portfolios-hist'
ISINES_PARAMS_TABLE         = 'dnb-rfli-portfolio-track-params-isines'
ISINES_PARAMS_HIST_TABLE    = 'dnb-rfli-portfolio-track-params-isines-hist'
ALL_ISINES_TABLE            = 'dnb-rfli-portfolio-track-all-isines'
USER_ISINES_TABLE           = 'dnb-rfli-portfolio-track-user-isines'
DATA_VERSION_TABLE          = 'dnb-rfli-data-version-intra'

dynamodb                    = boto3.resource("dynamodb")
param_store                 = boto3.client("ssm")

def get_user(token):     
    payload = token.split('.')[1]   
    payload_decoded = base64.urlsafe_b64decode(payload + '==')  
    payload_decoded = json.loads(payload_decoded)
    
    user = payload_decoded['unique_name']
    return user
    
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal): 
            return float(str(o))
        return super(DecimalEncoder, self).default(o)
        
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):

    port_param_tab   = dynamodb.Table(PORTFOLIO_PARAMS_TABLE)
    port_param_tab_h = dynamodb.Table(PORTFOLIO_PARAMS_HIST_TABLE)
    isin_param_tab   = dynamodb.Table(ISINES_PARAMS_TABLE)
    isin_param_tab_h = dynamodb.Table(ISINES_PARAMS_HIST_TABLE)
    isin_all_tab     = dynamodb.Table(ALL_ISINES_TABLE)
    isin_user_tab    = dynamodb.Table(USER_ISINES_TABLE)
    data_version_tab = dynamodb.Table(DATA_VERSION_TABLE)
  
    intradiapp_user = get_user(event['headers']['Authorization'])
    market_rate_env    = os.environ['MARKET_RATE']
    market_rate = int(param_store.get_parameter(Name=market_rate_env, WithDecryption=True)['Parameter']['Value'])
    kms_id           = os.environ['KMS_ID']
    
    
    version_response = data_version_tab.get_item(
        Key = {
            'component': 'portfolio-track'
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
    
    now_time = time.time() 
    
    next_update_options = { 
        'intraday' : next_update - int(now_time),
        'pre_eod'  : next_update - int(now_time) if (next_update - int(now_time) >= 0) else market_rate , 
        'final_eod': next_update - int(now_time)
    } 
    
    time_to_wait = next_update_options[next_status]
    
    user_isines_response = isin_user_tab.get_item(
            Key = {
                'user_id': intradiapp_user
            }
        )
        
    user_portfolio_response = decrypt_item(intradiapp_user, PORTFOLIO_PARAMS_TABLE, kms_id)
     
    print('user portfolio respons')
    print(user_portfolio_response) 
        
    if 'Item' not in user_isines_response:
        
        logger.info('No se encontró parametrización de ISINES para el usuario, se usa default')
            
        user_isines_response = isin_user_tab.get_item( 
            Key = {
                'user_id': DEFAULT_USER
                }
            )
        
        if 'Item' not in user_isines_response:
            
            logger.info('No se encontraron ISINES para para el usuario default')
                
            return {
                'headers': GENERIC_HEADERS,
                'statusCode': 404,
                'body': 'Error al realizar la consulta'
            }; 
            
    if 'portfolios' not in user_portfolio_response: 
        
        logger.info('No se encontró parametrización de PORTAFOLIO para el usuario')
        
        user_portfolio_response = decrypt_item(DEFAULT_USER, PORTFOLIO_PARAMS_TABLE, kms_id) 
        user_isines_response = isin_user_tab.get_item(
            Key = {
                'user_id': DEFAULT_USER
            }
        )
        
        
        if user_portfolio_response == {}:
            logger.info('No se encontró PORTAFOLIO para para el usuario default')
            return {
                'headers': GENERIC_HEADERS,
                'statusCode': 404,
                'body': 'Error al realizar la consulta'
            }; 
        
            
        logger.info('Se retornó el PORTAFOLIO DEFAULT')
       
    
    isines = user_isines_response['Item']['isines']
    portfolios = user_portfolio_response['portfolios']
    
    
    
    complete_portfolio = {
        'portfolios': portfolios,
        'isines': isines
    }
    
    final_object = {
                'version': current_version,
                'next_update': time_to_wait,
                'next_status': next_status,
                'data': complete_portfolio,
            }
    
    
    final_response = {
        'headers': GENERIC_HEADERS, 
        'body' : json.dumps(final_object, cls=DecimalEncoder)
       } 
   
    return final_response