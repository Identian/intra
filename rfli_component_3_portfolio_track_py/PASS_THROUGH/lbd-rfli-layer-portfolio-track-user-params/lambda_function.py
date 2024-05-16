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

GENERIC_HEADERS             = {'Access-Control-Allow-Headers': '*', 
                               'Access-Control-Allow-Origin' : '*', 
                               'Access-Control-Allow-Methods': '*' }
                    
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
     
    print(type(event['body'])) 
     
    #print(event['body']['portfolios']) 
    
    port_param_tab   = dynamodb.Table(PORTFOLIO_PARAMS_TABLE)
    port_param_tab_h = dynamodb.Table(PORTFOLIO_PARAMS_HIST_TABLE)
    isin_param_tab   = dynamodb.Table(ISINES_PARAMS_TABLE)
    isin_param_tab_h = dynamodb.Table(ISINES_PARAMS_HIST_TABLE)
    isin_all_tab     = dynamodb.Table(ALL_ISINES_TABLE)
    isin_user_tab    = dynamodb.Table(USER_ISINES_TABLE)
    data_version_tab = dynamodb.Table(DATA_VERSION_TABLE) 
     
    market_rate_env  = os.environ['MARKET_RATE']
    kms_id           = os.environ['KMS_ID']
    market_rate      = int(param_store.get_parameter(Name=market_rate_env, WithDecryption=True)['Parameter']['Value'])
    
    request_body     = json.loads(event['body'])
    portfolios       = request_body["portfolios"]
    print(request_body)
    
    request_isines   = request_body["isines"]
    
    response_isines  = get_items_from_tab(request_isines, isin_all_tab)
    
    intradiapp_user  = get_user(event['headers']['Authorization'])
    
    logger.info(intradiapp_user) 
    
    logger.info(f"isines solicitados {len(request_isines)}")
    logger.info(f"isines encontrados {len(response_isines)}")
    
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
    
    if(intradiapp_user != None):
        
        logger.info('Usuario existe')
        
        def get_isin_name(isin):
                return isin['isin']
        
        if len(response_isines) != len(request_isines):
            
            logger.info('algún isin es incorrecto')
            
            if len(response_isines) ==0:
                logger.info('no se encontraron los isines de entrada')
                
                return {
                'headers': GENERIC_HEADERS,
                'statusCode': 404,
                'body': 'No se encontró ningún ISIN',
                'message': 'No fue posible encontrar ninguno de los isines especificados.'
                }; 
                
            obtained_isines = list(map(get_isin_name, response_isines))
            wrong_isines = [x for x in request_isines if x not in obtained_isines]
            
            user_isin_params = {
                'user_id': intradiapp_user,
                'isines': obtained_isines
            }
            
            put_user_isin_param = isin_param_tab.put_item(
                Item = user_isin_params
            )
            
            user_isin_params.update({"update_date": str(datetime.now())})
            
            put_user_isines_h = isin_param_tab_h.put_item(
                Item = user_isin_params)
            
            user_isines = {
                'user_id': intradiapp_user,
                'isines': response_isines
            }
            
            put_user_isines = isin_user_tab.put_item(
                Item = user_isines
                )
                
            print(str(portfolios))     
            
            portfolio_object = {
                "portfolios": portfolios,
                "isines": response_isines
            }
            
            final_object = {
                'version': current_version,
                'next_update': time_to_wait,
                'next_status': next_status,
                'message': f"Por favor corregir los siguientes isines: {wrong_isines}",
                'data': portfolio_object
                }
                
            final_response = {
                'headers': GENERIC_HEADERS, 
                'body' : json.dumps(final_object, cls=DecimalEncoder)
            }    
            
            
            logger.info('Encriptando y guardando portafolio')
                 
            encrypted_portfolio = encrypt_portf(intradiapp_user, portfolios, PORTFOLIO_PARAMS_TABLE, kms_id)
            
            encrypted_portfolio.update({"update_date": str(datetime.now())})
            
            print(encrypted_portfolio) 
            
            print("hola")
            #print(decrypt_item(intradiapp_user, PORTFOLIO_PARAMS_TABLE, kms_id))
            
            logger.info('Encriptando y guardando portafolio histórico')
            
            port_param_tab_h.put_item(Item = encrypted_portfolio)
        
        else:
            
            logger.info('Todos los isines son válidos')
            
            user_isin_params = {
                'user_id': intradiapp_user,
                'isines': request_isines
            }
            
            put_user_param = isin_param_tab.put_item(
                Item = user_isin_params
            )
            
            user_isin_params.update({"update_date": str(datetime.now())})
            
            put_user_isines_h = isin_param_tab_h.put_item(
                Item = user_isin_params)
            
            user_isines = {
                'user_id': intradiapp_user,
                'isines': response_isines
            }
            
            put_user_isines = isin_user_tab.put_item(
                Item = user_isines
                )
                 
            portfolio_object = {
                "portfolios": portfolios,
                "isines": response_isines
            }    
                
            final_object = {
                'version': current_version,
                'next_update': time_to_wait,
                'next_status': next_status,
                'data': portfolio_object
                }
                 
            final_response = {
                'headers': GENERIC_HEADERS, 
                'body' : json.dumps(final_object, cls=DecimalEncoder) 
            } 
            
            encrypted_portfolio = encrypt_portf(intradiapp_user, portfolios, PORTFOLIO_PARAMS_TABLE, kms_id)
            
            encrypted_portfolio.update({"update_date": str(datetime.now())})
             
            port_param_tab_h.put_item(Item = encrypted_portfolio)
            
    else:
        
        logger.info('no se encontró usuario válido') 
        
        return {
                'headers':{ 'Access-Control-Allow-Headers': '*',
                            'Access-Control-Allow-Origin' : '*', 
                            'Access-Control-Allow-Methods': '*'},
                'statusCode': 500,
                'body': {
                    'message': 'No se encontró al usuario'
                }
            };
            
            
    return final_response
    
    
    
    """
def encrypt_portf(user, portfolio, table_name, kms_id):
    obj = {
        "user_id": user,
        "portfolios": portfolio
    }
    
    
    
    encrypted_attributes = set(obj.keys())
    
    table = dynamodb.Table(table_name)
    table_info = TableInfo(name=table_name)
    table_info.refresh_indexed_attributes(table.meta.client)
    aws_kms_cmp = AwsKmsCryptographicMaterialsProvider(kms_id)
    encryption_context = EncryptionContext(
        table_name=table_name,
        partition_key_name=table_info.primary_index.partition
    )
    
    actions = AttributeActions(
        attribute_actions={"user_id": CryptoAction.DO_NOTHING},
        default_action=CryptoAction.ENCRYPT_AND_SIGN
    )
    
    actions.set_index_keys(*table_info.protected_index_keys())
    crypto_config = CryptoConfig(
        materials_provider=aws_kms_cmp, encryption_context=encryption_context, attribute_actions=actions
    )

    encrypted_item = encrypt_python_item(obj, crypto_config)
    
    table.put_item(Item=encrypted_item)
    
    return encrypted_item

"""

"""
def decrypt_item(user, table_name, kms_id):
    table = dynamodb.Table(table_name)
    
    table_info = TableInfo(name=table_name)
    table_info.refresh_indexed_attributes(table.meta.client)
    
    aws_kms_cmp = AwsKmsCryptographicMaterialsProvider(kms_id)
    
    encryption_context = EncryptionContext(
        table_name=table_name,
        partition_key_name=table_info.primary_index.partition,
    )
    
    actions = AttributeActions(
        attribute_actions={"user_id": CryptoAction.DO_NOTHING},
        default_action=CryptoAction.ENCRYPT_AND_SIGN
    )
    
    crypto_config = CryptoConfig(
        materials_provider=aws_kms_cmp, encryption_context=encryption_context, attribute_actions=actions
    )
    
    read_item = table.get_item(Key = {"user_id": user})
    decrypted_item = decrypt_python_item(read_item["Item"], crypto_config)
    return decrypted_item
    """