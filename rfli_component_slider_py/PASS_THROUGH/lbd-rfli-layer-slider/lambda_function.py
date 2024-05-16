import json
import logging  
import boto3
import decimal
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
                    

SLIDER_COLLECTION= 'dnb-rfli-slider'
DATA_VERSION_TABLE = 'dnb-rfli-data-version-intra'


def lambda_handler(event, context):
    dynamodb = boto3.resource("dynamodb")
    param_store = boto3.client("ssm")
    market_rate_env    = os.environ['MARKET_RATE']
    market_rate = int(param_store.get_parameter(Name=market_rate_env, WithDecryption=True)['Parameter']['Value'])
    SLIDER_COLLECTION_table = dynamodb.Table(SLIDER_COLLECTION)
    version_table     = dynamodb.Table(DATA_VERSION_TABLE)
    version_response  = version_table.get_item(
        Key = {
                'component': 'slider'
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
            }
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
    logger.info(f"\ncurrent_version: {current_version}\nnext_update: {time_to_wait}\nnext_status: {next_status}")
    response = SLIDER_COLLECTION_table.get_item(
        Key = {
            'slider_key': 1
        }
    )
    if 'Item' not in response:
        logger.info('No se encontró data para el top category')
        final_object = {
            'version': current_version,
            'next_update': time_to_wait,
            'next_status': next_status,
            'data': None,
        }
    else:
        data = response['Item']['data']
        final_object = {
            'version': current_version,
            'next_update': time_to_wait,
            'next_status': next_status,
            'data': data
        } 
    final_response = {
        'headers': GENERIC_HEADERS, 
        'body' : json.dumps(final_object, cls=DecimalEncoder)
       }
    
    return final_response