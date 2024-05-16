import json
import boto3
import os
import logging
import decimal
import time
import datetime 
import sys
from boto3.dynamodb.conditions import Key, Attr
import os

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(str(o))
        return super(DecimalEncoder, self).default(o)
        
        
logger = logging.getLogger()
logger.setLevel(logging.ERROR)
dynamodb = boto3.resource("dynamodb")
param_store = boto3.client("ssm")


eod_table_name = "dnb-rfli-curve-compare-curves-eod" #** cambiar data_table_name
version_table_name = "dnb-rfli-data-version-intra" #** cambiar version_table_name

def lambda_handler(event, context):
    
    table = dynamodb.Table(eod_table_name)
    version_table = dynamodb.Table(version_table_name)
    version_response = version_table.query(KeyConditionExpression=Key('component').eq('compare-curves'), Limit = 1, ScanIndexForward = False)
    print(version_response)
    print(len(version_response['Items']))
    

    
    market_time = int(param_store.get_parameter(Name=os.environ['RATE_TIME'], WithDecryption=True)['Parameter']['Value'])
    
    if len(version_response['Items'])==0:
        return {
                'headers':{ 'Access-Control-Allow-Headers': '*', 
                            'Access-Control-Allow-Origin': '*', 
                            'Access-Control-Allow-Methods': '*'},
                'statusCode': 400,
                'body': 'No se encontró el elemento'
            };
    
    
    version_data = version_response['Items'][0]
    curve          = event["queryStringParameters"]["cc_curve"]
    
    valuation_date = event["queryStringParameters"]["valuation_date"]
    
    
    if 'version' in event["queryStringParameters"]:
        
        user_version = event["queryStringParameters"]["version"]
        next_status = version_data['next_status']
        next_update = version_data['next_update']
        now = time.time()
        db_version = version_data['version']
        today = str(datetime.date.today())
        
        print(today)
        
        aux_date = datetime.datetime.now();
        hour = (int(aux_date.strftime("%H"))) 
        minutes = int(aux_date.strftime("%M"))
        seconds = int(aux_date.strftime("%S"))
        
        logger.info(next_status)
        
        if float(user_version) == float(db_version): 
            
            if next_status == 'pre_eod':
                print('por aca')
                
                wait_time = next_update - int(now)
                print(now)
                
                final_object = {
                    'data': None,
                    'version': db_version,
                    'next_update': wait_time if wait_time >= 0 else market_time,
                    'next_status': next_status
                }
                
            elif next_status == 'final_eod':
                
                response = table.get_item(
                    Key={
                        'valuation_date': today,
                        'cc_curve': curve
                    }
                )
                
                
                
                if 'Item' in response:
                    
                    data = response['Item']['data']
                  
                    wait_time = (20 - hour + 5)*3600 - minutes*60 - seconds
                
                    final_object = {
                        'version': db_version,
                        'next_update': wait_time,
                        'next_status': next_status,
                        'data': data
                    }
                    
                else:
                    
                    final_object = {
                        'version': db_version,
                        'next_update': market_time,
                        'next_status': next_status,
                        'data': None
                    }
                
            
            
        else:
            
            
            response = table.get_item(
            Key={
                'cc_curve': curve,
                'valuation_date': today
                }
            )
        
            if 'Item' in response:
                data = response['Item']
                wait_time = (20 - hour + 5)*3600 - minutes*60 - seconds
                
                final_object = {
                    'version': db_version,
                    'next_update': wait_time,
                    'next_status': next_status,
                    'data': data['data']
                }
                
            else:
                
                final_object = {
                    'version': db_version,
                    'next_update': market_time,
                    'next_status': next_status,
                    'data': None
                }
                
    else:
        
        response = table.get_item(
            Key={
                'cc_curve': curve,
                'valuation_date': valuation_date
            }
        )

        #logger.info('%s', check_intra)
        print(valuation_date)
        print('else')
    
        if 'Item' not in response:
            return {
                'headers':{'Access-Control-Allow-Headers': '*', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': '*'},
                'statusCode': 400,
                'body': 'No se encontró el elemento'
            };
    
        logger.info(response['Item'])
        data = response['Item']
    
        final_object = {
            'version': None,
            'next_update': None,
            "data": data['data']
        }
    
    final_response = {  'headers':{'Access-Control-Allow-Headers': '*', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': '*'}, 
                        'body' : json.dumps(final_object, cls=DecimalEncoder)
                      }
    #logger.info(f'{final_response}')
    return final_response