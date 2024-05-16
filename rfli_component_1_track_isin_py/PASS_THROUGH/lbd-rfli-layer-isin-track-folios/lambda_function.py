import json
import boto3
import os
import logging 
import decimal

logger = logging.getLogger()
logger.setLevel(logging.INFO)

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal): 
            return float(str(o))
        return super(DecimalEncoder, self).default(o)

FOLIOS_TABLE = "dnb-rfli-isin-track-folios"


def lambda_handler(event, context):
    
    dynamodb = boto3.resource("dynamodb")
    #table = dynamodb.Table(os.environ["TABLE_NAME"])
    table = dynamodb.Table(FOLIOS_TABLE)
    
    logger.info(f'[lambda_handler] event de entrada:\n{event}')
    
    isin = event["queryStringParameters"]["isin"]
    
    response = table.get_item(
            Key = {
                'isin': isin
            }
        )
        
    if 'Item' not in response:
        return {
            'headers':{'Access-Control-Allow-Headers': '*', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': '*'},
            'statusCode': 400,
            'body': 'No se encontró el elemento'
        }
    
    data = response['Item']
    
    final_object = {
        'version': None,
        'next_update': None,
        'data': data['folios']
    }
    
    final_response = {
        'headers':{ 
                'Access-Control-Allow-Headers': '*', 
                'Access-Control-Allow-Origin' : '*', 
                'Access-Control-Allow-Methods': '*'  }, 
                
        'body' : json.dumps(final_object, cls=DecimalEncoder)
       }
     
    logger.info(f'{final_response}')
       
    return final_response