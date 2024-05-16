import json
import boto3
import os
import logging
import decimal

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal): 
            return float(str(o))
        return super(DecimalEncoder, self).default(o)

def lambda_handler(event, context):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    dynamodb = boto3.resource("dynamodb")

    table = dynamodb.Table("dnb-rfli-curve-compare-folios-intra")
    
    logger.info(f'[lambda_handler] event de entrada:\n{event}')
   
    curve = event["queryStringParameters"]["cc_curve"]
    response = table.get_item(
        Key={
            'cc_curve': curve
        }
    )
    
    if 'Item' not in response:
        return {
            'headers':{'Access-Control-Allow-Headers': '*', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': '*'},
            'statusCode': 400,
            'body': 'No se encontró el elemento'
        };
        
    
    data = response['Item']['data']
    
    
    finalObject = {
        'version': None,
        'next_update': None,
        'data':data
    }
    
    #jsonBonito = json.loads(json.dumps(data, cls=DecimalEncoder))
    
    #json.dumps(data, cls=DecimalEncoder)
    
    final_response = {'headers':{'Access-Control-Allow-Headers': '*', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': '*'}, 'body' : json.dumps(finalObject, cls=DecimalEncoder)
       }
    logger.info(f'{final_response}')
    return final_response