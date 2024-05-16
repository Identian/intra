import os
import json
import boto3
from sys import stdout as sys_stdout, exc_info as sys_exc_info, argv as sys_argv
from logging import getLogger, StreamHandler, Formatter, INFO
from boto3 import client as bt3_client, resource as bt3_resource
import decimal
from decimal import Decimal

def setup_logging():
    PRECIA_LOG_FORMAT = (
        "%(asctime)s [%(levelname)s] [%(filename)s](%(funcName)s): %(message)s"
    )
    logger = getLogger()
    for handler in logger.handlers:
        logger.removeHandler(handler)
    precia_handler = StreamHandler(sys_stdout)
    precia_handler.setFormatter(Formatter(PRECIA_LOG_FORMAT))
    logger.addHandler(precia_handler)
    logger.setLevel(INFO)
    return logger


logger = setup_logging()


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(str(o))
        return super(DecimalEncoder, self).default(o)
    
def get_data_request(json_request):
    collection_name = "dnb-rfli-isin-search-issuers"
    dynamodb_client = bt3_client("dynamodb")
    try:
        logger.info("Comienza lectura en Dynamo")
        response = dynamodb_client.scan(**json_request)
        return convert_clean_json(response["Items"])
    except Exception as save_data_into_dynamo_exception:
        exception_line = sys_exc_info()[2].tb_lineno
        logger.error(
            "Error leyendo datos en dynamoDB para la tabla " + collection_name + "."
        )
        current_error = save_data_into_dynamo_exception
        logger.error(
            current_error.__class__.__name__
            + "["
            + str(exception_line)
            + "] "
            + str(current_error)
        )
        raise

def convert_clean_json(dictionary_list):
    clean_list = [
        {
            clave: float(valor["N"]) if "N" in valor else valor["S"]
            for clave, valor in diccionario.items()
        }
        for diccionario in dictionary_list
    ]
    return clean_list

def lambda_handler(event, context):
    request = {
        "TableName": "dnb-rfli-isin-search-issuers",
        "ProjectionExpression": "#issuer",
        "ExpressionAttributeNames": {"#issuer":"issuer"}
    }
    try:
        element = get_data_request(request)
        return {
            'headers':{ 'Access-Control-Allow-Headers': '*',
                            'Access-Control-Allow-Origin' : '*', 
                            'Access-Control-Allow-Methods': '*'},
            "statusCode": 200, "body": json.dumps(element, cls=DecimalEncoder)}
    except Exception:
        logger.error(
            "Error iniciando el Glue para la inicializacion de la información de versiones - Aplicación intradia"
        )
        raise
