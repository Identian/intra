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


def get_only_item(URL):
    collection_name = "dnb-rfli-isin-search-all-isines"
    try:
        if (
            "queryStringParameters" in URL
            and URL["queryStringParameters"]["isin"] is not None
        ):
            dynamodb_resource = bt3_resource("dynamodb")
            table = dynamodb_resource.Table("dnb-rfli-isin-search-all-isines")
            element = table.get_item(Key={"isin": URL["queryStringParameters"]["isin"]})
            if "Item" in element:
                    return element["Item"]
            else:
                return {
                "error": {
                    "message": "El isin no existe.",
                    "code": 400
                }
            }
        else:
            return {
                "error": {
                    "message": "La solicitud es incorrecta o defectuosa.",
                    "code": 500
                }
            }
    except Exception as get_only_item_exception:
        exception_line = sys_exc_info()[2].tb_lineno
        logger.error(
            "Error leyendo datos en dynamoDB para la tabla " + collection_name + "."
        )
        current_error = get_only_item_exception
        logger.error(
            current_error.__class__.__name__
            + "["
            + str(exception_line)
            + "] "
            + str(current_error)
        )
        raise


def get_version():
    dynamodb_resource = bt3_resource("dynamodb")
    table1 = dynamodb_resource.Table("dnb-rfli-data-version-intra")
    version = table1.get_item(Key={"component": "isin-search"})
    return version["Item"]


def create_answer_request(event):
    collection_name = "dnb-rfli-isin-search-all-isines"
    try:
        only_item = get_only_item(event)
        version_item = get_version()
        answer = {
            "next_status": version_item["next_status"],
            "version": version_item["version"],
            "next_update": version_item["next_update"],
            "data": only_item,
        }
        # print("Json de respuesta:", answer)
        return answer
    except Exception as create_string_query_exception:
        exception_line = sys_exc_info()[2].tb_lineno
        logger.error(
            "Error leyendo datos en dynamoDB para la tabla " + collection_name + "."
        )
        current_error = create_string_query_exception
        logger.error(
            current_error.__class__.__name__
            + "["
            + str(exception_line)
            + "] "
            + str(current_error)
        )
        raise


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(str(o))
        return super(DecimalEncoder, self).default(o)


def lambda_handler(event, context):
    try:
        element = create_answer_request(event)
        
        if 'error' in element['data']:
            
            return {
                'headers':{ 'Access-Control-Allow-Headers': '*',
                            'Access-Control-Allow-Origin' : '*', 
                            'Access-Control-Allow-Methods': '*'},
                'statusCode': element['data']['error']['code'],
                'body': str({
                    'message': element['data']['error']['message']
                    
                })
            };
        
        return {"statusCode": 200, "body": json.dumps(element, cls=DecimalEncoder)}
    except Exception:
        logger.error(
            "Error iniciando el Glue para la inicializacion de la información de versiones - Aplicación intradia"
        )
        raise
