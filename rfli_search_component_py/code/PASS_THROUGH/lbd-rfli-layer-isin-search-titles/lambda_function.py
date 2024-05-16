import os
import json
from sys import stdout as sys_stdout, exc_info as sys_exc_info
from logging import getLogger, StreamHandler, Formatter, INFO
from boto3 import resource as bt3_resource
from boto3.dynamodb.conditions import And, Attr
from boto3.dynamodb.types import DYNAMODB_CONTEXT
from decimal import Decimal, Inexact, Rounded
from functools import reduce


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

DYNAMODB_CONTEXT.traps[Inexact] = 0
DYNAMODB_CONTEXT.traps[Rounded] = 0

logger = setup_logging()
GENERIC_HEADERS = { 'Access-Control-Allow-Headers': '*', 
                    'Access-Control-Allow-Origin' : '*', 
                    'Access-Control-Allow-Methods': '*'  }


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(str(o))
        return super(DecimalEncoder, self).default(o)


def build_filter_conditions(entry_body):
    try:
        conditions_list = []
        if entry_body['issuer']:
            logger.info(f"Se encontró un valor para issuer: {entry_body['issuer']}")
            conditions_list.append(
                Attr('issuer_name').eq(str(entry_body['issuer']))
            )
        if entry_body['rating']:
            logger.info(f"Se encontró un valor para rating: {entry_body['rating']}")
            conditions_list.append(
                Attr('real_rating').eq(str(entry_body['rating']))
            )
        if entry_body['rate_type']:
            logger.info(f"Se encontró un valor para rate_type: {entry_body['rate_type']}")
            conditions_list.append(
                Attr('rate_type').eq(str(entry_body['rate_type']))
            )
        if entry_body['currency']:
            logger.info(f"Se encontró un valor para currency: {entry_body['currency']}")
            conditions_list.append(
                Attr('currency_type').eq(str(entry_body['currency']))
            )
        if entry_body['class_name']:
            logger.info(f"Se encontró un valor para class_name: {entry_body['class_name']}")
            conditions_list.append(
                Attr('class_name').eq(str(entry_body['class_name']))
            )
        if entry_body['maturity_days']['min'] and entry_body['maturity_days']['max']:
            logger.info(f"Se encontró para maturity_days el valor min: {entry_body['maturity_days']['min']} y el max: {entry_body['maturity_days']['max']}")
            conditions_list.append(
                Attr('maturity_days').between(DYNAMODB_CONTEXT.create_decimal(entry_body['maturity_days']['min']),DYNAMODB_CONTEXT.create_decimal(entry_body['maturity_days']['max']))
            )
        elif not entry_body['maturity_days']['min'] and entry_body['maturity_days']['max']:
            logger.info(f"Se encontró para maturity_days el valor max: {entry_body['maturity_days']['max']}")
            conditions_list.append(
                Attr('maturity_days').lte(DYNAMODB_CONTEXT.create_decimal(entry_body['maturity_days']['max']))
            )
        elif entry_body['maturity_days']['min'] and not entry_body['maturity_days']['max']:
            logger.info(f"Se encontró para maturity_days el valor min: {entry_body['maturity_days']['min']}")
            conditions_list.append(
                Attr('maturity_days').gte(DYNAMODB_CONTEXT.create_decimal(entry_body['maturity_days']['min']))
            )
        if entry_body['yield']['min'] and entry_body['yield']['max']:
            logger.info(f"Se encontró para yield el valor min: {entry_body['yield']['min']} y el max: {entry_body['yield']['max']}")
            conditions_list.append(
                Attr('yield').between(DYNAMODB_CONTEXT.create_decimal(entry_body['yield']['min']),DYNAMODB_CONTEXT.create_decimal(entry_body['yield']['max']))
            )
        elif not entry_body['yield']['min'] and entry_body['yield']['max']:
            logger.info(f"Se encontró para yield el valor max: {entry_body['yield']['max']}")
            conditions_list.append(
                Attr('yield').lte(DYNAMODB_CONTEXT.create_decimal(entry_body['yield']['max']))
            )
        elif entry_body['yield']['min'] and not entry_body['yield']['max']:
            logger.info(f"Se encontró para yield el valor min: {entry_body['yield']['min']}")
            conditions_list.append(
                Attr('yield').gte(DYNAMODB_CONTEXT.create_decimal(entry_body['yield']['min']))
            )
        logger.info(f"La lista resultante es: {conditions_list}")
        return conditions_list
    except Exception as build_filter_conditions_exception:
        exception_line = sys_exc_info()[2].tb_lineno
        logger.error(
            "Error generando los filtros para el buscador."
        )
        current_error = build_filter_conditions_exception
        logger.error(
            current_error.__class__.__name__
            + "["
            + str(exception_line)
            + "] "
            + str(current_error)
        )
        raise 
    
    
def get_dynamo_db_data(filters, current_last_key={}):
    try:
        all_table_items = []
        last_key = None
        collection_name = "dnb-rfli-isin-search-all-isines"
        dynamodb = bt3_resource('dynamodb')
        table = dynamodb.Table(collection_name)
        if filters:
            if current_last_key:
                response = table.scan(FilterExpression=reduce(And,(filters)), ExclusiveStartKey=current_last_key)
            else:
                response = table.scan(FilterExpression=reduce(And,(filters)))
        else:
            if current_last_key:
                response = table.scan(ExclusiveStartKey=current_last_key)
            else:
                response = table.scan()
        all_table_items.extend([{k: item[k] for k in item} for item in response['Items']])
        while 'LastEvaluatedKey' in response:
            if filters:
                response = table.scan(FilterExpression=reduce(And,(filters)), ExclusiveStartKey=response['LastEvaluatedKey'])
            else:
                response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            all_table_items.extend([{k: item[k] for k in item} for item in response['Items']])
            last_key = response['LastEvaluatedKey'] if 'LastEvaluatedKey' in response else None
            if len(all_table_items)>int(os.environ["MAX_RESPONSE_ITEMS"]):
                break
        return all_table_items, last_key
    except Exception as get_dynamo_db_data_exception:
        exception_line = sys_exc_info()[2].tb_lineno
        logger.error(
            "Error consultando la data de dynamo."
        )
        current_error = get_dynamo_db_data_exception
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
    version_table = dynamodb_resource.Table("dnb-rfli-data-version-intra")
    version = version_table.get_item(Key={"component": "isin-search"})
    return version["Item"]


def create_answer_request(list_item, last_key):
    try:
        version_item = get_version()
        answer = {
            "next_status": version_item["next_status"],
            "version": version_item["version"],
            "next_update":  version_item["next_update"],
            "data": list_item
        }
        return answer
    except Exception as create_answer_request_exception:
        exception_line = sys_exc_info()[2].tb_lineno
        logger.error(
            "Error generando la respuesta de la petición para el filtro del buscador."
        )
        current_error = create_answer_request_exception
        logger.error(
            current_error.__class__.__name__
            + "["
            + str(exception_line)
            + "] "
            + str(current_error)
        )
        raise 


def lambda_handler(event, context):
    entry_body = json.loads(event["body"])
    try:
        entry_body = json.loads(event["body"])
        # current_last_key = {"isin":entry_body['last_key']} if entry_body['last_key'] else {}
        # logger.info(current_last_key)
        filters = build_filter_conditions(entry_body)
        data, last_key = get_dynamo_db_data(filters)
        response = create_answer_request(data, last_key)
        return {'headers': GENERIC_HEADERS,"statusCode": 200, "body": json.dumps(response, cls=DecimalEncoder)}
    except Exception as main_exception:
        exception_line = sys_exc_info()[2].tb_lineno
        logger.error(
            f"Error general de la aplicación: No se pudo completar la solicitud de información."
        )
        current_error = main_exception
        logger.error(
            current_error.__class__.__name__
            + "["
            + str(exception_line)
            + "] "
            + str(current_error)
        )
        return {'headers': GENERIC_HEADERS,"statusCode": 500, "body": json.dumps({}, cls=DecimalEncoder)}