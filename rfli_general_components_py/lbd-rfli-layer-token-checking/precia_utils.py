import logging
import os
import boto3
import base64
import json

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_enviroment_variable(variable_key):
    """
    Obtiene la variable de entorno por medio de 'variable_key'
    :param variable_key: (string) Contiene el nombre de la variable de entorno
    :return: (string) El valor de las variables de entorno
    """
    try:
        logger.info('[utils] (get_enviroment_variable) Obtener secreto ' + variable_key)
        variable_value = os.environ[str(variable_key)]
        return variable_value
    except Exception as e:
        logger.error('[utils] (get_enviroment_variable) No se encontró las variables de entorno' + str(e))

def get_secret(secret_name):
    """
    Obtiene las credenciales que vienen del Secret Manager
    :param secret_region: (string) Nombre de la region donde se encuentran las credenciales
    :param secrete_name: (string) Nombre del secreto
    :return: (dict) Diccionario con las credenciales
    """
    session = boto3.session.Session()
    client_secrets_manager = session.client(service_name='secretsmanager', region_name="us-east-1")
    secret_data = client_secrets_manager.get_secret_value(SecretId=secret_name)
    if 'SecretString' in secret_data:
        secret_str = secret_data['SecretString']
    else:
        secret_str = base64.b64decode(secret_data['SecretBinary'])
        logger.info('[utils] (get_secret) Se obtuvo el secreto')
    return json.loads(secret_str)