import datetime as dt
from dateutil import tz
from boto3 import client as bt3_client, resource as bt3_resource
from sys import stdout as sys_stdout, exc_info as sys_exc_info, argv as sys_argv
from logging import getLogger, StreamHandler, Formatter, INFO
from awsglue.utils import getResolvedOptions
from email_utils.email_utils import *
from json import loads as json_loads
import pymysql.cursors


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


def get_parameter_store(parameter_name):
    logger.info("Intentando leer el parametro: "+parameter_name)
    ssm_client = bt3_client('ssm')
    response = ssm_client.get_parameter(Name=parameter_name, WithDecryption=True)
    logger.info("El parametro tiene el valor: "+str(response['Parameter']['Value']))
    return response['Parameter']['Value']


def get_seconds_from_ssm(hour_to_parse):
    seconds = (int(hour_to_parse/100)*3600)+(int(hour_to_parse % 100)*60)
    logger.info("Se transformó la hora militar "+str(hour_to_parse)+" a segundos: "+str(seconds))
    return seconds


def get_secret(secret_name):
    parameters = {}
    try:
        logger.info("Intentando acceder al secreto: "+str(secret_name))
        secrets_manager_client = bt3_client('secretsmanager')
        get_secret_value_response = secrets_manager_client.get_secret_value(SecretId=secret_name)
        secret = get_secret_value_response['SecretString']
        parameters = json_loads(secret)
        logger.info("Secreto cargado correctamente: "+str(secret_name))
    except Exception as sec_exc:
        error_msg = "No se pudo obtener el secreto "+secret_name
        logger.error(error_msg)
        logger.error(sec_exc)
    return parameters


def get_bogota_current_time():
    try:
        logger.info('Configurando la hora Colombia.')
        bog_time_today = dt.datetime.now(
            tz=tz.gettz('America/Bogota')).replace(tzinfo=None)
        logger.info('La fecha y hora actual de bogotá es -> ' +
                    bog_time_today.strftime("%Y-%m-%d %H:%M:%S"))
        return bog_time_today
    except Exception as set_diff_time_error:
        exception_line = sys_exc_info()[2].tb_lineno
        current_error = set_diff_time_error
        logger.error(current_error.__class__.__name__ +
                     "[" + str(exception_line) + "] " + str(current_error))


def get_enviroment_variable(variable):
    variable_value = getResolvedOptions(sys_argv, [variable])
    return variable_value[variable]


def send_error_mail(timeout=False):
    try:
        logger.info("Iniciando envío de mensaje.")
        report_date = get_bogota_current_time()
        subject = "[INTRADAY_RESET_DATA_ISIN_SEARCH] Error intradia limpiando tablas Isin Search {}".format(report_date.strftime('%Y-%m-%d %H:%M:%S'))
        body = f"Se ha generado un error al limpiar las tablas para Isin Search. Por favor informar para su respectiva revision.\n\
            Mensaje enviado por servicio ETL Intradia a la fecha y hora {report_date.strftime('%Y-%m-%d %H:%M:%S')}"
        if timeout is True:
            subject = "[INTRADAY_RESET_DATA_ISIN_SEARCH] TIMEOUT_ERROR Error intradia limpiando tablas Isin Search {}".format(
            report_date.strftime('%Y-%m-%d %H:%M:%S'))
            body = f"Se ha generado un error al limpiar las tablas para Isin Search. El error ha sido generado\
                por un exceso en el tiempo de ejecución en la base de datos origen. Las consultas se han frenado para\
                impedir otros errores operativos. \nPor favor informar para su respectiva revision.\n\
            Mensaje enviado por servicio ETL Intradia a la fecha y hora {report_date.strftime('%Y-%m-%d %H:%M:%S')}"
        email = ReportEmail(subject, body)
        smtp_connection = email.connect_to_smtp(get_secret(get_enviroment_variable("SMTP_CREDENTIALS")))
        message = email.create_mail_base(get_enviroment_variable('ORIGIN_MAIL'),get_enviroment_variable('DESTINATION_MAIL'))
        email.send_email(smtp_connection, message)
    except Exception as send_final_mail_error:
        exception_line = sys_exc_info()[2].tb_lineno
        current_error = send_final_mail_error
        logger.error(current_error.__class__.__name__ +
                    "[" + str(exception_line) + "] " + str(current_error))


class InitIsinTrack:
    
    
    def __init__(self):
        self.query_timeout = int(get_enviroment_variable('QUERY_TIMEOUT'))
        self.issuer_db_collection_name = "dnb-rfli-isin-search-issuers"
        self.dynamo_db_collections_intra = [
            "dnb-rfli-isin-search-all-isines",
            "dnb-rfli-isin-search-issuers"
        ]
        self.dynamodb_session = bt3_resource('dynamodb')
        self.inactive_isines, self.issuers = self.get_init_sql_data()
        self.empty_all_isines = True if get_enviroment_variable('EMPTY_ALL_ISINES')=='YES' else False


    def create_string_query(self, seconds):
        return f"/*+ MAX_EXECUTION_TIME({seconds*1000}) */"
    
    
    def run(self):
        try:
            if self.validate_business_day():
                self.reset_versions()
                self.insert_dynamodb_data(self.issuer_db_collection_name, self.issuers)
        except Exception as run_exception:
            exception_line = sys_exc_info()[2].tb_lineno
            logger.error(
                "No se actualizaron correctamente todas las versiones. El proceso finalizó con error.")
            current_error = run_exception
            logger.error(current_error.__class__.__name__ +
                         "[" + str(exception_line) + "] " + str(current_error))
            send_error_mail()


    def insert_dynamodb_data(self, collection_name, data):
        try:
            logger.info("Comienza escritura en Dynamo")
            if len(data) > 0:
                table = self.dynamodb_session.Table(collection_name)
                with table.batch_writer() as batch:
                    for item in data:
                        batch.put_item(Item=item)
        except Exception as save_data_into_dynamo_exception:
            exception_line = sys_exc_info()[2].tb_lineno
            logger.error(
                "Error insertando datos en dynamoDB para la tabla "
                + collection_name
                + "."
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


    def get_init_sql_data(self):
        full_valuation_date = get_bogota_current_time()
        yesterday_full_valuation_date = full_valuation_date - dt.timedelta(
            days=1
        )
        get_inactive_isines_query = f"SELECT {self.create_string_query(self.query_timeout)} \
                    isin_code\
                FROM\
                    precia_published.pub_rfl_prices\
                WHERE\
                    valuation_date = STR_TO_DATE('{yesterday_full_valuation_date.date().strftime('%Y-%m-%d')}', '%Y-%m-%d')\
                    AND maturity_date <= STR_TO_DATE('{full_valuation_date.date().strftime('%Y-%m-%d')}', '%Y-%m-%d')\
                    AND isin_code != '';"
        get_issuers_query = f"SELECT {self.create_string_query(self.query_timeout)} \
                    DISTINCT COALESCE(all_issuers.name, 'NA') AS issuer\
                FROM\
                    precia_sources.src_rfl_instrument AS active_issuers\
                LEFT JOIN\
                    precia_sources.src_rfl_issuer AS all_issuers\
                ON active_issuers.issuer = all_issuers.issuer\
                WHERE\
                    active_issuers.inst_condition='A'\
                GROUP BY active_issuers.issuer;"
        try:
            with pymysql.connect(host=get_secret(get_enviroment_variable('FLASH_ORIGIN_DB'))['host'],
                                 port=int(get_secret(get_enviroment_variable(
                                     'FLASH_ORIGIN_DB'))['port']),
                                 user=get_secret(get_enviroment_variable(
                                     'FLASH_ORIGIN_DB'))['username'],
                                 password=get_secret(get_enviroment_variable(
                                     'FLASH_ORIGIN_DB'))['password'],
                                 cursorclass=pymysql.cursors.DictCursor
                                 ) as connection:
                with connection.cursor() as cursor_connection:
                    try:
                        lista_isines = []
                        issuers = []
                        logger.info("Consultando isines vencidos.")
                        cursor_connection.execute(get_inactive_isines_query)
                        if cursor_connection.rowcount > 0:
                            logger.info(f"Se encontraron {cursor_connection.rowcount} isines vencidos.")
                            isines = cursor_connection.fetchall()
                            lista_isines = [isin['isin_code'] for isin in isines]
                            logger.info(f"Retornando {len(lista_isines)} isines vencidos.")
                        logger.info("Consultando emisores activos (issuer).")
                        cursor_connection.execute(get_issuers_query)
                        if cursor_connection.rowcount > 0:
                            logger.info(f"Se encontraron {cursor_connection.rowcount} emisores activos.")
                            result_issuers = cursor_connection.fetchall()
                            issuers = [{"issuer":str(issuer["issuer"])} for issuer in result_issuers]
                            logger.info(f"Retornando {len(issuers)} issuers.")
                        return lista_isines, issuers
                    except pymysql.err.MySQLError as MySQL_Error:
                        error_code = MySQL_Error.args[0]
                        exception_line = sys_exc_info()[2].tb_lineno
                        if error_code==3024:
                            logger.error(
                            "Tiempo de ejeucion excedido. Se detuvo el proceso de consulta en base de datos.")
                            send_error_mail(timeout=True)
                        else:
                            logger.error(
                                "No se pudo cargar la información desde el origen correctamente.")
                        current_error = MySQL_Error.args[1]
                        logger.error(current_error.__class__.__name__ +
                                    "[" + str(exception_line) + "] " + str(current_error))
                        raise
                    except Exception as get_yesterday_active_isnes_exe_exception:
                        exception_line = sys_exc_info()[2].tb_lineno
                        current_error = get_yesterday_active_isnes_exe_exception
                        logger.error(current_error.__class__.__name__ +
                                     "[" + str(exception_line) + "] " + str(current_error))
                        raise
        except Exception as get_yesterday_active_isnes_exception:
            exception_line = sys_exc_info()[2].tb_lineno
            logger.error(
                "No se pudo cargar la información desde el origen correctamente.")
            current_error = get_yesterday_active_isnes_exception
            logger.error(current_error.__class__.__name__ +
                         "[" + str(exception_line) + "] " + str(current_error))
            raise
        pass


    def validate_business_day(self):
        try:
            with pymysql.connect(host=get_secret(get_enviroment_variable('FLASH_ORIGIN_DB'))['host'],
                                 port=int(get_secret(get_enviroment_variable(
                                     'FLASH_ORIGIN_DB'))['port']),
                                 user=get_secret(get_enviroment_variable(
                                     'FLASH_ORIGIN_DB'))['username'],
                                 password=get_secret(get_enviroment_variable(
                                     'FLASH_ORIGIN_DB'))['password'],
                                 cursorclass=pymysql.cursors.DictCursor
                                 ) as connection:
                with connection.cursor() as cursor_connection:
                    try:
                        logger.info("Consultando si el dia es laboral.")
                        today_date = get_bogota_current_time().strftime("%Y-%m-%d")
                        business_day_consult = f"SELECT {self.create_string_query(self.query_timeout)} business_date\
                                                FROM precia_sources.src_rfl_settdaysco\
                                                WHERE business_date='{today_date}';"
                        cursor_connection.execute(business_day_consult)
                        if cursor_connection.rowcount > 0:
                            logger.info(f"El dia {today_date} es laboral.")
                            return True
                        else:
                            logger.info(f"El dia {today_date} no es laboral.")
                            return False
                    except pymysql.err.MySQLError as MySQL_Error:
                        error_code = MySQL_Error.args[0]
                        exception_line = sys_exc_info()[2].tb_lineno
                        if error_code==3024:
                            logger.error(
                            "Tiempo de ejeucion excedido. Se detuvo el proceso de consulta en base de datos.")
                            send_error_mail(timeout=True)
                        else:
                            logger.error(
                                "No se pudo cargar la información desde el origen correctamente.")
                        current_error = MySQL_Error.args[1]
                        logger.error(current_error.__class__.__name__ +
                                    "[" + str(exception_line) + "] " + str(current_error))
                        raise
                    except Exception as insert_in_destination_exe_exception:
                        exception_line = sys_exc_info()[2].tb_lineno
                        current_error = insert_in_destination_exe_exception
                        logger.error(current_error.__class__.__name__ +
                                     "[" + str(exception_line) + "] " + str(current_error))
                        raise
        except Exception as insert_in_destination_exception:
            exception_line = sys_exc_info()[2].tb_lineno
            logger.error(
                "No se pudo cargar la información desde el origen correctamente.")
            current_error = insert_in_destination_exception
            logger.error(current_error.__class__.__name__ +
                         "[" + str(exception_line) + "] " + str(current_error))
            raise


    def empty_table_data(self, table_name):
        try:
            logger.info("Inicializando cliente Dynamo para la eliminación.")
            dynamodb_client = bt3_client('dynamodb')
            logger.info("Recolectando metadata de la tabla.")
            collection_metadata = dynamodb_client.describe_table(TableName=table_name)
            key_list = collection_metadata['Table']['KeySchema']
            primary_key = None
            sort_key = None
            logger.info("Organizando llaves para la eliminación.")
            for key in key_list:
                key_type = key['KeyType']
                key_name = key['AttributeName']
                if key_type == 'HASH':
                    primary_key = key_name
                elif key_type == 'RANGE':
                    sort_key = key_name
            if primary_key is None:
                logger.error("No se pudo encontrar llave primaria. Limpieza de tabla ",table_name," fallida.")
                return
            logger.info("Se inicia el scaneo de items y se crean las solicitudes.")
            table_to_empty = self.dynamodb_session.Table(table_name)
            all_table_items = []
            projection_definition = f"{primary_key}{f', {sort_key}'if sort_key!=None else ''}"
            response = table_to_empty.scan(ProjectionExpression=projection_definition)
            all_table_items.extend([{k: item[k] for k in item} for item in response['Items']])
            while 'LastEvaluatedKey' in response:
                response = table_to_empty.scan(ProjectionExpression=projection_definition, ExclusiveStartKey=response['LastEvaluatedKey'])
                all_table_items.extend([{k: item[k] for k in item} for item in response['Items']])
            with table_to_empty.batch_writer() as batch:
                for key in all_table_items:
                    if (table_name=='dnb-rfli-isin-search-all-isines'
                        ) and key not in self.inactive_isines and not self.empty_all_isines:
                        continue
                    batch.delete_item(Key=key)
        except Exception as insert_in_destination_exception:
            exception_line = sys_exc_info()[2].tb_lineno
            logger.error(
                "No se pudo realizar la aliminacion de datos para la tabla "+table_name)
            current_error = insert_in_destination_exception
            logger.error(current_error.__class__.__name__ +
                         "[" + str(exception_line) + "] " + str(current_error))
            raise


    def reset_versions(self):
        try:
            for table_to_drop in self.dynamo_db_collections_intra:
                self.empty_table_data(table_to_drop)
        except Exception as reset_versions_exception:
            exception_line = sys_exc_info()[2].tb_lineno
            logger.error(
                "No se pudo borrar las tablas correctamente.")
            current_error = reset_versions_exception
            logger.error(current_error.__class__.__name__ +
                            "[" + str(exception_line) + "] " + str(current_error))
            raise


if __name__ == "__main__":
    InitIsinTrack().run()