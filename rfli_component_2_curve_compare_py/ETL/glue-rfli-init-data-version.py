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


def send_error_mail():
    try:
        logger.info("Iniciando envío de mensaje.")
        report_date = get_bogota_current_time()
        subject = "[INTRADAY_RESET_DATA] Error intradia inicializacion de version {}".format(report_date.strftime('%Y-%m-%d %H:%M:%S'))
        body = f"Se ha generado un error al actualizar la información de las versiones para las tablas de intradia. Por favor informar para su respectiva revision.\n\
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
        

class InitDataVersion:
    def __init__(self):
        self.query_timeout = int(get_enviroment_variable('QUERY_TIMEOUT'))
        self.version_collection_name = "dnb-rfli-data-version-intra"
        self.components = [
            "compare-curves",
            "isin-track",
            "portfolio-track",
            "isin-search",
            "top-delta-category"
        ]
        self.market_open_time = int(get_parameter_store(get_enviroment_variable("MARKET_OPEN_TIME")))
        self.intra_rate_time = int(get_parameter_store(get_enviroment_variable("INTRA_RATE_TIME")))
        self.market_open_time_seconds = get_seconds_from_ssm(
            self.market_open_time)
        self.dynamodb_session = bt3_resource('dynamodb')

    def run(self):
        try:
            if self.validate_business_day():
                self.reset_versions()
        except Exception as run_exception:
            exception_line = sys_exc_info()[2].tb_lineno
            logger.error(
                "No se actualizaron correctamente todas las versiones. El proceso finalizó con error.")
            current_error = run_exception
            logger.error(current_error.__class__.__name__ +
                         "[" + str(exception_line) + "] " + str(current_error))
            send_error_mail()

    
    def create_string_query(self, seconds):
        return f"/*+ MAX_EXECUTION_TIME({seconds*1000}) */"
    
    
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
    

    def reset_versions(self):
        try:
            version_table = self.dynamodb_session.Table(
                self.version_collection_name)
            current_day = get_bogota_current_time().replace(hour=0,minute=0,second=0,microsecond=0)
            logger.info("La fecha de para el versionado es: "+str(current_day))
            logger.info("El timestamp para la fecha de hoy es: "+str(int(current_day.timestamp())))
            data_to_insert = []
            for component_name in self.components:
                response = version_table.get_item(Key={'component': component_name})
                new_data = {
                    "component": component_name,
                    "next_update": int(current_day.timestamp())+(self.market_open_time_seconds+self.intra_rate_time),
                    "version": 1,
                    "next_status": "intraday"
                }
                logger.info("La respuesta ha sido, para el componente "+component_name+" la siguiente: "+str(response))
                data_to_insert.append(new_data)
            logger.info("Preparando los datos para insertar: ")
            logger.info(str(data_to_insert))
            with version_table.batch_writer() as batch:
                for item in data_to_insert:
                    batch.put_item(Item=item)
        except Exception as reset_versions_exception:
            exception_line = sys_exc_info()[2].tb_lineno
            logger.error(
                "No se pudo actualizar correctamente la tabla de versiones, sucedió un error inesperado.")
            current_error = reset_versions_exception
            logger.error(current_error.__class__.__name__ +
                            "[" + str(exception_line) + "] " + str(current_error))
            raise


if __name__ == "__main__":
    InitDataVersion().run()
