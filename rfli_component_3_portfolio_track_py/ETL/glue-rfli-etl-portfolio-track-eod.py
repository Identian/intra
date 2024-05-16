import datetime as dt
import pymysql.cursors
from sys import stdout as sys_stdout, exc_info as sys_exc_info, argv as sys_argv
from logging import getLogger, StreamHandler, Formatter, INFO
from dateutil import tz
from awsglue.utils import getResolvedOptions
from json import loads as json_loads
from boto3 import client as bt3_client, resource as bt3_resource
import time
from email_utils.email_utils import *


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

def get_secret(secret_name):
    parameters = {}
    try:
        secrets_manager_client = bt3_client('secretsmanager')
        get_secret_value_response = secrets_manager_client.get_secret_value(
            SecretId=secret_name)
        secret = get_secret_value_response['SecretString']
        parameters = json_loads(secret)
    except Exception as sec_exc:
        error_msg = "No se pudo obtener el secreto "+secret_name
        logger.error(error_msg)
        logger.error(sec_exc)
    return parameters

def get_parameter_store(parameter_name):
    logger.info("Intentando leer el parametro: "+parameter_name)
    ssm_client = bt3_client('ssm')
    response = ssm_client.get_parameter(
        Name=parameter_name, WithDecryption=True)
    logger.info("El parametro tiene el valor: " +
                str(response['Parameter']['Value']))
    return response['Parameter']['Value']

def get_enviroment_variable(variable):
    variable_value = getResolvedOptions(sys_argv, [variable])
    return variable_value[variable]

def send_error_mail():
    try:
        logger.info("Iniciando envío de mensaje.")
        report_date = get_bogota_current_time()
        subject = "[PORTFOLIO_TRACK_INTRADAY] Error intradia portafolio de isines {}".format(
            report_date.strftime('%Y-%m-%d %H:%M:%S'))
        body = f"Se ha generado un error al actualizar la información de los isines. Por favor informar para su respectiva revision.\n\
            Mensaje enviado por servicio ETL Intradia a la fecha y hora {report_date.strftime('%Y-%m-%d %H:%M:%S')}"
        email = ReportEmail(subject, body)
        smtp_connection = email.connect_to_smtp(
            get_secret(get_enviroment_variable("SMTP_CREDENTIALS")))
        message = email.create_mail_base(get_enviroment_variable(
            'ORIGIN_MAIL'), get_enviroment_variable('DESTINATION_MAIL'))
        email.send_email(smtp_connection, message)
    except Exception as send_final_mail_error:
        exception_line = sys_exc_info()[2].tb_lineno
        current_error = send_final_mail_error
        logger.error(current_error.__class__.__name__ +
                     "[" + str(exception_line) + "] " + str(current_error))

def get_seconds_from_ssm(hour_to_parse):
    return (int(hour_to_parse/100)*3600)+(int(hour_to_parse % 100)*60)

def get_bogota_current_time():
    try:
        logger.info('Configurando la hora Colombia.')
        bog_time_today = dt.datetime.now(tz=tz.gettz('America/Bogota')).replace(tzinfo=None)
        logger.info('La fecha y hora actual de bogotá es -> ' +
                    bog_time_today.strftime("%Y-%m-%d %H:%M:%S"))
        return bog_time_today
    except Exception as set_diff_time_error:
        exception_line = sys_exc_info()[2].tb_lineno
        current_error = set_diff_time_error
        logger.error(current_error.__class__.__name__ +
                     "[" + str(exception_line) + "] " + str(current_error))
class IntradayFolios:
    def __init__(self):
        self.query_timeout = int(get_enviroment_variable("QUERY_TIMEOUT"))
        entry_valuation_date = str(get_enviroment_variable("VALUATION_DATE"))
        self.full_valuation_date = dt.datetime.strptime(
            entry_valuation_date, "%Y-%m-%d"
        )
        self.yesterday_full_valuation_date = self.full_valuation_date - dt.timedelta(
            days=1
        )
        self.final_eod_time = int(
            get_parameter_store(get_enviroment_variable("FINAL_EOD_TIME"))
        )
        self.pre_eod_time = int(
            get_parameter_store(get_enviroment_variable("PRELIM_EOD_TIME"))
        )
        self.dynamodb_session = bt3_resource("dynamodb")
        self.user_params_collection_name = "dnb-rfli-portfolio-track-params-isines"

        self.get_data_pub_rfl_prices = f"SELECT {self.create_string_query(self.query_timeout)} rfl_prices.isin_code,\
                cast(rfl_prices.maturity_date As CHAR) AS maturity_date,\
                rfl_prices.yield,\
                rfl_prices_yesterday.yield AS 'yesterday_yield',\
                rfl_prices.clean_price,\
                rfl_prices.accrued_interest,\
                rfl_prices_yesterday.clean_price AS 'clean_price_yesterday',\
                (rfl_prices.clean_price - rfl_prices_yesterday.clean_price) * 100 AS 'difference',\
                rfl_prices.instrument,\
                cast(rfl_prices.issue_date AS CHAR) AS issue_date,\
                rfl_prices.spread,\
                rfl_prices.real_rating,\
                rfl_prices.payment_frequency,\
                rfl_prices.mean_price,\
                rfl_prices_yesterday.mean_price AS 'mean_price_yesterday',\
                ROUND(rfl_prices.margin_value, 4) AS 'margin_value',\
                rfl_prices.category_id as category_id,\
                rfl_prices.equivalent_margin\
            FROM precia_published.pub_rfl_prices rfl_prices\
            INNER JOIN precia_published.pub_rfl_prices rfl_prices_yesterday ON rfl_prices.isin_code = rfl_prices_yesterday.isin_code\
            WHERE rfl_prices.valuation_date = '{self.full_valuation_date.date()}' AND rfl_prices_yesterday.valuation_date = '{self.yesterday_full_valuation_date.date()}' AND rfl_prices.isin_code != ''\
                AND rfl_prices.instrument NOT IN('TIDISDVL','CERTS') AND rfl_prices_yesterday.maturity_date>'{self.full_valuation_date.date()}';"
        self.get_data_rfl_instrument = f"SELECT {self.create_string_query(self.query_timeout)} rfl_instrument.issuer,\
                issuer_information.name AS issuer_name,\
				rfl_instrument.cc_curve,\
				rfl_instrument.isin_code\
			FROM precia_sources.src_rfl_instrument AS rfl_instrument\
            LEFT JOIN precia_sources.src_rfl_issuer AS issuer_information \
            ON rfl_instrument.issuer = issuer_information.issuer\
			WHERE rfl_instrument.isin_code != '';"

    
    def create_string_query(self, seconds):
        return f"/*+ MAX_EXECUTION_TIME({seconds*1000}) */"
    
    
    def get_origin_data(self):
        logger.info("Iniciando carga de información.")
        isines = []
        instruments = []
        try:
            with pymysql.connect(
                host=get_secret(get_enviroment_variable("FLASH_ORIGIN_DB"))["host"],
                port=int(
                    get_secret(get_enviroment_variable("FLASH_ORIGIN_DB"))["port"]
                ),
                user=get_secret(get_enviroment_variable("FLASH_ORIGIN_DB"))["username"],
                password=get_secret(get_enviroment_variable("FLASH_ORIGIN_DB"))[
                    "password"
                ],
                cursorclass=pymysql.cursors.DictCursor,
            ) as connection:
                with connection.cursor() as cursor_connection:
                    try:
                        logger.info("Consultando información de curvas.")
                        cursor_connection.execute(self.get_data_pub_rfl_prices)
                        if cursor_connection.rowcount > 0:
                            isines = cursor_connection.fetchall()
                            logger.info(
                                "Consultando información de la categoria asociadas a isines"
                            )
                            cursor_connection.execute(self.get_data_rfl_instrument)
                            if cursor_connection.rowcount > 0:
                                instruments = cursor_connection.fetchall()
                                logger.info(
                                    "Consultando información de la categoria asociadas a instruments"
                                )
                    except Exception as insert_in_destination_exe_exception:
                        exception_line = sys_exc_info()[2].tb_lineno
                        current_error = insert_in_destination_exe_exception
                        logger.error(
                            current_error.__class__.__name__
                            + "["
                            + str(exception_line)
                            + "] "
                            + str(current_error)
                        )
                        raise
        except Exception as insert_in_destination_exception:
            exception_line = sys_exc_info()[2].tb_lineno
            logger.error(
                "No se pudo cargar la información desde el origen correctamente."
            )
            current_error = insert_in_destination_exception
            logger.error(
                current_error.__class__.__name__
                + "["
                + str(exception_line)
                + "] "
                + str(current_error)
            )
            raise
        else:
            logger.info("Se terminó el proceso de carga de información.")
        return isines, instruments

    def reformat_dictionary(self, data):
        new_dictionary = [
            {
                "isin": d["isin_code"],
                "data": {k: v for k, v in d.items() if k != "isin_code"},
            }
            for d in data
        ]
        return new_dictionary

    def update_last_version_info(self):
        try:
            logger.info("Iniciando actualización de versión.")
            table = self.dynamodb_session.Table("dnb-rfli-data-version-intra")
            response = table.get_item(Key={"component": "portfolio-track"})
            current_day = get_bogota_current_time().replace(
                hour=5, minute=0, second=0, microsecond=0
            )
            if "Item" in response:
                item = response["Item"]
                logger.info("Se carga la siguiente data de versión: " + str(item))
                item["version"] = int(item["version"]) + 1
                item["next_update"] = int(
                    current_day.timestamp()
                ) + get_seconds_from_ssm(self.final_eod_time)
                item["next_status"] = "final_eod"
                logger.info(
                    "Se actualiza la data de versión con la siguiente informacion: "
                    + str(item)
                )
                with table.batch_writer() as batch:
                    batch.put_item(Item=item)
            else:
                print(
                    "No se encontró información de versión para 'PORTFOLIO_TRACK_EOD'. No se cambia el versionado."
                )
        except Exception as update_last_version_info_exception:
            exception_line = sys_exc_info()[2].tb_lineno
            logger.error(
                "Error al actualizar el versionado de la tabla de versiones para intradia."
            )
            current_error = update_last_version_info_exception
            logger.error(
                current_error.__class__.__name__
                + "["
                + str(exception_line)
                + "] "
                + str(current_error)
            )
            raise

    def make_dictionary(self, dictionary_1, dictionary_2):
        map = {}
        for dictionary in dictionary_1:
            key = dictionary["isin_code"]
            map[key] = dictionary
        for dictionary in dictionary_2:
            key = dictionary["isin_code"]
            if key in map:
                map[key].update(dictionary)
            else:
                map[key] = dictionary
        final_dictionary = list(map.values())
        final_dictionary = self.reformat_dictionary(final_dictionary)
        return final_dictionary

    def save_data_into_dynamo(self, collection_name, data):
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

    def read_all_user_params(self):
        user_params_table = self.dynamodb_session.Table(
            self.user_params_collection_name
        )
        response = user_params_table.scan()
        if "Items" in response:
            return response["Items"]
        return []

    def make_user_dictionary(self, params, data):
        item_list = filter(lambda item: item["isin"] in params["isines"], data)
        return {"user_id": params["user_id"], "isines": list(item_list)}

    def run(self):
        try:
            bogota_current_datetime = get_bogota_current_time()
            prelim_start_timestamp = int(bogota_current_datetime.replace(
                hour=0, minute=0, second=0, microsecond=0
            ).timestamp()+get_seconds_from_ssm(self.pre_eod_time))
            if bogota_current_datetime.timestamp()>=prelim_start_timestamp:
                inicio = time.time()
                result_isin, result_instrument = self.get_origin_data()
                data = self.make_dictionary(result_isin, result_instrument)
                self.save_data_into_dynamo("dnb-rfli-portfolio-track-all-isines", data)
                user_params = self.read_all_user_params()
                all_user_list = list(
                    map(lambda object: self.make_user_dictionary(object, data), user_params)
                )
                self.save_data_into_dynamo(
                    "dnb-rfli-portfolio-track-user-isines", all_user_list
                )
                self.update_last_version_info()
                fin = time.time()
                logger.info("Tiempo transcurrido: ", fin - inicio)
            else:
                logger.info(f"Aun no se ha entrado en periodo preliminar. No se ejecuta el Glue.")
        except Exception as IntradayFolios:
            exception_line = sys_exc_info()[2].tb_lineno
            logger.error("Ocurrió un error inseperado en la actualización de la data.")
            current_error = IntradayFolios
            logger.error(
                current_error.__class__.__name__
                + "["
                + str(exception_line)
                + "] "
                + str(current_error)
            )
            send_error_mail()


if __name__ == "__main__":
    data = IntradayFolios()
    data.run()
