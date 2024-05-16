import datetime as dt
import pymysql.cursors
from sys import stdout as sys_stdout, exc_info as sys_exc_info, argv as sys_argv
from logging import getLogger, StreamHandler, Formatter, INFO
from dateutil import tz
from awsglue.utils import getResolvedOptions
from json import loads as json_loads
from boto3 import client as bt3_client, resource as bt3_resource
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
        secrets_manager_client = bt3_client("secretsmanager")
        get_secret_value_response = secrets_manager_client.get_secret_value(
            SecretId=secret_name
        )
        secret = get_secret_value_response["SecretString"]
        parameters = json_loads(secret)
    except Exception as sec_exc:
        error_msg = "No se pudo obtener el secreto " + secret_name
        logger.error(error_msg)
        logger.error(sec_exc)
    return parameters


def get_parameter_store(parameter_name):
    logger.info("Intentando leer el parametro: " + parameter_name)
    ssm_client = bt3_client("ssm")
    response = ssm_client.get_parameter(
        Name=parameter_name, WithDecryption=True)
    logger.info("El parametro tiene el valor: " +
                str(response["Parameter"]["Value"]))
    return response["Parameter"]["Value"]


def get_enviroment_variable(variable):
    variable_value = getResolvedOptions(sys_argv, [variable])
    return variable_value[variable]


def send_error_mail(timeout=False):
    try:
        logger.info("Iniciando envío de mensaje.")
        report_date = get_bogota_current_time()
        subject = f"[ISIN_SEARCH_INTRADAY] Error intradia buscador de isines {report_date.strftime('%Y-%m-%d %H:%M:%S')}"
        body = f"Se ha generado un error al actualizar la información de las categorias en el mapa de calor. Por favor informar para su respectiva revision.\n\
            Mensaje enviado por servicio ETL Intradia a la fecha y hora {report_date.strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        if timeout is True:
            subject = f"[ISIN_SEARCH_INTRADAY] TIMEOUT_ERROR intradia buscador de isines {report_date.strftime('%Y-%m-%d %H:%M:%S')}"
            body = f"Se ha generado un error al actualizar la información de los isines. El error ha sido generado\
                por un exceso en el tiempo de ejecución en la base de datos origen. Las consultas se han frenado para\
                impedir otros errores operativos. \nPor favor informar para su respectiva revision.\n\
            Mensaje enviado por servicio ETL Intradia a la fecha y hora {report_date.strftime('%Y-%m-%d %H:%M:%S')}"
        email = ReportEmail(subject, body)
        smtp_connection = email.connect_to_smtp(
            get_secret(get_enviroment_variable("SMTP_CREDENTIALS"))
        )
        message = email.create_mail_base(
            get_enviroment_variable("ORIGIN_MAIL"),
            get_enviroment_variable("DESTINATION_MAIL"),
        )
        email.send_email(smtp_connection, message)
    except Exception as send_final_mail_error:
        exception_line = sys_exc_info()[2].tb_lineno
        current_error = send_final_mail_error
        logger.error(
            current_error.__class__.__name__
            + "["
            + str(exception_line)
            + "] "
            + str(current_error)
        )


def get_seconds_from_ssm(hour_to_parse):
    return (int(hour_to_parse / 100) * 3600) + (int(hour_to_parse % 100) * 60)


def get_bogota_current_time() -> dt.datetime:
    try:
        logger.info("Configurando la hora Colombia.")
        bog_time_today = dt.datetime.now(tz=tz.gettz("America/Bogota")).replace(
            tzinfo=None
        )
        logger.info(
            "La fecha y hora actual de bogotá es -> "
            + bog_time_today.strftime("%Y-%m-%d %H:%M:%S")
        )
        return bog_time_today
    except Exception as set_diff_time_error:
        exception_line = sys_exc_info()[2].tb_lineno
        current_error = set_diff_time_error
        logger.error(
            current_error.__class__.__name__
            + "["
            + str(exception_line)
            + "] "
            + str(current_error)
        )


class LookUp:
    def __init__(self):
        self.query_timeout = int(get_enviroment_variable("QUERY_TIMEOUT"))
        entry_valuation_date = str(get_enviroment_variable("VALUATION_DATE"))
        self.full_valuation_date = dt.datetime.strptime(entry_valuation_date,'%Y-%m-%d')
        self.yesterday_full_valuation_date = self.full_valuation_date - dt.timedelta(
            days=1
        )
        self.final_eod_time = int(
            get_parameter_store(get_enviroment_variable("FINAL_EOD_TIME"))
        )
        self.pre_eod_time = int(get_parameter_store(get_enviroment_variable("PRE_EOD_TIME")))
        self.body = f"Se ha generado un error al limpiar las tablas para isin_track. \
            Por favor informar para su respectiva revision.\n\
            Mensaje enviado por servicio ETL Intradia a la fecha y hora"

        self.get_data_intradia = f"SELECT {self.create_string_query(self.query_timeout)} \
                    today_prices.isin_code as isin,\
                    today_prices.instrument as nemo,\
                    cast(today_prices.issue_date As CHAR) AS issue_date,\
                    cast(today_prices.maturity_date As CHAR) AS maturity_date,\
                    today_prices.maturity_days,\
                    today_prices.margin_value as margin,\
                    IF(today_prices.rate_type='FS', 'NA', today_prices.equivalent_margin) AS equivalent_margin,\
                    today_prices.mean_price,\
                    today_prices.clean_price,\
                    today_prices.accrued_interest,\
                    today_prices.convexity,\
                    today_prices.duration,\
                    today_prices.modified_duration,\
                    today_prices.rate_type,\
                    today_prices.category_id as category_id,\
                    today_prices.currency_type,\
                    today_prices.yield,\
                    COALESCE(today_prices.real_rating, yesterday_prices.yesterday_real_rating, 'NA') AS real_rating\
                    FROM \
                        precia_published.pub_rfl_prices AS today_prices\
                    LEFT JOIN (\
                        SELECT isin_code AS yesterday_isin_code,\
                            real_rating AS yesterday_real_rating\
                        FROM precia_published.pub_rfl_prices\
                        WHERE valuation_date = '{self.yesterday_full_valuation_date.date().strftime('%Y-%m-%d')}'\
                            AND isin_code != ''\
                            AND instrument NOT IN('TIDISDVL', 'CERTS')\
                    ) AS yesterday_prices ON today_prices.isin_code = yesterday_prices.yesterday_isin_code\
                WHERE today_prices.valuation_date = '{self.full_valuation_date.date().strftime('%Y-%m-%d')}'\
                    AND today_prices.isin_code != ''\
                    AND today_prices.instrument NOT IN('TIDISDVL', 'CERTS');"
        self.get_issuer_info = f"SELECT {self.create_string_query(self.query_timeout)} \
                        DISTINCT instrument_table.isin_code AS isin_code,\
                        COALESCE(issuer_table.name, 'NA') AS issuer_name\
                    FROM\
                        precia_sources.src_rfl_instrument AS instrument_table\
                        LEFT JOIN\
                        precia_sources.src_rfl_issuer AS issuer_table\
                        ON instrument_table.issuer=issuer_table.issuer;"
        self.get_categories_query = f"SELECT {self.create_string_query(self.query_timeout)} \
                    CONVERT(class,DECIMAL(3,0)) AS category_class,\
                    category_id,\
                    rating_group,\
                    maturity_range\
                FROM precia_sources.src_rfl_category;"
        self.category_definition = {
            10:"Sin calificación",
            20:"E",
            30:"BB+,BB,BB-",
            31:"B+,B,B-",
            32:"CCC,CC,C", 
            33:"D",
            40:"BBB+,BBB,BBB-",
            50:"A+,A,A-",
            58:"AA-",
            59:"AA",
            60:"AA+",
            70:"AAA",
            80:"Fogafin",
            90:"Nación",
            100:"Multilaterales"
        }
        self.version_collection_name = "dnb-rfli-data-version-intra"
        self.all_isines_isin_search_collection = "dnb-rfli-isin-search-all-isines"
        self.dynamodb_session = bt3_resource("dynamodb")
        self.isin_search_params = json_loads(json_loads(
            get_parameter_store(get_enviroment_variable("ISIN_SEARCH_PARAMS"))
        ))
        
        
    def transform_isines_result(self, isines, categories, issuers):
        rating_group_dictionary = {x["category_id"]:x["rating_group"] for x in categories}
        maturity_range_dictionary = {x["category_id"]:x["maturity_range"] for x in categories}
        class_dictionary = {x["category_id"]:x["category_class"] for x in categories}
        issuers_dictionary = {x["isin_code"]:x["issuer_name"] for x in issuers}
        category_class_definition = self.isin_search_params["PARAMETERS_CLASS"]
        for i in range(len(isines)):
            current_category_id = isines[i]["category_id"]
            current_isin_code = isines[i]["isin"]
            isines[i]['real_rating'] = self.category_definition.get(
                int(rating_group_dictionary.get(current_category_id))
                ) if rating_group_dictionary.get(current_category_id) else 'NA'
            isines[i]['maturity_range'] = maturity_range_dictionary.get(current_category_id,0)
            isines[i]['issuer_name'] = issuers_dictionary.get(current_isin_code,'NA')
            isines[i]['class_name']=category_class_definition.get(str(class_dictionary.get(current_category_id)),'NA')
            del isines[i]['category_id']
        return isines
    
    
    def get_origin_data(self):
        logger.info("Iniciando carga de información.")
        isines = []
        try:
            db_credentials = get_secret(get_enviroment_variable("FLASH_ORIGIN_DB"))
            with pymysql.connect(
                host=db_credentials["host"],
                port=int(db_credentials["port"]),
                user=db_credentials["username"],
                password=db_credentials["password"],
                cursorclass=pymysql.cursors.DictCursor,
            ) as connection:
                with connection.cursor() as cursor_connection:
                    try:
                        logger.info("Consultando información de curvas.")
                        cursor_connection.execute(self.get_data_intradia)
                        if cursor_connection.rowcount > 0:
                            isines = cursor_connection.fetchall()
                            logger.info("Consultando información de las categorias.")
                            cursor_connection.execute(self.get_categories_query)
                            categories = cursor_connection.fetchall()
                            logger.info("Consultando información de los issuers.")
                            cursor_connection.execute(self.get_issuer_info)
                            issuers = cursor_connection.fetchall()
                            isines = self.transform_isines_result(isines, categories, issuers)
                        logger.info("Se terminó el proceso de carga de información.")
                        return isines
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
                        self.body = f"Se ha generado un error por TIME_OUT en la consulta SQL\
                                por favor revisar los tiempos de la misma \n"
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

    def update_last_version_info(self):
        try:
            logger.info("Iniciando actualización de versión.")
            table = self.dynamodb_session.Table("dnb-rfli-data-version-intra")
            response = table.get_item(Key={"component": "isin-search"})
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
                    "No se encontró información de versión para 'ISIN_LOOKUP'. No se cambia el versionado."
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
            raise

    def create_string_query(self, seconds):
        return f"/*+ MAX_EXECUTION_TIME({int(seconds)*1000}) */"

    def run(self):
        try:
            bogota_current_datetime = get_bogota_current_time()
            prelim_start_timestamp = int(bogota_current_datetime.replace(
                hour=0, minute=0, second=0, microsecond=0
            ).timestamp()+get_seconds_from_ssm(self.pre_eod_time))
            if bogota_current_datetime.timestamp()>=prelim_start_timestamp:
                data = self.get_origin_data()
                self.save_data_into_dynamo(self.all_isines_isin_search_collection, data)
                self.update_last_version_info()
            else:
                logger.info(f"Aun no se ha entrado en periodo preliminar. No se ejecuta el Glue.")
        except Exception as lookup_exception:
            exception_line = sys_exc_info()[2].tb_lineno
            current_error = lookup_exception
            logger.error(
                current_error.__class__.__name__
                + "["
                + str(exception_line)
                + "] "
                + str(current_error)
            )
            send_error_mail()


if __name__ == "__main__":
    running = LookUp()
    running.run()
