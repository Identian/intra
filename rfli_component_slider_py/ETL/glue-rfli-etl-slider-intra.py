import datetime as dt
import pymysql.cursors
from sys import stdout as sys_stdout, exc_info as sys_exc_info, argv as sys_argv
from logging import getLogger, StreamHandler, Formatter, INFO
from dateutil import tz
from awsglue.utils import getResolvedOptions
from email_utils.email_utils import ReportEmail
from json import loads as json_loads
from boto3 import client as bt3_client, resource as bt3_resource


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
        subject = f"[SLIDER_INTRADAY] Error intradia Slider{report_date.strftime('%Y-%m-%d %H:%M:%S')}"
        body = f"Se ha generado un error al actualizar la información de las categorias en el mapa de calor. Por favor informar para su respectiva revision.\n\
            Mensaje enviado por servicio ETL Intradia a la fecha y hora {report_date.strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        if timeout is True:
            subject = f"[SLIDER_INTRADAY] TIMEOUT_ERROR intradia Slider {report_date.strftime('%Y-%m-%d %H:%M:%S')}"
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


class IntradaySlider:
    def __init__(self):
        self.slider_params = json_loads(
            get_parameter_store(get_enviroment_variable("SLIDER_PARAMS"))
        )
        # GENERAMOS LA LISTA DE CATEGORIAS NECESARIAS PARA LA CONFIGURACIÓN DEL QUERY
        self.integer_category_id_list = [int(category_id) 
                for category_id in self.slider_params['CORPORATIVE_BANKING_CATEGORIES'].keys()]
        self.query_timeout = int(get_enviroment_variable("QUERY_TIMEOUT"))
        self.full_valuation_date = get_bogota_current_time()
        self.yesterday_full_valuation_date = self.full_valuation_date - dt.timedelta(
            days=1
        )
        self.one_month_ago_valuation_date = self.full_valuation_date - dt.timedelta(
            days=30
        )
        self.utc_time = self.full_valuation_date + dt.timedelta(
            hours=5
        )
        self.slider_information_tes_query = f"SELECT {self.create_string_query(self.query_timeout)} top_instrument_table.instrument AS instrument,\
                prices.isin_code AS isin_code,\
                prices.yield AS yield,\
                prices.pbs_change AS pbs_change,\
                0 AS category_id\
            FROM (\
                    SELECT instrument,\
                        SUM(volume) AS volume\
                    FROM precia_process.prc_rfl_operations\
                    WHERE operation_date >= '{self.one_month_ago_valuation_date.strftime('%Y-%m-%d')}'\
                        AND operation_date <= '{self.full_valuation_date.strftime('%Y-%m-%d')}'\
                        AND (\
                            instrument LIKE 'TUVT%'\
                            OR instrument LIKE 'TFIT%'\
                        )\
                    GROUP BY instrument\
                    ORDER BY volume DESC\
                    LIMIT 6\
                ) AS top_instrument_table\
                LEFT JOIN (\
                    SELECT prices_today.instrument,\
                        prices_today.yield,\
                        COALESCE((prices_today.yield - prices_yesterday.yield),0)*100 AS pbs_change,\
                        prices_today.isin_code,\
                        prices_today.maturity_days\
                    FROM precia_published.pub_rfl_prices AS prices_today\
                        LEFT JOIN precia_published.pub_rfl_prices AS prices_yesterday ON prices_yesterday.isin_code = prices_today.isin_code\
                    WHERE prices_today.valuation_date = '{self.full_valuation_date.strftime('%Y-%m-%d')}'\
                        AND prices_yesterday.valuation_date = '{self.yesterday_full_valuation_date.strftime('%Y-%m-%d')}'\
                ) AS prices ON top_instrument_table.instrument = prices.instrument\
                ORDER BY prices.maturity_days ASC;"
        self.slider_information_corporative_query =f"SELECT {self.create_string_query(self.query_timeout)} today_prices.instrument,\
                today_prices.isin_code,\
                today_prices.yield,\
                COALESCE((today_prices.yield - yesterday_prices.yield),0)*100 AS pbs_change,\
                today_prices.category_id\
            FROM (\
                    SELECT category_id,\
                        instrument,\
                        isin_code,\
                        maturity_days,\
                        yield\
                    FROM precia_published.pub_rfl_prices\
                    WHERE valuation_date = '{self.full_valuation_date.strftime('%Y-%m-%d')}'\
                        AND category_id = %s\
                    ORDER BY maturity_days DESC\
                    LIMIT 1\
                ) AS today_prices\
                LEFT JOIN (\
                    SELECT isin_code,\
                        yield\
                    FROM precia_published.pub_rfl_prices\
                    WHERE valuation_date = '{self.yesterday_full_valuation_date.strftime('%Y-%m-%d')}'\
                ) AS yesterday_prices ON today_prices.isin_code = yesterday_prices.isin_code;"
        self.slider_collection_name = "dnb-rfli-slider"
        self.version_collection_name = "dnb-rfli-data-version-intra"
        self.market_close_time = int(
            get_parameter_store(get_enviroment_variable("MARKET_CLOSE_TIME"))
        )
        self.market_open_time = int(
            get_parameter_store(get_enviroment_variable("MARKET_OPEN_TIME"))
        )
        self.pre_eod_time = int(
            get_parameter_store(get_enviroment_variable("PRE_EOD_TIME"))
        )
        self.intra_rate_time = int(
            get_parameter_store(get_enviroment_variable("INTRA_RATE_TIME"))
        )
        self.dynamodb_session = bt3_resource("dynamodb")
        

    def create_string_query(self, seconds):
        return f"/*+ MAX_EXECUTION_TIME({seconds*1000}) */"

    def run(self):
        try:
            # EXTRACT
            origin_data_slider = self.get_origin_data()
            # TRANSFORM
            data_to_insert_slider = self.transform_data(origin_data_slider)
            # LOAD
            logger.info(f"Insertando datos para el slider. {'Sin embargo, No hay información para este momento. Insercion vacia.' if not data_to_insert_slider else ''}")
            self.save_data_into_dynamo(
                self.slider_collection_name, data_to_insert_slider
            )
            self.update_last_version_info()

        except Exception as intraday_slider_exception:
            exception_line = sys_exc_info()[2].tb_lineno
            logger.error(
                "Ocurrió un error inseperado en la ejecución del intradia para Slider"
            )
            current_error = intraday_slider_exception
            logger.error(
                current_error.__class__.__name__
                + "["
                + str(exception_line)
                + "] "
                + str(current_error)
            )
            send_error_mail()

    def get_origin_data(self):
        logger.info(
            "Iniciando carga de información para el componente 6 - slider."
        )
        slider_data = []
        try:
            connection_db_credentials = get_secret(
                get_enviroment_variable("DB_SECRET"))

            with pymysql.connect(
                host=connection_db_credentials["host"],
                port=int(connection_db_credentials["port"]),
                user=connection_db_credentials["username"],
                password=connection_db_credentials["password"],
                cursorclass=pymysql.cursors.DictCursor,
            ) as connection:
                with connection.cursor() as cursor_connection:
                    try:
                        logger.info(
                            "Iniciando proceso de extracción de información - Consulta en base de datos para componente 6 - Slider."
                        )
                        logger.info("Consultando el top de instrument.")
                        cursor_connection.execute(self.slider_information_tes_query)
                        slider_tes_data = cursor_connection.fetchall()
                        slider_data.extend(slider_tes_data)
                        for category in self.integer_category_id_list:
                            cursor_connection.execute(self.slider_information_corporative_query % str(category))
                            current_corporative_item = cursor_connection.fetchall()
                            slider_data.extend(current_corporative_item)
                        logger.info(
                            f"En total se encontraron {len(slider_data)} valores.")
                        return slider_data
                    except pymysql.err.MySQLError as MySQL_Error:
                        error_code = MySQL_Error.args[0]
                        exception_line = sys_exc_info()[2].tb_lineno
                        if error_code == 3024:
                            logger.error(
                                "Tiempo de ejeucion excedido. Se detuvo el proceso de consulta en base de datos."
                            )
                            send_error_mail(timeout=True)
                        else:
                            logger.error(
                                "No se pudo cargar la información desde el origen correctamente."
                            )
                        current_error = MySQL_Error.args[1]
                        logger.error(
                            current_error.__class__.__name__
                            + "["
                            + str(exception_line)
                            + "] "
                            + str(current_error)
                        )
                        raise
                    except Exception as get_origin_data_exception:
                        exception_line = sys_exc_info()[2].tb_lineno
                        current_error = get_origin_data_exception
                        logger.error(
                            current_error.__class__.__name__
                            + "["
                            + str(exception_line)
                            + "] "
                            + str(current_error)
                        )
                        raise
        except Exception as get_origin_data_exception:
            exception_line = sys_exc_info()[2].tb_lineno
            logger.error(
                "Error al traer datos desde origen. El proceso de consulta de información da como resultado un error."
            )
            current_error = get_origin_data_exception
            logger.error(
                current_error.__class__.__name__
                + "["
                + str(exception_line)
                + "] "
                + str(current_error)
            )
            raise

    def transform_data(self, origin_data_slider):
        final_data_to_insert_slider = []
        categories_definition = self.slider_params.get('CORPORATIVE_BANKING_CATEGORIES')
        try:
            for origin_item_slider in origin_data_slider:
                final_data_to_insert_slider.append(
                    {
                        "name":categories_definition.get(str(origin_item_slider['category_id']),origin_item_slider['instrument']),
                        "yield":origin_item_slider.get('yield',0),
                        "pbs_change":origin_item_slider.get('pbs_change',0)
                    }
                )
            slider_data_to_insert = {
                "slider_key":1,
                "data":final_data_to_insert_slider
            }
        except Exception as transform_data_exception:
            exception_line = sys_exc_info()[2].tb_lineno
            logger.error(
                "No se pudo formatear la información desde el origen correctamente."
            )
            current_error = transform_data_exception
            logger.error(
                current_error.__class__.__name__
                + "["
                + str(exception_line)
                + "] "
                + str(current_error)
            )
            raise
        return slider_data_to_insert
    
    
    def save_data_into_dynamo(self, collection_name, data):
        try:
            if len(data) > 0:
                table = self.dynamodb_session.Table(collection_name)
                with table.batch_writer() as batch:
                    batch.put_item(Item=data)
            else:
                logger.info(
                    "No hay data para insertar en la tabla " + collection_name)
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
    
    
    def calculate_last_execution(self):
        start_minutes = self.market_open_time // 100 * 60 + self.market_open_time % 100
        end_minutes = self.market_close_time // 100 * 60 + self.market_close_time % 100
        last_exe_minutes = start_minutes
        while last_exe_minutes + (int(self.intra_rate_time/60)+int(self.intra_rate_time%60)) < end_minutes:
            last_exe_minutes += (int(self.intra_rate_time/60)+int(self.intra_rate_time%60))
        last_exe_final_hour = last_exe_minutes // 60 * 100 + last_exe_minutes % 60
        logger.info(f"La hora final de ejecución es: {last_exe_final_hour}")
        return last_exe_final_hour
    
    
    def update_last_version_info(self):
        logger.info("Iniciando actualización de versión.")
        table = self.dynamodb_session.Table(self.version_collection_name)
        response = table.get_item(Key={"component": "slider"})
        current_day = self.full_valuation_date.replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        logger.info("Fecha para la versión: " + str(current_day))
        logger.info("Fecha y hora de ejecución: " + str(self.full_valuation_date))
        if "Item" in response:
            item = response["Item"]
            logger.info("Se carga la siguiente data de versión: " + str(item))
            item["version"] = int(item["version"]) + 1
            if self.full_valuation_date.timestamp() >= (
                current_day.timestamp() + get_seconds_from_ssm(self.calculate_last_execution())
            ):
                logger.info(
                    "Pasando a estado: Preliminar Fin de dia (pre_eod)")
                item["next_update"] = int(
                    current_day.timestamp()
                ) + get_seconds_from_ssm(self.pre_eod_time)
                item["next_status"] = "pre_eod"
            else:
                logger.info("Se continua en el estado de Intradia (intraday)")
                next_update = self.full_valuation_date + dt.timedelta(hours=5)
                item["next_update"] = (
                    int(next_update.timestamp()) + self.intra_rate_time
                )
                item["next_status"] = "intraday"
            logger.info(
                "Se actualiza la data de versión con la siguiente informacion: "
                + str(item)
            )
            with table.batch_writer() as batch:
                batch.put_item(Item=item)
        else:
            print(
                "No se encontró información de versión para 'slider'. No se cambia el versionado."
            )


if __name__ == "__main__":
    IntradaySlider().run()
