import datetime as dt
import pymysql.cursors
from sys import stdout as sys_stdout, exc_info as sys_exc_info, argv as sys_argv
from logging import getLogger, StreamHandler, Formatter, INFO
from dateutil import tz
from awsglue.utils import getResolvedOptions
from email_utils.email_utils import *
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
    response = ssm_client.get_parameter(Name=parameter_name, WithDecryption=True)
    logger.info("El parametro tiene el valor: " + str(response["Parameter"]["Value"]))
    return response["Parameter"]["Value"]


def get_enviroment_variable(variable):
    variable_value = getResolvedOptions(sys_argv, [variable])
    return variable_value[variable]


def send_error_mail(timeout=False):
    try:
        logger.info("Iniciando envío de mensaje.")
        report_date = get_bogota_current_time()
        subject = "[ISIN_TRACK_INTRADAY] Error intradia seguimiento de isines {}".format(
            report_date.strftime("%Y-%m-%d %H:%M:%S")
        )
        body = f"Se ha generado un error al actualizar la información de los isines. Por favor informar para su respectiva revision.\n\
            Mensaje enviado por servicio ETL Intradia a la fecha y hora {report_date.strftime('%Y-%m-%d %H:%M:%S')}"
        if timeout is True:
            subject = "[ISIN_TRACK_EOD] TIMEOUT_ERROR intradia seguimiento de isines {}".format(
                report_date.strftime("%Y-%m-%d %H:%M:%S")
            )
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


class IntradayIsinTrack:
    def __init__(self):
        self.query_timeout = int(get_enviroment_variable("QUERY_TIMEOUT"))
        self.full_valuation_date = get_bogota_current_time()
        self.yesterday_full_valuation_date = self.full_valuation_date - dt.timedelta(
            days=1
        )
        self.get_all_isines_today_query = f"SELECT {self.create_string_query(self.query_timeout)} \
                    today_prices.isin_code,\
                    today_prices.instrument,\
                    today_prices.yield,\
                    today_prices.equivalent_margin,\
                    round(today_prices.margin_value, 4) as margin,\
                    today_prices.spread,\
                    today_prices.mean_price,\
                    today_prices.clean_price,\
                    cast(today_prices.issue_date As CHAR) AS issue_date,\
                    cast(today_prices.maturity_date As CHAR) AS maturity_date,\
                    today_prices.category_id,\
                    yesterday_prices.yesterday_yield,\
                    yesterday_prices.yesterday_mean_price,\
                    ((today_prices.yield - yesterday_prices.yesterday_yield)*100) AS pbs_change\
                FROM precia_published.pub_rfl_prices AS today_prices\
                    LEFT JOIN (\
                        SELECT isin_code AS yesterday_isin_code,\
                            yield as yesterday_yield,\
                            mean_price as yesterday_mean_price\
                        FROM precia_published.pub_rfl_prices\
                        WHERE valuation_date = '{self.yesterday_full_valuation_date.date().strftime('%Y-%m-%d')}'\
                            AND isin_code != ''\
                            AND instrument NOT IN('TIDISDVL', 'CERTS')\
                    ) AS yesterday_prices ON today_prices.isin_code = yesterday_prices.yesterday_isin_code\
                WHERE today_prices.valuation_date = '{self.full_valuation_date.date().strftime('%Y-%m-%d')}'\
                    AND today_prices.isin_code != ''\
                    AND today_prices.instrument NOT IN('TIDISDVL', 'CERTS');"
        self.get_all_categories_query = f"SELECT {self.create_string_query(self.query_timeout)} \
                    isin_code,\
                    CASE\
                        WHEN margin_origin = 'CAT' THEN 'Categoria'\
                        WHEN margin_origin = 'IND' THEN 'Individual'\
                        ELSE 'NA'\
                    END AS margin_origin,\
                    CASE\
                        WHEN margin_type = 'C' THEN 'Calculado'\
                        WHEN margin_type = 'H' THEN 'Historico'\
                        WHEN margin_type = 'A' THEN 'Actualizado'\
                        ELSE 'NA'\
                    END AS margin_type,\
                    get_category.issuer AS instrument_issuer,\
                    issuer_information.name AS issuer_name,\
                    cc_curve\
                FROM precia_process.prc_rfl_get_category AS get_category\
                    LEFT JOIN precia_sources.src_rfl_issuer AS issuer_information ON get_category.issuer = issuer_information.issuer\
                WHERE category_date = '{self.full_valuation_date.date().strftime('%Y-%m-%d')}'\
                    AND instrument NOT IN('TIDISDVL', 'CERTS')\
                    AND isin_code != '';"
        self.get_folios_isines_query = f"SELECT {self.create_string_query(self.query_timeout)} prc_rfl_operations.amount,\
                cast(prc_rfl_operations.maturity_date as CHAR) AS maturity_date,\
                curve_instrument.instrument,\
                prc_rfl_operations.folio,\
                prc_rfl_operations.maturity_days AS maturity_days,\
                prc_rfl_operations.yield,\
                prc_rfl_operations.category_id\
            FROM (\
                    (\
                        SELECT operation_date,\
                            instrument\
                        FROM precia_process.prc_rfl_basket_tes\
                        WHERE operation_date = '{self.full_valuation_date.date().strftime('%Y-%m-%d')}'\
                    )\
                    UNION\
                    (\
                        SELECT operation_date,\
                            instrument\
                        FROM precia_process.prc_rfl_corporative_basket\
                        WHERE operation_date = '{self.full_valuation_date.date().strftime('%Y-%m-%d')}'\
                            AND instrument REGEXP '^(?!TFIT|TUVT)'\
                    )\
                ) AS curve_instrument\
                JOIN precia_process.prc_rfl_operations ON curve_instrument.operation_date = prc_rfl_operations.operation_date\
                AND curve_instrument.instrument = prc_rfl_operations.instrument\
                AND prc_rfl_operations.num_control = 1 AND prc_rfl_operations.yield!=100;"
        self.get_categories_query = f"SELECT {self.create_string_query(self.query_timeout)} category_id,\
                rating_group\
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
        self.user_params_collection_name = "dnb-rfli-isin-track-user-params"
        self.all_isines_collection_name = "dnb-rfli-isin-track-all-isines"
        self.track_folios_collection_name = "dnb-rfli-isin-track-folios"
        self.user_isines_collection_name = "dnb-rfli-isin-track-user-isines"
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
            logger.info("Iniciando proceso de ISIN-TACK-INTRADIA.")
            (
                origin_data_isines,
                origin_data_folios,
                eod_isines
            ) = self.get_origin_data()
            logger.info("Iniciando lectura de usuarios disponibles.")
            user_isines_params = self.read_all_user_params()
            isines_dictionary = {}
            folios_dictionary = {}
            for i in range(len(origin_data_isines)):
                isines_dictionary[
                    origin_data_isines[i]["isin_code"]
                ] = origin_data_isines[i].copy()
            for i in range(len(origin_data_folios)):
                current_category_id = origin_data_folios[i]["category_id"]
                if current_category_id not in folios_dictionary:
                    folios_dictionary[current_category_id] = []
                if origin_data_folios[i]["yield"]!=100:
                    folios_dictionary[current_category_id].append(
                        origin_data_folios[i].copy()
                    )
            del origin_data_isines
            del origin_data_folios
            (
                transformed_all_isines,
                transformed_folios,
                transformed_user_isines,
            ) = self.transform_isines_folios_data(
                isines_dictionary,
                folios_dictionary,
                user_isines_params,
                eod_isines
            )
            del user_isines_params
            logger.info(f"Insertando datos de isines. {'Sin embargo, No hay información de isines para este momento. Insercion vacia.' if not transformed_all_isines else ''}")
            self.save_data_into_dynamo(
                self.all_isines_collection_name, transformed_all_isines
            )
            logger.info(f"Insertando datos de folios. {'Sin embargo, No hay información de folios para este momento. Incersion vacia.' if not transformed_folios else ''}")
            self.save_data_into_dynamo(
                self.track_folios_collection_name, transformed_folios
            )
            logger.info(f"Insertando datos de isines en los usuarios. {'Sin embargo, no hay información para los usuarios. Incersion vacia.' if not transformed_user_isines else ''}")
            self.save_data_into_dynamo(
                self.user_isines_collection_name, transformed_user_isines
            )
            self.update_last_version_info()
        except Exception as intraday_compare_curves_exception:
            exception_line = sys_exc_info()[2].tb_lineno
            logger.error("Ocurrió un error inseperado en la actualización de la data.")
            current_error = intraday_compare_curves_exception
            logger.error(
                current_error.__class__.__name__
                + "["
                + str(exception_line)
                + "] "
                + str(current_error)
            )
            send_error_mail()

    def merge_all_isines_data(self, today_isines, today_category, categories):
        try:
            logger.info(
                        "Iniciando proceso de merge entre isines y categorias."
                    )
            today_category_temp_dictionary = {x["isin_code"]: x for x in today_category}
            categories_dictionary = {x["category_id"]:x["rating_group"] for x in categories}
            isin_keys_today = set([x["isin_code"] for x in today_isines])
            isin_keys_categories = set(today_category_temp_dictionary.keys())
            end_of_day_isines = list(isin_keys_categories-isin_keys_today)
            result_list = []
            for current_isin in today_isines:
                current_isin_code = current_isin["isin_code"]
                if (current_isin_code in today_category_temp_dictionary
                    and current_isin_code not in end_of_day_isines):
                    
                    pbs_change = current_isin["pbs_change"]
                    new_isin_object = {
                        "isin_code": current_isin_code,
                        "instrument": current_isin["instrument"],
                        "yesterday_yield": current_isin["yesterday_yield"],
                        "instrument_issuer": today_category_temp_dictionary[current_isin_code][
                            "instrument_issuer"
                        ],
                        "issuer_name":today_category_temp_dictionary[current_isin_code][
                            "issuer_name"
                        ],
                        "yield": current_isin["yield"],
                        "pbs_change": pbs_change,
                        "margin_origin": today_category_temp_dictionary[current_isin_code][
                            "margin_origin"
                        ],
                        "margin_type": today_category_temp_dictionary[current_isin_code][
                            "margin_type"
                        ],
                        "category_id": current_isin["category_id"],
                        "equivalent_margin": current_isin["equivalent_margin"],
                        "margin": current_isin["margin"],
                        "spread": current_isin["spread"],
                        "cc_curve": today_category_temp_dictionary.get(current_isin_code)[
                            "cc_curve"
                        ],
                        "mean_price": current_isin["mean_price"],
                        "clean_price": current_isin["clean_price"],
                        "yesterday_mean_price": current_isin["yesterday_mean_price"],
                        "issue_date": current_isin["issue_date"],
                        "maturity_date": current_isin["maturity_date"],
                        "rating": self.category_definition.get(
                            int(categories_dictionary.get(current_isin["category_id"]))
                            ) if categories_dictionary.get(current_isin["category_id"]) else 'NA'
                    }
                result_list.append(new_isin_object)
            logger.info(
                        f"Se juntaron un total de {len(result_list)} isines."
                    )
            return result_list, end_of_day_isines
        except Exception as merge_all_isines_exception:
            exception_line = sys_exc_info()[2].tb_lineno
            logger.error(
                "Error formateando los datos para isin_track"
            )
            current_error = merge_all_isines_exception
            logger.error(
                current_error.__class__.__name__
                + "["
                + str(exception_line)
                + "] "
                + str(current_error)
            )
            raise
        

    def get_origin_data(self):
        logger.info("Iniciando carga de información desde BASE DE DATOS SQL.")
        isines = []
        folios = []
        connection_credentials = get_secret(get_enviroment_variable("FLASH_ORIGIN_DB"))
        try:
            with pymysql.connect(
                host=connection_credentials["host"],
                port=int(connection_credentials["port"]),
                user=connection_credentials["username"],
                password=connection_credentials["password"],
                cursorclass=pymysql.cursors.DictCursor,
            ) as connection:
                with connection.cursor() as cursor_connection:
                    try:
                        logger.info(
                            "Consultando información de todos los isines para hoy."
                        )
                        cursor_connection.execute(self.get_all_isines_today_query)
                        if cursor_connection.rowcount > 0:
                            today_isines = cursor_connection.fetchall()
                            logger.info(
                                "Consultando información de la categoria para hoy."
                            )
                            cursor_connection.execute(self.get_all_categories_query)
                            today_category = cursor_connection.fetchall()
                            logger.info(f"Total de isines categorizados para hoy: {len(today_category)}")
                            logger.info(
                                "Consultando información de las categorias."
                            )
                            cursor_connection.execute(self.get_categories_query)
                            all_categories = cursor_connection.fetchall()
                            isines, eod_isines = self.merge_all_isines_data(
                                today_isines, today_category, categories=all_categories
                            )
                            del today_category, today_isines, all_categories
                            logger.info(
                                "Consultando información de los folios asociados a isines"
                            )
                            cursor_connection.execute(self.get_folios_isines_query)
                            if cursor_connection.rowcount > 0:
                                folios = cursor_connection.fetchall()
                            else:
                                logger.error(
                                    "No se encontró información de los folios."
                                )
                        else:
                            logger.error("No se encontró información de los isines.")
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
                "No se pudo cargar la información desde el origen correctamente."
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
        else:
            logger.info("Se terminó el proceso de carga de información.")
        return isines, folios, eod_isines

    def read_all_user_params(self):
        user_params_table = self.dynamodb_session.Table(
            self.user_params_collection_name
        )
        response = user_params_table.scan()
        if "Items" in response:
            return response["Items"]
        return []

    def save_data_into_dynamo(self, collection_name, data):
        try:
            if len(data) > 0:
                table = self.dynamodb_session.Table(collection_name)
                with table.batch_writer() as batch:
                    for item in data:
                        batch.put_item(Item=item)
            else:
                logger.info("No hay data para insertar en la tabla " + collection_name)
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
        response = table.get_item(Key={"component": "isin-track"})
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
                "No se encontró información de versión para 'isin-track'. No se cambia el versionado."
            )

    def transform_isines_folios_data(
        self,
        origin_data_isines,
        origin_data_folios,
        user_isines_params,
        eod_isines
    ):
        try:
            final_isines_data = []
            final_folios_data = []
            final_users_isines = []
            calculated_isines_number = 0
            updated_isines_number = 0
            historical_isines_number = 0
            logger.info("Iniciando transformación de datos")
            logger.info(f"Se tienen en total {len(origin_data_isines.keys())} isines.")
            logger.info(f"Se tienen en total {len(origin_data_folios.keys())} folios.")
            logger.info(f"Se tienen en total {len(user_isines_params)} usuarios.")
            logger.info(f"Formateando isines y folios.")
            for isin in origin_data_isines.keys():
                temp_isin_dictionary = {}
                temp_isin_dictionary["isin"] = isin
                temp_isin_dictionary["data"] = origin_data_isines[isin].copy()
                del temp_isin_dictionary["data"]["isin_code"]
                temp_isin_dictionary["folios"] = origin_data_folios.get(
                    origin_data_isines[isin]["category_id"], []
                )
                final_isines_data.append(
                    {
                        "isin": temp_isin_dictionary["isin"],
                        "data": temp_isin_dictionary["data"].copy() if isin not in eod_isines else None,
                    }
                )
                if len(temp_isin_dictionary["folios"]) > 0:
                    if temp_isin_dictionary["data"]["margin_type"] == "Calculado":
                        calculated_isines_number += 1
                    elif temp_isin_dictionary["data"]["margin_type"] == "Historico":
                        historical_isines_number += 1
                    elif temp_isin_dictionary["data"]["margin_type"] == "Actualizado":
                        updated_isines_number += 1
                    final_folios_data.append(
                        {
                            "isin": temp_isin_dictionary["isin"],
                            "folios": temp_isin_dictionary["folios"].copy(),
                        }
                    )
            logger.info(
                f"Se encontraron {updated_isines_number} isines de tipo Actualizado con folios."
            )
            logger.info(
                f"Se encontraron {historical_isines_number} isines de tipo Historico con folios."
            )
            logger.info(
                f"Se encontraron {calculated_isines_number} isines de tipo Calculado con folios."
            )
            logger.info(f"Formateando isines para los usuarios.")
            for i in range(len(user_isines_params)):
                current_user_isines = []
                for j in range(len(list(user_isines_params[i]["isines"]))):
                    current_isin = list(user_isines_params[i]["isines"])[j]
                    if list(user_isines_params[i]["isines"])[j] in origin_data_isines.keys():
                        if current_isin not in eod_isines:
                            current_user_isines.append({
                                "isin":current_isin,
                                "data":origin_data_isines[current_isin].copy()
                            }
                            )
                            del current_user_isines[-1]['data']["isin_code"]
                        else:
                            current_user_isines.append({
                                "isin":current_isin,
                                "data":{}
                            }
                            )
                final_users_isines.append(
                    {
                        "user_id": user_isines_params[i]["user_id"],
                        "isines": current_user_isines.copy(),
                    }
                )
        except Exception as save_data_into_dynamo_exception:
            exception_line = sys_exc_info()[2].tb_lineno
            logger.error("Error formateando los datos.")
            current_error = save_data_into_dynamo_exception
            logger.error(
                current_error.__class__.__name__
                + "["
                + str(exception_line)
                + "] "
                + str(current_error)
            )
        return final_isines_data, final_folios_data, final_users_isines


if __name__ == "__main__":
    IntradayIsinTrack().run()
