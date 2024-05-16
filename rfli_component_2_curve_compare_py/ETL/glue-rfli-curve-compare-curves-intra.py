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


def send_error_mail():
    try:
        logger.info("Iniciando envío de mensaje.")
        report_date = get_bogota_current_time()
        subject = "[COMPARE_CURVES_INTRADAY] Error intradia Curvas y Folios {}".format(
            report_date.strftime("%Y-%m-%d %H:%M:%S")
        )
        body = f"Se ha generado un error al actualizar la información de curvas y folios en intradia. Por favor informar para su respectiva revision.\n\
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


def get_bogota_current_time():
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


class IntradayCompareCurves:
    def __init__(self):
        self.full_valuation_date = get_bogota_current_time()
        self.query_timeout = int(get_enviroment_variable('QUERY_TIMEOUT'))
        self.get_curves_query = f"SELECT {self.create_string_query(self.query_timeout)} pub_rfl_betas.cc_curve,\
            beta_0,\
            beta_1,\
            beta_2,\
            tau,\
            beta_0_r,\
            beta_1_r,\
            beta_2_r,\
            tau_r,\
            days\
        FROM (\
                SELECT parameter_value AS days,\
                    CASE\
                        WHEN parameter_name = 'curve_term_cop' THEN 'CEC'\
                        WHEN parameter_name = 'curve_term_uvr' THEN 'CECUVR'\
                        WHEN parameter_name = 'BAAA2_term' THEN 'BAAA2'\
                        WHEN parameter_name = 'BAAA3_term' THEN 'BAAA3'\
                        WHEN parameter_name = 'BAAA12_term' THEN 'BAAA12'\
                    END AS cc_curve\
                FROM precia_sources.src_rfl_parameters\
                WHERE parameter_name IN (\
                        'curve_term_cop',\
                        'curve_term_uvr',\
                        'BAAA2_term',\
                        'BAAA3_term',\
                        'BAAA12_term'\
                    )\
            ) AS cc_curve_days\
            INNER JOIN (\
                SELECT pub_rfl_betas.cc_curve AS cc_curve,\
                    pub_rfl_betas.beta_0 AS beta_0,\
                    pub_rfl_betas.beta_1 AS beta_1,\
                    pub_rfl_betas.beta_2 AS beta_2,\
                    pub_rfl_betas.tao_1 AS tau,\
                    NULL AS beta_0_r,\
                    NULL AS beta_1_r,\
                    NULL AS beta_2_r,\
                    NULL AS tau_r\
                FROM precia_published.pub_rfl_betas\
                WHERE curve_date = '{self.full_valuation_date.date().strftime('%Y-%m-%d')}'\
            ) AS pub_rfl_betas ON cc_curve_days.cc_curve = pub_rfl_betas.cc_curve"
        self.get_folios_query = f"SELECT {self.create_string_query(self.query_timeout)} curve_instrument.cc_curve AS cc_curve,\
            prc_rfl_operations.amount AS amount,\
            cast(prc_rfl_operations.maturity_date as CHAR) AS maturity_date,\
            curve_instrument.instrument AS nemo,\
            prc_rfl_operations.folio AS sheet,\
            prc_rfl_operations.maturity_days AS maturity_days,\
            prc_rfl_operations.yield AS yield,\
            CAST(DATE_SUB(prc_rfl_operations.timestamp_operation, INTERVAL 5 HOUR) AS CHAR) AS timestamp_operation\
        FROM (\
                (\
                    SELECT operation_date,\
                        instrument,\
                        cc_curve\
                    FROM precia_process.prc_rfl_basket_tes\
                    WHERE operation_date = '{self.full_valuation_date.date().strftime('%Y-%m-%d')}'\
                )\
                UNION\
                (\
                    SELECT operation_date,\
                        instrument,\
                        cc_curve\
                    FROM precia_process.prc_rfl_corporative_basket\
                    WHERE operation_date = '{self.full_valuation_date.date().strftime('%Y-%m-%d')}'\
                    AND instrument REGEXP '^(?!TFIT|TUVT)'\
                )\
            ) AS curve_instrument\
            JOIN precia_process.prc_rfl_operations ON curve_instrument.operation_date = prc_rfl_operations.operation_date\
            AND curve_instrument.instrument = prc_rfl_operations.instrument\
                AND prc_rfl_operations.num_control = 1\
                AND prc_rfl_operations.yield!=100\
        ORDER BY cc_curve,\
            folio;"
        self.curves_collection_name = "dnb-rfli-curve-compare-curves-intra"
        self.folios_collection_name = "dnb-rfli-curve-compare-folios-intra"
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
            origin_data_curves, origin_data_folios = self.get_origin_data()
            (
                transformed_curve_info,
                transformed_folios_info,
            ) = self.transform_curves_folios_data(
                origin_data_curves, origin_data_folios
            )
            logger.info(f"Insertando datos de curvas. {'Sin embargo, No hay información de isines para este momento. Insercion vacia.' if not origin_data_curves else ''}")
            self.save_data_into_dynamo(
                self.curves_collection_name, transformed_curve_info
            )
            logger.info(f"Insertando datos de folios. {'Sin embargo, No hay información de isines para este momento. Insercion vacia.' if not origin_data_folios else ''}")
            self.save_data_into_dynamo(
                self.folios_collection_name, transformed_folios_info
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

    def save_data_into_dynamo(self, collection_name, data):
        try:
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

    def get_origin_data(self):
        logger.info("Iniciando carga de información.")
        curves = []
        folios = []
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
                        cursor_connection.execute(self.get_curves_query)
                        if cursor_connection.rowcount > 0:
                            curves = cursor_connection.fetchall()
                            logger.info("Consultando información de folios.")
                            cursor_connection.execute(self.get_folios_query)
                            if cursor_connection.rowcount > 0:
                                folios = cursor_connection.fetchall()
                            else:
                                logger.error(
                                    "No se encontró información de los folios."
                                )
                        else:
                            logger.error("No se encontró información de las curvas.")
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
        return curves, folios


    def consult_current_curves(self):
        curves_dynamo_table = self.dynamodb_session.Table(self.curves_collection_name)
        dynamo_response = curves_dynamo_table.scan(
            ProjectionExpression="cc_curve"
        )
        curves_list = []
        for item in dynamo_response['Items']:
            curves_list.append(item["cc_curve"])
        return curves_list
    

    def transform_curves_folios_data(self, curves_data, folios_data):
        
        initial_dictionary = {}
        transformed_folios_data = []
        transformed_curves_data = []
        curves_list = self.consult_current_curves()
        curves_with_data = []
        logger.info("Iniciando transformación de curvas.")
        if curves_data:
            for i in range(5):
                transformed_curves_data.append(
                    {
                        "cc_curve": curves_data[i]["cc_curve"],
                        "data": curves_data[i].copy(),
                    }
                )
                curves_with_data.append(curves_data[i]["cc_curve"])
                del transformed_curves_data[-1]["data"]["cc_curve"]
                if i <= 1:
                    transformed_curves_data[-1]["data"]["beta_0_r"] = curves_data[3][
                        "beta_0"
                    ]
                    transformed_curves_data[-1]["data"]["beta_1_r"] = curves_data[3][
                        "beta_1"
                    ]
                    transformed_curves_data[-1]["data"]["beta_2_r"] = curves_data[3][
                        "beta_2"
                    ]
                    transformed_curves_data[-1]["data"]["tau_r"] = curves_data[3]["tau"]
                elif i == 2:
                    transformed_curves_data[-1]["data"]["beta_0_r"] = curves_data[4][
                        "beta_0"
                    ]
                    transformed_curves_data[-1]["data"]["beta_1_r"] = curves_data[4][
                        "beta_1"
                    ]
                    transformed_curves_data[-1]["data"]["beta_2_r"] = curves_data[4][
                        "beta_2"
                    ]
                    transformed_curves_data[-1]["data"]["tau_r"] = curves_data[4]["tau"]
        else:
            logger.info("No hay información de curvas que transformar.")
        logger.info("Finalizando transformación de curvas.")
        logger.info("Iniciando transformación de folios.")
        cc_curves_in_folios = []
        if folios_data:
            for i in range(len(folios_data)):
                if folios_data[i]['cc_curve'] not in initial_dictionary.keys():
                    initial_dictionary[folios_data[i]['cc_curve']] = []
                if folios_data[i]['yield'] != 100:
                    initial_dictionary[folios_data[i]['cc_curve']].append(
                        folios_data[i].copy()
                    )
                del initial_dictionary[folios_data[i]['cc_curve']][-1]['cc_curve']
            for key in initial_dictionary.keys():
                cc_curves_in_folios.append(key)
                transformed_folios_data.append(
                    {"cc_curve": key, "data": initial_dictionary[key].copy()}
                )
        else:
            logger.info("No hay información de folios que transformar.")
        for curve in curves_list:
            if curve not in cc_curves_in_folios:
                transformed_folios_data.append(
                    {"cc_curve":curve, "data":[]}
                )
            if curve not in curves_with_data:
                transformed_curves_data.append({
                    "cc_curve": curve,
                    "data": {}
                })
        logger.info("Finalizando transformación de folios.")
        return transformed_curves_data, transformed_folios_data

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
        response = table.get_item(Key={"component": "compare-curves"})
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
                "No se encontró información de versión para 'compare-curves'. No se cambia el versionado."
            )


if __name__ == "__main__":
    IntradayCompareCurves().run()
