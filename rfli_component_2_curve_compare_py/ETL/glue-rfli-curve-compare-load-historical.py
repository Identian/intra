import datetime as dt
import pymysql.cursors
from sys import stdout as sys_stdout, exc_info as sys_exc_info, argv as sys_argv
from logging import getLogger, StreamHandler, Formatter, INFO
from awsglue.utils import getResolvedOptions
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


def get_enviroment_variable(variable):
    variable_value = getResolvedOptions(sys_argv, [variable])
    return variable_value[variable]


def get_secret(secret_name):
    parameters = {}
    try:
        secrets_manager_client = bt3_client('secretsmanager')
        get_secret_value_response = secrets_manager_client.get_secret_value(SecretId=secret_name)
        secret = get_secret_value_response['SecretString']
        parameters = json_loads(secret)
    except Exception as sec_exc:
        error_msg = "No se pudo obtener el secreto "+secret_name
        logger.error(error_msg)
        logger.error(sec_exc)
    return parameters


class IntradayCompareCurves:

    def __init__(self):
        self.query_timeout = int(get_enviroment_variable('QUERY_TIMEOUT'))
        self.full_start_date = dt.datetime.strptime(get_enviroment_variable('START_DATE'),'%Y-%m-%d')
        self.full_end_date = dt.datetime.strptime(get_enviroment_variable('END_DATE'),'%Y-%m-%d')
        self.get_curves_query = "SELECT " + self.create_string_query(self.query_timeout) + " pub_rfl_betas.cc_curve,\
            beta_0,\
            beta_1,\
            beta_2,\
            tau,\
            beta_0_r,\
            beta_1_r,\
            beta_2_r,\
            tau_r,\
            days,\
            '{valuation_date}' as curve_date\
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
                WHERE curve_date = '{valuation_date}'\
            ) AS pub_rfl_betas ON cc_curve_days.cc_curve = pub_rfl_betas.cc_curve\
            GROUP BY curve_date, pub_rfl_betas.cc_curve;"
        
        self.get_folios_query = "SELECT " + self.create_string_query(self.query_timeout) + " curve_instrument.cc_curve AS cc_curve,\
            prc_rfl_operations.amount AS amount,\
            cast(prc_rfl_operations.maturity_date as CHAR) AS maturity_date,\
            curve_instrument.instrument AS nemo,\
            prc_rfl_operations.folio AS sheet,\
            prc_rfl_operations.maturity_days AS maturity_days,\
            prc_rfl_operations.yield AS yield,\
            CAST(DATE_SUB(prc_rfl_operations.timestamp_operation, INTERVAL 5 HOUR) AS CHAR) AS timestamp_operation,\
            '{valuation_date}' as curve_date\
        FROM (\
                (\
                    SELECT operation_date,\
                        instrument,\
                        cc_curve\
                    FROM precia_process.prc_rfl_basket_tes\
                    WHERE operation_date = '{valuation_date}'\
                )\
                UNION\
                (\
                    SELECT operation_date,\
                        instrument,\
                        cc_curve\
                    FROM precia_process.prc_rfl_corporative_basket\
                    WHERE operation_date = '{valuation_date}'\
                    AND instrument REGEXP '^(?!TFIT|TUVT)'\
                )\
            ) AS curve_instrument\
            JOIN precia_process.prc_rfl_operations ON curve_instrument.operation_date = prc_rfl_operations.operation_date\
            AND curve_instrument.instrument = prc_rfl_operations.instrument\
            AND prc_rfl_operations.num_control = 1\
                AND prc_rfl_operations.yield!=100\
        ORDER BY cc_curve,\
            folio;"
        self.curves_collection_name = "dnb-rfli-curve-compare-curves-eod"
        self.folios_collection_name = "dnb-rfli-curve-compare-folios-eod"
        self.dynamodb_session = bt3_resource('dynamodb')


    def run(self):
        try:
            origin_data_curves, origin_data_folios = self.get_historical_data()
            transformed_curve_info, transformed_folios_info = self.transform_curves_folios_data(origin_data_curves, origin_data_folios)
            del origin_data_curves
            del origin_data_folios
            logger.info("Insertando curvas."+str(len(transformed_curve_info)))
            self.save_data_into_dynamo(self.curves_collection_name, transformed_curve_info)
            del transformed_curve_info
            logger.info("Insertando folios."+str(len(transformed_folios_info)))
            self.save_data_into_dynamo(self.folios_collection_name, transformed_folios_info)
        except Exception as intraday_compare_curves_exception:
            exception_line = sys_exc_info()[2].tb_lineno
            logger.error(
                "Ocurrió un error inseperado en la carga de la data.")
            current_error = intraday_compare_curves_exception
            logger.error(current_error.__class__.__name__ +
                        "[" + str(exception_line) + "] " + str(current_error))
            raise


    def create_string_query(self, seconds):
        return f"/*+ MAX_EXECUTION_TIME({seconds*1000}) */"


    def get_historical_data(self):
        logger.info("Iniciando carga de información.")
        curves = []
        folios = []
        connection_credentials = get_secret(get_enviroment_variable('FLASH_ORIGIN_DB'))
        try:
            with pymysql.connect(host=connection_credentials['host'],
                                        port=int(connection_credentials['port']),
                                        user=connection_credentials['username'],
                                        password=connection_credentials['password'],
                                        cursorclass=pymysql.cursors.DictCursor
            ) as connection:
                with connection.cursor() as cursor_connection:
                    try:
                        current_date = self.full_start_date
                        while current_date <= self.full_end_date:
                            # logger.info("Consultando información de curvas para el dia "+current_date.strftime("%Y-%m-%d"))
                            cursor_connection.execute(self.get_curves_query.format(valuation_date=current_date.strftime("%Y-%m-%d")))
                            if cursor_connection.rowcount > 0:
                                curves += cursor_connection.fetchall()
                                # logger.info("Consultando información de folios para el dia "+current_date.strftime("%Y-%m-%d"))
                                cursor_connection.execute(self.get_folios_query.format(valuation_date=current_date.strftime("%Y-%m-%d")))
                                if cursor_connection.rowcount > 0:
                                    folios += cursor_connection.fetchall()
                                else:
                                    logger.error("No se encontró información de los folios para el dia "+current_date.strftime("%Y-%m-%d"))
                            else:
                                logger.error("No se encontró información de las curvas para el dia "+current_date.strftime("%Y-%m-%d"))
                            current_date += dt.timedelta(days=1)
                        logger.info("Se terminó el proceso de carga de información.")
                        return curves, folios
                    except pymysql.err.MySQLError as MySQL_Error:
                        error_code = MySQL_Error.args[0]
                        exception_line = sys_exc_info()[2].tb_lineno
                        if error_code==3024:
                            logger.error(
                            "Tiempo de ejeucion excedido. Se detuvo el proceso de consulta en base de datos.")
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
    
    def transform_curves_folios_data(self, curves_data, folios_data):
        initial_dictionary = {}
        transformed_folios_data = []
        transformed_curves_data = []
        logger.info("Iniciando transformación de curvas.")
        if curves_data:
            ordened_curves_data = {}
            for i in range(len(curves_data)):
                if curves_data[i]['curve_date'] not in ordened_curves_data.keys():
                    ordened_curves_data[curves_data[i]['curve_date']] = []
                ordened_curves_data[curves_data[i]['curve_date']].append(curves_data[i].copy())
            for curve_date in ordened_curves_data.keys():
                ordened_curves_current_date = sorted(ordened_curves_data[curve_date],key=lambda x: x['cc_curve'])
                for i in range(len(ordened_curves_current_date)):
                    if len(ordened_curves_current_date)<5:
                        logger.info("La cantidad de curvas para el dia "+curve_date+" es menor a la esperada. Se pasa a la siguiente.")
                    else:
                        transformed_curves_data.append(
                            {
                                'valuation_date': ordened_curves_current_date[i]['curve_date'],
                                'cc_curve': ordened_curves_current_date[i]['cc_curve'],
                                'data': ordened_curves_current_date[i].copy()
                            }
                        )
                        del transformed_curves_data[-1]['data']['cc_curve']
                        del transformed_curves_data[-1]['data']['curve_date']
                        if i <= 1:
                            transformed_curves_data[-1]['data']['beta_0_r'] = ordened_curves_current_date[3]['beta_0']
                            transformed_curves_data[-1]['data']['beta_1_r'] = ordened_curves_current_date[3]['beta_1']
                            transformed_curves_data[-1]['data']['beta_2_r'] = ordened_curves_current_date[3]['beta_2']
                            transformed_curves_data[-1]['data']['tau_r'] = ordened_curves_current_date[3]['tau']
                        elif i == 2:
                            transformed_curves_data[-1]['data']['beta_0_r'] = ordened_curves_current_date[4]['beta_0']
                            transformed_curves_data[-1]['data']['beta_1_r'] = ordened_curves_current_date[4]['beta_1']
                            transformed_curves_data[-1]['data']['beta_2_r'] = ordened_curves_current_date[4]['beta_2']
                            transformed_curves_data[-1]['data']['tau_r'] = ordened_curves_current_date[4]['tau']
            print(str(transformed_curves_data))
        else:
            logger.info("No hay información de curvas que transformar.")
        logger.info("Finalizando transformación de curvas.")
        logger.info("Iniciando transformación de folios.")
        if folios_data:
            total_folios = 0
            total_dias = 0
            for i in range(len(folios_data)):
                if (str(folios_data[i]['curve_date'])+";"+str(folios_data[i]['cc_curve'])) not in initial_dictionary.keys():
                    initial_dictionary[str(folios_data[i]['curve_date'])+";"+str(folios_data[i]['cc_curve'])] = []
                initial_dictionary[str(folios_data[i]['curve_date'])+";"+str(folios_data[i]['cc_curve'])].append(
                    folios_data[i].copy()
                )
                del initial_dictionary[str(folios_data[i]['curve_date'])+";"+str(folios_data[i]['cc_curve'])][-1]['cc_curve']
                del initial_dictionary[str(folios_data[i]['curve_date'])+";"+str(folios_data[i]['cc_curve'])][-1]['curve_date']
            for key in initial_dictionary.keys():
                total_dias+=1
                total_folios+=len(initial_dictionary[key])
                transformed_folios_data.append(
                    {
                        'valuation_date': str(key).split(";")[0],
                        'cc_curve': str(key).split(";")[1]
                    }
                )
                transformed_folios_data[-1]['data'] = initial_dictionary[key].copy()
            logger.info("Total folios: "+str(total_folios))
        else:
            logger.info("No hay información de folios que transformar.")
        logger.info("Finalizando transformación de folios.")
        return transformed_curves_data, transformed_folios_data


    def save_data_into_dynamo(self,collection_name, data):
        table = self.dynamodb_session.Table(collection_name)
        with table.batch_writer() as batch:
            logger.info("Insertando "+str(len(data))+" registros.")
            for item in data:
                try:
                    batch.put_item(Item=item)
                except Exception as insert_in_destination_exception:
                    exception_line = sys_exc_info()[2].tb_lineno
                    logger.error(
                        "Error insertando el item: "+str(item))
                    current_error = insert_in_destination_exception
                    logger.error(current_error.__class__.__name__ +
                                "[" + str(exception_line) + "] " + str(current_error))
                    continue


if __name__ == "__main__":
    IntradayCompareCurves().run()