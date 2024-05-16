import datetime as dt
import pymysql.cursors
from sys import stdout as sys_stdout, exc_info as sys_exc_info, argv as sys_argv
from logging import getLogger, StreamHandler, Formatter, INFO
from dateutil import tz
from awsglue.utils import getResolvedOptions
from email_utils.email_utils import *
from json import loads as json_loads
from boto3 import client as bt3_client, resource as bt3_resource
import re
import os
import sys
import traceback
from decimal import Decimal
from functools import wraps

def debugger_wrapper(error_log):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                resultado = func(*args, **kwargs)
                return resultado
            except Exception as e:
                logger.error(f"[{func.__name__}] {error_log}, linea: {get_especific_error_line(func.__name__)}, motivo: {str(e)}")
                raise Exception(f"{error_log}")
        return wrapper
    return decorator
    
def get_especific_error_line(func_name):
    _, _, exc_tb = sys.exc_info()
    for trace in traceback.extract_tb(exc_tb):
        if func_name in trace:
            return str(trace[1])

running_context = 'dev' #'real'
use_query_time_out = False

def setup_logging():
    PRECIA_LOG_FORMAT = ("%(asctime)s [%(levelname)s] [%(filename)s](%(funcName)s): %(message)s")
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
        subject = f"[TOP_DELTA_EOD] Error intradia mapa de calor de categorias {report_date.strftime('%Y-%m-%d %H:%M:%S')}"
        body = f"Se ha generado un error al actualizar la información de las categorias en el mapa de calor. Por favor informar para su respectiva revision.\n\
            Mensaje enviado por servicio ETL Intradia a la fecha y hora {report_date.strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        if timeout is True:
            subject = f"[TOP_DELTA_EOD] TIMEOUT_ERROR intradia mapa de calor de categorias {report_date.strftime('%Y-%m-%d %H:%M:%S')}"
            body = f"Se ha generado un error al actualizar la información de los isines. El error ha sido generado\
                por un exceso en el tiempo de ejecución en la base de datos origen. Las consultas se han frenado para\
                impedir otros errores operativos. \nPor favor informar para su respectiva revision.\n\
            Mensaje enviado por servicio ETL Intradia a la fecha y hora {report_date.strftime('%Y-%m-%d %H:%M:%S')}"
        email = ReportEmail(subject, body)
        smtp_connection = email.connect_to_smtp(get_secret(get_enviroment_variable("SMTP_CREDENTIALS")))
        message = email.create_mail_base(get_enviroment_variable("ORIGIN_MAIL"),get_enviroment_variable("DESTINATION_MAIL"),)
        email.send_email(smtp_connection, message)
    except Exception as send_final_mail_error:
        exception_line = sys_exc_info()[2].tb_lineno
        current_error = send_final_mail_error
        logger.error(current_error.__class__.__name__ + "[" + str(exception_line) + "] " + str(current_error))


def get_seconds_from_ssm(hour_to_parse):
    return (int(hour_to_parse / 100) * 3600) + (int(hour_to_parse % 100) * 60)


def get_bogota_current_time() -> dt.datetime:
    try:
        logger.info("Configurando la hora Colombia.")
        bog_time_today = dt.datetime.now(tz=tz.gettz("America/Bogota")).replace(tzinfo=None)
        logger.info("La fecha y hora actual de bogotá es -> " + bog_time_today.strftime("%Y-%m-%d %H:%M:%S"))
        if 'VALUATION_DATE' in os.environ:
            return dt.datetime.strptime(get_enviroment_variable('VALUATION_DATE'),'%Y-%m-%d')
        return dt.datetime.strptime('2024-03-05', '%Y-%m-%d') if running_context == 'dev' else bog_time_today
        #return bog_time_today
        
    except Exception as set_diff_time_error:
        exception_line = sys_exc_info()[2].tb_lineno
        current_error = set_diff_time_error
        logger.error(current_error.__class__.__name__ + "[" + str(exception_line) + "] " + str(current_error))
        
        
class IntradayTopDeltaCategory:
    def __init__(self):
        self.top_category_params = json_loads(json_loads(get_parameter_store(get_enviroment_variable("TOP_CATEGORY_PARAMS"))))
        self.query_timeout = int(get_enviroment_variable("QUERY_TIMEOUT"))
        self.timeout_string = self.timeout_string_query(self.query_timeout)
        self.full_valuation_date = get_bogota_current_time()
        self.yesterday_full_valuation_date = self.full_valuation_date - dt.timedelta(days=1)
        self.full_valuation_date_str = self.full_valuation_date.strftime("%Y-%m-%d")
        self.yesterday_full_valuation_date_str = self.yesterday_full_valuation_date.strftime("%Y-%m-%d")
        self.excluded_category_classes = get_enviroment_variable("EXCLUDED _CATEGORY_ CLASSES")
        
        #Este query trae las 10 categorías con mayor cantidad de isines
        self.get_top_info_isines_query = f"""
                SELECT {self.timeout_string} 
                prices_consult.category_id,
                prices_consult.tir_variation,
                prices_consult.abs_tir_variation,
                CONVERT(category_info.class,DECIMAL(3,0)) AS category_class,
                category_info.currency_group AS currency_group,
                category_info.rate_group AS rate_group,
                category_info.rating_group AS rating_group,
                category_info.maturity_range AS maturity_range
                FROM 
                    (SELECT today_prices.category_id AS category_id, ROUND((today_prices.promedio_valor - yesterday_prices.promedio_valor) * 100, 2) AS tir_variation,
                    ROUND(ABS(today_prices.promedio_valor - yesterday_prices.promedio_valor) * 100, 2) AS abs_tir_variation,today_prices.category_volume AS category_volume
                    FROM 
                        (SELECT category_id, AVG(yield) AS promedio_valor, COUNT(isin_code) AS category_volume
                        FROM precia_published.pub_rfl_prices WHERE valuation_date = '{self.full_valuation_date_str}' GROUP BY category_id) AS today_prices
                    JOIN 
                        (SELECT category_id, AVG(yield) AS promedio_valor
                        FROM precia_published.pub_rfl_prices WHERE valuation_date = '{self.yesterday_full_valuation_date_str}' GROUP BY category_id) AS yesterday_prices 
                    ON today_prices.category_id = yesterday_prices.category_id ORDER BY category_volume DESC) AS prices_consult\
                RIGHT JOIN precia_sources.src_rfl_category AS category_info ON prices_consult.category_id = category_info.category_id\
                WHERE category_info.class NOT IN({self.excluded_category_classes}) ORDER BY prices_consult.category_volume DESC LIMIT 10;"""
                
        #este query trae los 20 con mayor movimiento tipo avg(yield)
        self.get_top_info_query = """
                SELECT {timeout_string}  
                prices_consult.category_id,
                prices_consult.tir_variation,
                prices_consult.abs_tir_variation,
                CONVERT(category_info.class,DECIMAL(3,0)) AS category_class,
                category_info.currency_group AS currency_group,
                category_info.rate_group AS rate_group,
                category_info.rating_group AS rating_group,
                category_info.maturity_range AS maturity_range
                FROM 
                    (SELECT today_prices.category_id AS category_id, 
                    ROUND((today_prices.promedio_valor - yesterday_prices.promedio_valor)*100, 2) AS tir_variation,
                    ROUND(ABS((today_prices.promedio_valor - yesterday_prices.promedio_valor)*100), 2) AS abs_tir_variation
                    FROM 
                        (SELECT category_id, AVG(yield) AS promedio_valor FROM precia_published.pub_rfl_prices
                        WHERE valuation_date = '{today_date}' GROUP BY category_id) AS today_prices
                    JOIN 
                        (SELECT category_id, AVG(yield) AS promedio_valor FROM precia_published.pub_rfl_prices
                        WHERE valuation_date = '{yesterday_date}' GROUP BY category_id) AS yesterday_prices
                    ON today_prices.category_id = yesterday_prices.category_id ORDER BY abs_tir_variation DESC) AS prices_consult
                RIGHT JOIN precia_sources.src_rfl_category AS category_info ON prices_consult.category_id = category_info.category_id
                WHERE category_info.class NOT IN({excluded_category_classes}) AND prices_consult.category_id NOT IN ({excluded_categories}) ORDER BY abs_tir_variation DESC LIMIT 20;"""
            
        self.get_categories_details_query = """
                SELECT {timeout_string} 
                category_id, 
                margin_type
                FROM precia_process.prc_rfl_category_margin WHERE margin_date = '{today_date}' AND category_id IN({included_categories});"""
                
        self.get_curves_change_max_query = """
                SELECT {timeout_string}  
                today_yield.cc_curve, 
                ROUND(AVG(today_yield.rate - yesterday_yield.rate) * 100,2) AS pbs_diff 
                FROM 
                    precia_published.pub_rfl_yield AS today_yield 
                LEFT JOIN 
                    precia_published.pub_rfl_yield AS yesterday_yield 
                ON today_yield.cc_curve=yesterday_yield.cc_curve AND today_yield.term=yesterday_yield.term 
                WHERE today_yield.rate_date = '{today_date}' AND yesterday_yield.rate_date='{yesterday_date}' GROUP BY today_yield.cc_curve;"""
                            
        self.get_curves_change_query = """
                SELECT {timeout_string} 
                today_yield.cc_curve, 
                ROUND(AVG(today_yield.rate - yesterday_yield.rate) * 100,2) AS pbs_diff 
                FROM 
                    precia_published.pub_rfl_yield AS today_yield 
                LEFT JOIN 
                    precia_published.pub_rfl_yield AS yesterday_yield 
                ON today_yield.cc_curve=yesterday_yield.cc_curve AND today_yield.term=yesterday_yield.term 
                WHERE today_yield.rate_date='{today_date}' AND yesterday_yield.rate_date='{yesterday_date}' 
                AND today_yield.term <= {value} GROUP BY today_yield.cc_curve;"""
                            
        self.get_folios_query = """ 
                SELECT {timeout_string} 
                category_id,
                instrument AS nemo,
                folio AS sheet,
                yield,
                amount,
                trading_system,
                IF(issue_date='{today_date}' AND sesion REGEXP '^([fFxG])$', 'PRIMARIO', 'SECUNDARIO') AS folio_type,
                CAST(maturity_date AS CHAR) AS maturity_date,
                CAST(DATE_SUB(timestamp_operation, INTERVAL 5 HOUR) AS CHAR) AS timestamp_operation
                FROM precia_process.prc_rfl_operations WHERE category_id IN({included_categories})  AND operation_date = '{today_date}' AND num_control = 1;"""
                    
        self.get_min_max_category_isins_query = """
                SELECT  min_results.category_id, min_results.cat_min_maturity, min_results.min_isin_code, min_results.min_instrument, min_results.min_yield, cat_max_maturity, max_isin_code, max_instrument, max_yield
                FROM (
                	SELECT original_data.category_id, cat_max_maturity, original_data.isin_code AS max_isin_code, original_data.instrument AS max_instrument, original_data.yield AS max_yield
                	FROM (
                	 	SELECT category_id, MAX(maturity_days) AS cat_max_maturity	FROM precia_published.pub_rfl_prices
                		WHERE valuation_date = '{today_date}' AND category_id IN ({included_categories}) GROUP BY category_id ORDER BY category_id) AS max_maturity
                	JOIN
                		precia_published.pub_rfl_prices AS original_data
                	ON max_maturity.category_id = original_data.category_id AND max_maturity.cat_max_maturity = original_data.maturity_days
                	WHERE valuation_date = '{today_date}' GROUP BY max_maturity.category_id ORDER BY max_maturity.category_id) AS max_results
                JOIN
                	(SELECT original_data.category_id, cat_min_maturity, original_data.isin_code AS min_isin_code, original_data.instrument AS min_instrument, original_data.yield AS min_yield
                	FROM (
                	 	SELECT category_id, MIN(maturity_days) AS cat_min_maturity	FROM precia_published.pub_rfl_prices
                		WHERE valuation_date = '{today_date}' AND category_id IN ({included_categories}) GROUP BY category_id ORDER BY category_id) AS min_maturity
                	JOIN
                		precia_published.pub_rfl_prices AS original_data
                	ON min_maturity.category_id = original_data.category_id AND min_maturity.cat_min_maturity = original_data.maturity_days
                	WHERE valuation_date = '{today_date}' GROUP BY min_maturity.category_id ORDER BY min_maturity.category_id) AS min_results
                ON max_results.category_id = min_results.category_id""" 
                
        self.get_median_isins_query = """
                SELECT {timeout_string} min_delta_result.category_id, normal_delta_result.isin_code, normal_delta_result.maturity_days, normal_delta_result.median_yield, normal_delta_result.median_instrument FROM 
                	(SELECT category_med_range.category_id, MIN(ABS(cat_isin_maturity.maturity_days - category_med_range.range_average)) AS min_median_distance FROM 
                		(SELECT using_categories.category_id, average_range.range_average from
                			(SELECT id_expiration_range, (start_range + end_range)/2 AS range_average FROM precia_sources.src_rfl_expiration_range) AS	average_range
                		JOIN
                			(SELECT category_id, maturity_range FROM precia_sources.src_rfl_category WHERE category_id IN ({included_categories})) AS using_categories
                		ON using_categories.maturity_range = average_range.id_expiration_range) AS category_med_range
                	JOIN
                		(SELECT category_id, isin_code, maturity_days FROM precia_published.pub_rfl_prices 
                		WHERE valuation_date = '{today_date}' AND category_id IN ({included_categories}) GROUP BY category_id, maturity_days) AS cat_isin_maturity
                	ON category_med_range.category_id = cat_isin_maturity.category_id GROUP BY category_med_range.category_id) AS min_delta_result
                JOIN
                	(SELECT category_med_range.category_id, cat_isin_maturity.isin_code, cat_isin_maturity.maturity_days, cat_isin_maturity.median_yield, ABS(cat_isin_maturity.maturity_days - category_med_range.range_average) AS median_distance, cat_isin_maturity.median_instrument FROM 
                		(SELECT using_categories.category_id, average_range.range_average from
                			(SELECT id_expiration_range, (start_range + end_range)/2 AS range_average FROM precia_sources.src_rfl_expiration_range) AS	average_range
                		JOIN
                			(SELECT category_id, maturity_range FROM precia_sources.src_rfl_category WHERE category_id IN ({included_categories})) AS using_categories
                		ON using_categories.maturity_range = average_range.id_expiration_range) AS category_med_range
                	JOIN
                		(SELECT category_id, isin_code, maturity_days, yield AS median_yield, instrument AS median_instrument FROM precia_published.pub_rfl_prices 
                		WHERE valuation_date = '{today_date}' AND category_id IN ({included_categories}) GROUP BY category_id, maturity_days) AS cat_isin_maturity
                	ON category_med_range.category_id = cat_isin_maturity.category_id) AS normal_delta_result
                WHERE min_delta_result.category_id = normal_delta_result.category_id AND min_delta_result.min_median_distance = normal_delta_result.median_distance"""
        
        self.yesterday_isines_query = """
                SELECT {timeout_string}
                category_id, 
                isin_code,
                instrument, 
                maturity_date, 
                yield AS yesterday_yield 
                FROM precia_published.pub_rfl_prices WHERE isin_code IN ({detailed_isin_list}) AND valuation_date = '{yesterday_date}'"""
                
        self.instruments_issuer_query = """
                SELECT {timeout_string} issuer_list.instrument, 
                precia_sources.src_rfl_issuer.name 
                FROM (SELECT instrument, issuer FROM precia_sources.src_rfl_instrument WHERE instrument IN ({found_instruments}) GROUP BY ISSUER) AS issuer_list
                JOIN 
                	precia_sources.src_rfl_issuer
                ON issuer_list.issuer = precia_sources.src_rfl_issuer.issuer"""
                
        self.category_issin_counter_query = """
                SELECT {timeout_string} category_id, COUNT(isin_code) AS isin_count from precia_published.pub_rfl_prices 
                WHERE valuation_date = "{today_date}" 
                AND category_id IN ({included_categories}) GROUP BY category_id """
                
        self.top_category_intra_collection_name = "dnb-rfli-top-delta-category"
        self.details_category_intra_collection_name = "dnb-rfli-top-delta-category-details"
        self.version_collection_name = "dnb-rfli-data-version-intra"
        self.final_eod_time = int(get_parameter_store(get_enviroment_variable("FINAL_EOD_TIME")))
        self.pre_eod_time = int(get_parameter_store(get_enviroment_variable("PRE_EOD_TIME")))
        self.dynamodb_session = bt3_resource('dynamodb')



    def create_string_query(self, seconds):
        return f"/*+ MAX_EXECUTION_TIME({seconds*1000}) */"

    def timeout_string_query(self, seconds):
        if use_query_time_out:
            return f" MAX_EXECUTION_TIME({seconds*1000}) "
        return ""

    def run(self):
        try:
            bogota_current_datetime = get_bogota_current_time()
            prelim_start_timestamp = int(bogota_current_datetime.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()+get_seconds_from_ssm(self.pre_eod_time))
            #if bogota_current_datetime.timestamp()>=prelim_start_timestamp:
                # EXTRACT
            origin_data_top_delta, curve_change_details, category_type, folios = self.get_origin_data()
            # TRANSFORM
            data_to_insert_top_delta, data_to_insert_details = self.transform_data(origin_data_top_delta, curve_change_details, category_type, folios)
            # LOAD
            self.save_data_into_dynamo(self.top_category_intra_collection_name, data_to_insert_top_delta)
            self.save_data_into_dynamo(self.details_category_intra_collection_name, data_to_insert_details)
            # UPDATE_DATA_VERSION
            self.update_last_version_info()
            #else:
            #    logger.info(f"Aun no se ha entrado en periodo preliminar. No se ejecuta el Glue.")
        except Exception as intraday_top_delta_exception:
            exception_line = sys_exc_info()[2].tb_lineno
            logger.error("Ocurrió un error inseperado en la ejecución del intradia para Top Delta.")
            current_error = intraday_top_delta_exception
            logger.error(current_error.__class__.__name__ + "[" + str(exception_line) + "] " + str(current_error))
            send_error_mail()


    def get_origin_data(self):
        logger.info("Iniciando carga de información para el componente 5 - top delta category.")
        top_delta = []
        category_type = []
        folios = []
        try:
            connection_db_credentials = get_secret(get_enviroment_variable("DB_SECRET"))
            with pymysql.connect(
                host=connection_db_credentials["host"],
                port=int(connection_db_credentials["port"]),
                user=connection_db_credentials["username"],
                password=connection_db_credentials["password"],
                cursorclass=pymysql.cursors.DictCursor) as connection:
                with connection.cursor() as cursor_connection:
                    try:
                        logger.info("Iniciando proceso de extracción de información - Consulta en base de datos para componente 5 Top Delta.")
                        
                        logger.info("Consultando el top categorias del día. Por volumne de isines")
                        logger.warning(f'[get_origin_data] Query para obtener las categorías con mayor volumen de isines:\n{self.get_top_info_isines_query}')
                        cursor_connection.execute(self.get_top_info_isines_query)
                        top_delta_isines = cursor_connection.fetchall()
                        excluded_categories = [str(top_item.get('category_id',0)) for top_item in top_delta_isines]
                        excluded_categories_full_string = ",".join(excluded_categories)
                        
                        logger.info("Consultando el top categorias del día. Por variación")
                        self.get_top_info_query = self.get_top_info_query.format(**{'timeout_string':self.timeout_string, 'today_date':self.full_valuation_date_str, 'yesterday_date':self.yesterday_full_valuation_date_str, 'excluded_category_classes':self.excluded_category_classes, 'excluded_categories': excluded_categories_full_string})
                        logger.warning(f'[get_origin_data] Query para obtener las categorías con mayor yield en promedio:\n{self.get_top_info_query}')
                        cursor_connection.execute(self.get_top_info_query)
                        top_delta = cursor_connection.fetchall()
                        
                        top_delta_isines.extend(top_delta)
                        logger.info(f"En total se encontraton {len(top_delta_isines)}")
                        category_id_list = [str(ranking_item["category_id"]) for ranking_item in top_delta_isines]
                        
                        logger.info("Consultando definición de categorias - Historicas y calculadas.")
                        category_id_list_str = ','.join(category_id_list)
                        self.get_categories_details_query = self.get_categories_details_query.format(**{'timeout_string':self.timeout_string, 'today_date':self.full_valuation_date_str, 'included_categories':category_id_list_str})
                        logger.warning(f'[get_origin_data] Query para obtener definicion de categorias:\n{self.get_categories_details_query}')
                        cursor_connection.execute(self.get_categories_details_query)
                        category_type = cursor_connection.fetchall()
                        
                        logger.info("Consultando variacion de PBS del día - curvas.")
                        ordened_curves_change_dictionary = {int(k):int(v) for k,v in sorted(self.top_category_params["MAX_MATURITY_RANGE"].items(), key=lambda item: int(item[1]))}
                        last_key = int(list(ordened_curves_change_dictionary.keys())[-1])
                        curve_change_final ={}
                        for k,v in ordened_curves_change_dictionary.items():
                            curve_change_final[k]={}
                            if k!=last_key:
                                cursor_connection.execute(self.get_curves_change_query.format(**{'timeout_string':self.timeout_string, 'today_date':self.full_valuation_date_str, 'yesterday_date':self.yesterday_full_valuation_date_str, 'value':str(v)}))
                            else:
                                cursor_connection.execute(self.get_curves_change_max_query. format(**{'timeout_string':self.timeout_string,  'today_date':self.full_valuation_date_str, 'yesterday_date':self.yesterday_full_valuation_date_str}))
                            current_curve_result = cursor_connection.fetchall()
                            if len(current_curve_result)>0:
                                for curve in current_curve_result:
                                    curve_change_final[k].update({curve["cc_curve"]:curve["pbs_diff"]})
                                    
                        logger.info("Consultando folios del día por categorias.")
                        cursor_connection.execute(self.get_folios_query.format(**{'timeout_string':self.timeout_string , 'included_categories':category_id_list_str , 'today_date':self.full_valuation_date_str})  )
                        folios = cursor_connection.fetchall()
                        
                        logger.info("Consultando isines minimos y máximos por categorias.")
                        self.get_min_max_category_isins_query = self.get_min_max_category_isins_query.format(**{'timeout_string':self.timeout_string, 'today_date': self.full_valuation_date_str, 'included_categories': category_id_list_str})
                        logger.warning(f'[get_origin_data] Query para obtener isines minimos y máximos por categoría:\n{self.get_min_max_category_isins_query}')
                        cursor_connection.execute(self.get_min_max_category_isins_query)
                        self.min_max_isin_information = cursor_connection.fetchall()
                        
                        logger.info("Consultando isines medios por categorias.")
                        self.get_median_isins_query = self.get_median_isins_query.format(**{'timeout_string':self.timeout_string, 'today_date': self.full_valuation_date_str, 'included_categories': category_id_list_str})
                        logger.warning(f'[get_origin_data] Query para obtener isines medios por categoría:\n{self.get_median_isins_query}')
                        cursor_connection.execute(self.get_median_isins_query)
                        self.median_isin_information = cursor_connection.fetchall()
                        
                        logger.info("Consultando detalles de isines para el día anterior.")
                        detailed_isin_list = set([row["min_isin_code"] for row in self.min_max_isin_information] + [row["max_isin_code"] for row in self.min_max_isin_information] + [row["isin_code"] for row in self.median_isin_information])
                        detailed_isin_list = [f'"{item}"' for item in detailed_isin_list]
                        detailed_isin_str = ','.join(detailed_isin_list)
                        self.yesterday_isines_query = self.yesterday_isines_query.format(**{'timeout_string':self.timeout_string, 'yesterday_date': self.yesterday_full_valuation_date_str, 'detailed_isin_list': detailed_isin_str})
                        logger.warning(f'[get_origin_data] Query para obtener detalles de los isines en el día anterior:\n{self.yesterday_isines_query}')
                        cursor_connection.execute(self.yesterday_isines_query)
                        self.yesterday_isin_information = cursor_connection.fetchall()
                        
                        logger.info("Consultando nombres de emisores de instrumentos.")
                        instruments_list = set([row["min_instrument"] for row in self.min_max_isin_information] + [row["max_instrument"] for row in self.min_max_isin_information] + [row["median_instrument"] for row in self.median_isin_information])
                        instruments_list = [f'"{item}"' for item in instruments_list]
                        instruments_list = ','.join(instruments_list)
                        self.instruments_issuer_query = self.instruments_issuer_query.format(**{'timeout_string':self.timeout_string, 'found_instruments': instruments_list})
                        logger.warning(f'[get_origin_data] Query para obtener los emisores de los instrumentos encontrados:\n{self.instruments_issuer_query}')
                        cursor_connection.execute(self.instruments_issuer_query)
                        self.instrument_issuer_directory = {row['instrument']:row['name'] for row in cursor_connection.fetchall()}
                        
                        logger.info("Consultando nombres de emisores de instrumentos.")
                        self.category_issin_counter_query = self.category_issin_counter_query.format(**{'timeout_string':self.timeout_string, 'included_categories': category_id_list_str, 'today_date': self.full_valuation_date_str})
                        logger.warning(f'[get_origin_data] Query para obtener la cantidad de isines por categoría:\n{self.category_issin_counter_query}')
                        cursor_connection.execute(self.category_issin_counter_query)
                        self.category_isines_count_directory = {row['category_id']:row['isin_count'] for row in cursor_connection.fetchall()}
                        
                        
                        
                        
                        return top_delta_isines, curve_change_final, category_type, folios
                        
                    except pymysql.err.MySQLError as MySQL_Error:
                        error_code = MySQL_Error.args[0]
                        exception_line = sys_exc_info()[2].tb_lineno
                        if error_code == 3024:
                            logger.error("Tiempo de ejeucion excedido. Se detuvo el proceso de consulta en base de datos.")
                            send_error_mail(timeout=True)
                        else:
                            logger.error("No se pudo cargar la información desde el origen correctamente.")
                        current_error = MySQL_Error.args[1]
                        logger.error(current_error.__class__.__name__ + "[" + str(exception_line) + "] " + str(current_error))
                        raise
                    except Exception as get_origin_data_exception:
                        exception_line = sys_exc_info()[2].tb_lineno
                        current_error = get_origin_data_exception
                        logger.error(current_error.__class__.__name__ + "[" + str(exception_line) + "] " + str(current_error))
                        raise
        except Exception as get_origin_data_exception:
            exception_line = sys_exc_info()[2].tb_lineno
            logger.error("No se pudo cargar la información desde el origen correctamente.")
            current_error = get_origin_data_exception
            logger.error(current_error.__class__.__name__ + "[" + str(exception_line) + "] " + str(current_error))
            raise


    def transform_data(self, origin_data_top_delta, curve_details, category_details, folios_details):
        logger.info("Generando diccionarios para la transformacion.")
        final_data_to_insert_top = []
        logger.info(f"Generando categorias.")
        category_details_dictionary = {x["category_id"]: x["margin_type"] for x in category_details}
        folios_details_dictionary = {}
        logger.info(f"Generando folios.")
        for folio in folios_details:
            if folio["category_id"] not in folios_details_dictionary:
                folios_details_dictionary[folio["category_id"]] = []
            folios_details_dictionary[folio["category_id"]].append(folio)
        category_class_definition = self.top_category_params["PARAMETERS_CLASS"]
        category_currency_definition = self.top_category_params["PARAMETERS_CURRENCY_GROUP"]
        category_maturity_definition = self.top_category_params["PARAMETERS_MATURITY_RANGE"]
        category_rate_definition = self.top_category_params["PARAMETERS_RATE_GROUP"]
        category_rating_definition = self.top_category_params["PARAMETERS_RATING_GROUP"]
        data_top_to_insert = []
        details_top_to_insert = []
        try:
            index = 0
            for top_item in origin_data_top_delta:
                short_isin_details, medium_isin_ref, long_isin_ref = self.organize_category_isin_details(top_item["category_id"])
                current_details = {
                    "total_isines": self.category_isines_count_directory[top_item["category_id"]],
                    "cc_curve": None,
                    "category_id": top_item["category_id"],
                    "maturity_range": None,
                    "pbs_change": None,
                    "folios": [],
                    "short_isin_ref": short_isin_details, 
                    "medium_isin_ref": medium_isin_ref, 
                    "long_isin_ref": long_isin_ref} 
                logger.info(f"En la posicion {index+1} esta el item con los siguientes datos: {top_item}")
                current_category_type = category_details_dictionary.get(top_item["category_id"])

                current_category_class = category_class_definition.get(str(top_item.get("category_class")), "UN SECTOR")
                current_currency = category_currency_definition.get(str(top_item.get("currency_group")), "UNA MONEDA")
                current_rate = category_rate_definition.get(str(top_item.get("rate_group")), "DE UNA TASA")
                current_rating = category_rating_definition.get(str(top_item.get("rating_group")), "CALIFICADA DE ALGUN MODO")
                current_maturity = category_maturity_definition.get(str(top_item.get("maturity_range")), "EN UN PLAZO ESPECIFICO")
                current_max_range = curve_details.get(top_item.get("maturity_range"), list(curve_details.items())[-1])
                if current_category_type == 'C':
                    current_details["folios"] = folios_details_dictionary.get(top_item["category_id"], [])
                else:
                    category_curve = self.find_curve_reference(f"{top_item.get('category_class')},{top_item.get('currency_group')},{top_item.get('rate_group')}")
                    if category_curve:
                        current_details["cc_curve"] = category_curve
                        current_details["maturity_range"] = current_maturity
                        current_details["pbs_change"] = current_max_range.get(category_curve)
                new_top_to_insert = {
                    "category_id": top_item["category_id"],
                    "abs_tir_variation": top_item["abs_tir_variation"],
                    "tir_variation": top_item["tir_variation"],
                    "description": f"{current_category_class} en {current_currency} {current_rate} {current_rating} {current_maturity}",
                    "class_name": f"{current_category_class}",
                    "category_type": f"{current_category_type}",
                    "ranking_index": index+1
                }
                data_top_to_insert.append(new_top_to_insert)
                details_top_to_insert.append({"ranking_index":index+1,"data":current_details})
                index += 1
            final_data_to_insert_top.append({"top_category": 1,"data": data_top_to_insert})
        except Exception as get_origin_data_exception:
            exception_line = sys_exc_info()[2].tb_lineno
            logger.error("No se pudo cargar la información desde el origen correctamente.")
            current_error = get_origin_data_exception
            logger.error(current_error.__class__.__name__ + "[" + str(exception_line) + "] " + str(current_error))
            raise
        return final_data_to_insert_top, details_top_to_insert


    def find_curve_reference(self, category_definition_tuple):
        logger.info(f"Buscando curva para la categoria con tupla: {category_definition_tuple}")
        curve_relation_regex = self.top_category_params["CURVE_RELATION"]
        for curve in curve_relation_regex.keys():
            curve_regex_result = re.fullmatch(curve_relation_regex[curve], category_definition_tuple)
            if curve_regex_result:
                return curve
        return None

    @debugger_wrapper('Error organizando detalles de isines para la categoria')
    def organize_category_isin_details(self, category_id):
        logger.info(f'Organizando la categoria: {category_id}')
        min_max_row = next(row for row in self.min_max_isin_information if row['category_id'] == category_id)
        median_row  = next(row for row in self.median_isin_information if row['category_id'] == category_id)

        min_isin_yesterday_yield = next((row['yesterday_yield'] for row in self.yesterday_isin_information if row['isin_code'] ==  min_max_row['min_isin_code']), 0)
        median_isin_yesterday_yield = next((row['yesterday_yield'] for row in self.yesterday_isin_information if row['isin_code'] ==  median_row['isin_code']), 0)
        max_isin_yesterday_yield = next((row['yesterday_yield'] for row in self.yesterday_isin_information if row['isin_code'] ==  min_max_row['max_isin_code']), 0)

        min_object = {  'isin_code': min_max_row['min_isin_code'],
                        'instrument': min_max_row['min_instrument'],
                        'maturity_days': min_max_row['cat_min_maturity'],
                        'issuer': self.instrument_issuer_directory.get(min_max_row['min_instrument'], 'No hallado'),
                        'today_yield': min_max_row['min_yield'],
                        'yesterday_yield': min_isin_yesterday_yield,
                        'variation': min_max_row['min_yield'] - min_isin_yesterday_yield}
                        
        median_object = {  'isin_code': median_row['isin_code'],
                        'instrument': median_row['median_instrument'], #falta
                        'maturity_days': median_row['maturity_days'],
                        'issuer': self.instrument_issuer_directory.get(median_row['median_instrument'], 'No hallado'),
                        'today_yield': median_row['median_yield'], #falta
                        'yesterday_yield': median_isin_yesterday_yield,
                        'variation': median_row['median_yield'] - median_isin_yesterday_yield}
                        
        max_object = {  'isin_code': min_max_row['max_isin_code'],
                        'instrument': min_max_row['max_instrument'],
                        'maturity_days': min_max_row['cat_max_maturity'],
                        'issuer': self.instrument_issuer_directory.get(min_max_row['max_instrument'], 'No hallado'), 
                        'today_yield': min_max_row['max_yield'],
                        'yesterday_yield': max_isin_yesterday_yield,
                        'variation': min_max_row['min_yield'] - max_isin_yesterday_yield}
        return min_object, median_object, max_object

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
            logger.error("Error insertando datos en dynamoDB para la tabla " + collection_name + ".")
            current_error = save_data_into_dynamo_exception
            logger.error(current_error.__class__.__name__ + "[" + str(exception_line) + "] " + str(current_error))
    
    
    def update_last_version_info(self):
        try:
            logger.info("Iniciando actualización de versión.")
            table = self.dynamodb_session.Table(self.version_collection_name)
            response = table.get_item(Key={'component': 'top-delta-category'})
            current_day = self.full_valuation_date.replace(hour=0,minute=0,second=0,microsecond=0)
            if 'Item' in response:
                item = response['Item']
                logger.info("Se carga la siguiente data de versión: "+str(item))
                item['version'] = int(item['version'])+1
                item['next_update'] = int(current_day.timestamp())+get_seconds_from_ssm(self.final_eod_time)
                item['next_status'] = "final_eod"
                logger.info("Se actualiza la data de versión con la siguiente informacion: "+str(item))
                with table.batch_writer() as batch:
                    batch.put_item(Item=item)
            else:
                print("No se encontró información de versión para 'top-delta-category'. No se cambia el versionado.")
        except Exception as update_last_version_info_exception:
            exception_line = sys_exc_info()[2].tb_lineno
            logger.error("Error al actualizar el versionado de la tabla de versiones para intradia.")
            current_error = update_last_version_info_exception
            logger.error(current_error.__class__.__name__ + "[" + str(exception_line) + "] " + str(current_error))
            raise
    
if __name__ == "__main__":
    IntradayTopDeltaCategory().run()