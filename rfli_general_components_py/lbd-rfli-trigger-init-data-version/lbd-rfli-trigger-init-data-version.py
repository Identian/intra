import os
import json
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    try:
        client = boto3.client("glue")
        init_data_glue_job_name = os.environ["INIT_DATA_JOB_NAME"]
        isin_track_init_glue_job_name = os.environ["ISIN_TRACK_INIT_JOB_NAME"]
        portfolio_track_init_glue_job_name = os.environ["PORTFOLIO_TRACK_INIT_JOB_NAME"]
        isin_search_init_glue_job_name = os.environ["ISIN_SEARCH_INIT_JOB_NAME"]
        response = client.start_job_run(JobName=init_data_glue_job_name)
        job_run_id = response["JobRunId"]
        logger.info(
            f"Se lanzó el Glue para la inicializacion de la información de versiones - Aplicación intradia con el ID {job_run_id}."
        )
        response = client.start_job_run(JobName=isin_track_init_glue_job_name)
        job_run_id = response["JobRunId"]
        logger.info(
            f"Se lanzó el Glue para la inicializacion de la información en ISIN TRACK - Aplicación intradia con el ID {job_run_id}."
        )
        response = client.start_job_run(JobName=portfolio_track_init_glue_job_name)
        job_run_id = response["JobRunId"]
        logger.info(
            f"Se lanzó el Glue para la inicializacion de la información en PORTFOLIO TRACK - Aplicación intradia con el ID {job_run_id}."
        )
        response = client.start_job_run(JobName=isin_search_init_glue_job_name)
        job_run_id = response["JobRunId"]
        logger.info(
            f"Se lanzó el Glue para la inicializacion de la información en ISIN SEARCH - Aplicación intradia con el ID {job_run_id}."
        )
        return {
            "statusCode": 200,
            "body": json.dumps("Se lanzaron el/los Glue correctamente"),
        }
    except Exception:
        logger.error(
            "Error iniciando el Glue para la inicializacion de la información de versiones - Aplicación intradia"
        )
        raise
