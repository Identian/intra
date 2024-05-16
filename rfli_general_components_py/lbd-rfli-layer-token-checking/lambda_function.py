import json
import logging
import sys
import os
from precia_utils import get_enviroment_variable, get_secret

import jwt
import requests
from cryptography.x509 import load_pem_x509_certificate
from cryptography.hazmat.backends import default_backend


logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    logger.info("Iniciando lambda de validación de token.")
    lo = lambda_object(event)
    return lo.starter() 
    
class lambda_object:
    def __init__(self, event):
        try:
            self.request_from_cloudfront = False
            self.request_authorized = False
            self.failed_init = False
            self.final_response = {"principalId": "user",
                                      "policyDocument": {
                                        "Version": "2012-10-17",
                                        "Statement": [
                                          {
                                            "Action": "execute-api:Invoke",
                                            "Effect": "Deny",
                                            "Resource": str()}]}}
            self.detailed_raise = ''
            self.partial_response = {}
            logger.warning(f'event de entrada: {str(event)}')
            self.token = event['authorizationToken']
            self.final_response['policyDocument']['Statement'][0]['Resource'] = event['methodArn']
        except Exception as init_error:
            exception_line = sys.exc_info()[2].tb_lineno
            current_error = init_error
            logger.error(current_error.__class__.__name__ +
                         "[" + str(exception_line) + "] " + str(current_error))


    def starter(self):
        try:
            if self.failed_init:
                raise AttributeError('Error con los datos recibidos al servicio')
            self.get_secret_information()
            self.check_token()
            logger.info("Se valido el token. Resultado: "+str(self.request_authorized))
            if not self.request_authorized:
                return self.response_maker(succesfull_run = True)
            self.final_response['policyDocument']['Statement'][0]['Effect'] = 'Allow'
            return self.response_maker(succesfull_run = True)
        except Exception as starter_error:
            exception_line = sys.exc_info()[2].tb_lineno
            current_error = starter_error
            logger.error(current_error.__class__.__name__ +
                         "[" + str(exception_line) + "] " + str(current_error))
      
    
    def get_secret_information(self):
        self.ms_tenant = get_secret(get_enviroment_variable("SECRET_AUTENTICATION_INFO"))['ms_tenant']
        #self.ms_tenant = "caa1dfbf-34d5-4061-9cdf-0ceaa516bf03"
        #self.client_id = "cc9cd684-7cf5-4c90-8a4d-e518e0968e63"
        self.client_id = get_secret(get_enviroment_variable("SECRET_AUTENTICATION_INFO"))['client_id']
    
    
    def check_token(self):
        try:
            logger.info("Iniciando validación de token.")
            PEMSTART = "-----BEGIN CERTIFICATE-----\n"
            PEMEND = "\n-----END CERTIFICATE-----\n"
            
            jwt_token = self.token
            jwks_uri = f"https://login.microsoftonline.com/{self.ms_tenant}/discovery/v2.0/keys"
            issuer = f'https://sts.windows.net/{self.ms_tenant}/'
            
            uri_response = requests.get(jwks_uri).json()
            keys = uri_response.get("keys", False)
            
            if keys:
                kid = jwt.get_unverified_header(jwt_token).get("kid")
                for key in keys:
                    if kid == key["kid"]:
                        logger.info("Procesando token.")
                        cert = PEMSTART + key["x5c"][0] + PEMEND
                        cert_obj = load_pem_x509_certificate(cert.encode(), default_backend())
                        public_key = cert_obj.public_key()
                        decoded = jwt.decode(jwt_token, public_key, algorithms=["RS256"], audience=self.client_id, issuer=issuer) #aca cambie el tenant por client id
                        
                        logger.info("DECODE"+str(decoded))
                        logger.warning(f"[check_token] El usuario {decoded['unique_name']} se autenticó correctamente")
                        self.request_authorized = True
                        break
        except Exception as check_token_error:
            exception_line = sys.exc_info()[2].tb_lineno
            current_error = check_token_error
            logger.error(current_error.__class__.__name__ +
                         "[" + str(exception_line) + "] " + str(current_error))
            
            
    def response_maker(self, succesfull_run = False, error_str = str()):
        return self.final_response
