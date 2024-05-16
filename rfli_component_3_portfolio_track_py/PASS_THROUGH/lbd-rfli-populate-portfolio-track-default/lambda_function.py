from dynamodb_encryption_sdk.encrypted import CryptoConfig
from dynamodb_encryption_sdk.encrypted.item import decrypt_python_item, encrypt_python_item
from dynamodb_encryption_sdk.identifiers import CryptoAction
from dynamodb_encryption_sdk.material_providers.aws_kms import AwsKmsCryptographicMaterialsProvider
from dynamodb_encryption_sdk.structures import AttributeActions, EncryptionContext, TableInfo
from dynamodb_encryption_sdk.transform import dict_to_ddb

import boto3
import os

dynamodb                    = boto3.resource("dynamodb")

def encrypt_portf(user, portfolio, table_name, kms_id):
    obj = {
        "user_id": user,
        "portfolios": portfolio
    }
    
    encrypted_attributes = set(obj.keys())
    
    table = dynamodb.Table(table_name)
    table_info = TableInfo(name=table_name)
    table_info.refresh_indexed_attributes(table.meta.client)
    aws_kms_cmp = AwsKmsCryptographicMaterialsProvider(kms_id)
    encryption_context = EncryptionContext(
        table_name=table_name,
        partition_key_name=table_info.primary_index.partition
    )
    
    actions = AttributeActions(
        attribute_actions={"user_id": CryptoAction.DO_NOTHING},
        default_action=CryptoAction.ENCRYPT_AND_SIGN
    )
    
    actions.set_index_keys(*table_info.protected_index_keys())
    crypto_config = CryptoConfig(
        materials_provider=aws_kms_cmp, encryption_context=encryption_context, attribute_actions=actions
    )

    encrypted_item = encrypt_python_item(obj, crypto_config)
    
    table.put_item(Item=encrypted_item)
    
    return encrypted_item


def decrypt_item(user, table_name, kms_id):
    table = dynamodb.Table(table_name)
    
    table_info = TableInfo(name=table_name)
    table_info.refresh_indexed_attributes(table.meta.client)
    
    aws_kms_cmp = AwsKmsCryptographicMaterialsProvider(kms_id)
    
    encryption_context = EncryptionContext(
        table_name=table_name,
        partition_key_name=table_info.primary_index.partition,
    )
    
    actions = AttributeActions(
        attribute_actions={"user_id": CryptoAction.DO_NOTHING},
        default_action=CryptoAction.ENCRYPT_AND_SIGN
    )
    
    crypto_config = CryptoConfig(
        materials_provider=aws_kms_cmp, encryption_context=encryption_context, attribute_actions=actions
    )
    
    read_item = table.get_item(Key = {"user_id": user})
    if 'Item' in read_item:
        
        decrypted_item = decrypt_python_item(read_item["Item"], crypto_config)
    
    else: 
        return {}
    return decrypted_item
    
def get_items_from_tab(items, table):
    
    isines = []
   
    for item in items:
        
        temp_key = {
            "isin": item
        }
        
        temp_item = table.get_item(Key = temp_key)
        
        if('Item' in temp_item):
            isines.append(temp_item['Item'])
            
    return isines

def lambda_handler(event, context):
  
    kms_id           = os.environ['KMS_ID']
  
    default_user_portfolio = {
      'portfolios': {
        'PREDETERMINADO':{
          'COD16CB00022':0,
	        'COT09CB00049':0,
	        'COT09CB00064':0,
	        'COT09CB00080':0,
	        'COT09CB00098':0,
	        'COT09CB00106':0,
	        'COB01CB00150':0,
        	'COB01CB00143':0,
	        'COB07CB00355':0,
	        'COB07CB00363':0
        }
      }
    }
    user_isines_table = dynamodb.Table('dnb-rfli-portfolio-track-params-isines')
    default_isines = { 
          'user_id':'default', 
          'isines':['COD16CB00022','COT09CB00049','COT09CB00064','COT09CB00080','COT09CB00098','COT09CB00106','COB01CB00150','COB01CB00143','COB07CB00355','COB07CB00363']
    }
    
    ALL_ISINES_TABLE='dnb-rfli-portfolio-track-all-isines'
    isin_all_tab     = dynamodb.Table(ALL_ISINES_TABLE)
    default_isines_value = get_items_from_tab(default_isines['isines'], isin_all_tab)
    
    
    user_isines = {
                'user_id': 'default',
                'isines': default_isines_value
            } 
    
    put_user_isines = dynamodb.Table('dnb-rfli-portfolio-track-user-isines').put_item(
                Item = user_isines
                )
     
    encrypt_portf('default', default_user_portfolio['portfolios'], 'dnb-rfli-portfolio-track-params-portfolios', kms_id)
    user_isines_table.put_item(Item=default_isines)
   
    user_portfolio_response = decrypt_item('default', 'dnb-rfli-portfolio-track-params-portfolios' , kms_id )
  
    
    
    
    
    
    
    
    return "FINALIZADO"