from dynamodb_encryption_sdk.encrypted import CryptoConfig
from dynamodb_encryption_sdk.encrypted.item import decrypt_python_item, encrypt_python_item
from dynamodb_encryption_sdk.identifiers import CryptoAction
from dynamodb_encryption_sdk.material_providers.aws_kms import AwsKmsCryptographicMaterialsProvider
from dynamodb_encryption_sdk.structures import AttributeActions, EncryptionContext, TableInfo
from dynamodb_encryption_sdk.transform import dict_to_ddb


import boto3
import json
import logging
import base64
from boto3.dynamodb.conditions import Key, Attr

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
    print(read_item) 
    if 'Item' in read_item:
        print('if')
        decrypted_item = decrypt_python_item(read_item["Item"], crypto_config)
        print(decrypted_item) 
    else: 
        print('else')
        return {} 
    return decrypted_item