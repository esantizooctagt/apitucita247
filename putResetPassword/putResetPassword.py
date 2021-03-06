import sys
import logging
import json

import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr
from dynamodb_json import json_util as json_dynamodb

import hmac
import hashlib
import base64

import Crypto
from Crypto.Cipher import AES
from hashlib import md5

import os

REGION = 'us-east-1'

secreKey = 'K968G66S4dC1Y5tNA5zKGT5KIjeMcpc8'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

cognitoId = os.environ['cognitoId']
cognitoClientId = os.environ['cognitoClientId']
cognitoSecret = os.environ['cognitoSecret']

dynamodbTable = boto3.client('dynamodb', region_name=REGION)
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def get_secret_hash(username):
    msg = username + cognitoClientId
    dig = hmac.new(str(cognitoSecret).encode('utf-8'), 
        msg = str(msg).encode('utf-8'), digestmod=hashlib.sha256).digest()
    d2 = base64.b64encode(dig).decode()
    return d2
    
def unpad(data):
    return data[:-(data[-1] if type(data[-1]) == int else ord(data[-1]))]

def bytes_to_key(data, salt, output=48):
    assert len(salt) == 8, len(salt)
    data += salt
    key = md5(data).digest()
    final_key = key
    while len(final_key) < output:
        key = md5(key + data).digest()
        final_key += key
    return final_key[:output]

def decrypt(encrypted, passphrase):
    encrypted = base64.b64decode(encrypted)
    assert encrypted[0:8] == b"Salted__"
    salt = encrypted[8:16]
    key_iv = bytes_to_key(passphrase, salt, 32+16)
    key = key_iv[:32]
    iv = key_iv[32:]
    aes = AES.new(key, AES.MODE_CBC, iv)
    return unpad(aes.decrypt(encrypted[16:]))
    
def lambda_handler(event, context):
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']
        
    try:
        statusCode = ''
        userName = ''
        data = json.loads(event['body'])
        userId = event['pathParameters']['userId']
        code = event['pathParameters']['code']
        
        key = secreKey.encode()
        ct_b64 = data['Password'] 
        passDecrypt = decrypt(ct_b64, key)

        response = dynamodbTable.query(
            TableName="TuCita247",
            IndexName="TuCita247_CustAppos",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='GSI2PK = :key AND GSI2SK = :key',
            ExpressionAttributeValues={
                ':key': {'S': 'USER#' + userId}
            }
        )
        for datos in json_dynamodb.loads(response['Items']):
            userName = datos['GSI1PK'].replace('EMAIL#','')
        
        client = boto3.client('cognito-idp')
        if int(code) != 0:
            client.confirm_forgot_password(
                ClientId=cognitoClientId,
                SecretHash=get_secret_hash(userName),
                Username=userName,
                ConfirmationCode=code,
                Password=passDecrypt.decode('utf-8'),
            )
        else:
            response = client.admin_set_user_password(
                UserPoolId=cognitoId,
                Username=userName,
                Password=passDecrypt.decode('utf-8'),
                Permanent=True
            )

        statusCode = 200
        body = json.dumps({"Message": "Password change successfully", "Code": 200})
           
    except client.exceptions.UserNotFoundException as e:
        statusCode = 404
        body = json.dumps({"error": True, "Code": 400, "success": False, "data":  None, "Message": "Username doesnt exists"})
    except client.exceptions.CodeMismatchException as e:
        statusCode = 404
        body = json.dumps({"error": True, "Code": 400, "success": False, "data": None, "Message": "Invalid Verification code"})
    except client.exceptions.NotAuthorizedException as e:
        statusCode = 404
        body = json.dumps({"error": True, "Code": 400, "success": False, "data": None, "Message": "User is already confirmed"})
    except Exception as e:
        statusCode = 500
        body = json.dumps({"error": True, "Code": 500, "success": False, "data": None, "Message": f"Unknown error {e.__str__()} "})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : cors
        },
        'body' : body
    }
    return response