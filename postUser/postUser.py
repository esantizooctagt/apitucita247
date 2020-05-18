import sys
import logging
import json

import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr

import hmac
import hashlib
import base64

import Crypto
from Crypto.Cipher import AES
from hashlib import md5

import uuid
import os

REGION = 'us-east-1'

secreKey = 'K968G66S4dC1Y5tNA5zKGT5KIjeMcpc8'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.client('dynamodb', region_name='us-east-1')
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def get_secret_hash(username):
    msg = username + '52k0o8239mueu31uu5fihccbbf'
    dig = hmac.new(str('1r2k3dm8748i5dfu632eu8ptai7vocidm01vp3la82nhq91jgqqt').encode('utf-8'), 
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
        userId = str(uuid.uuid4()).replace("-","")
        data = json.loads(event['body'])
        email = data['Email']

        #STATUS 3 PENDIENTE DE VERIFICACION DE CUENTA
        response = dynamodb.transact_write_items(
            TransactItems=[
                {
                    "Put": {
                        "TableName": "TuCita247",
                        "Item": {
                            "PKID": {"S": 'BUS#'+data['BusinessId']},
                            "SKID": {"S": 'USER#' + userId },
                            "GSI1PK": {"S": 'EMAIL#' + email},
                            "GSI1SK": {"S": 'USER#' + userId},
                            "FIRST_NAME": {"S": data['First_Name']},
                            "LAST_NAME": {"S": data['Last_Name']},
                            "PHONE": {"S": data['Phone'].replace('(','').replace(')','').replace('-','').replace(' ','')},
                            "MFACT_AUTH": {"N": "0"},
                            "IS_ADMIN": {"N": "0"},
                            "ROLEID": {"S": data['RoleId']},
                            "USERID": {"S": userId },
                            "STATUS": {"N": "1"}
                        },
                        "ConditionExpression": "attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                        # "ExpressionAttributeNames": {'#s': 'STATUS'},
                        "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
                    },
                },
                {
                    "Put": {
                        "TableName": "TuCita247",
                        "Item": {
                            "PKID": {"S": 'EMAIL#'+email},
                            "SKID": {"S": 'EMAIL#'+email},
                            "MFACT_AUTH": {"N": "0"}
                        },
                        "ConditionExpression": "attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                        "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
                    }
                }
            ]
        )
        logger.info("data")
        try:
            key = secreKey.encode()
            ct_b64 = data['Password'] 
            passDecrypt = decrypt(ct_b64, key)
            logger.info(passDecrypt)
            client = boto3.client('cognito-idp')
            response = client.sign_up(
                        ClientId='52k0o8239mueu31uu5fihccbbf',
                        SecretHash=get_secret_hash(email),
                        Username=email,
                        Password=passDecrypt.decode('utf-8'),
                        UserAttributes=[
                            {
                                'Name': 'email',
                                'Value': email
                            },
                            {
                                'Name': 'custom:userId',
                                'Value': userId
                            },
                            {
                                'Name': 'custom:wpUserId',
                                'Value': ''
                            },
                            {   
                                'Name': 'custom:isAdmin',
                                'Value': '0'
                            }
                        ]
                    )

            statusCode = 200
            body = json.dumps({'Message': 'User added successfully'})

        except client.exceptions.UsernameExistsException as e:
            statusCode = 404
            body = json.dumps({"Code":400, "error": False, 
                    "success": True, 
                    "message": "This email already exists", 
                    "data": None})
        except client.exceptions.InvalidPasswordException as e:
            statusCode = 404
            body = json.dumps({"Code":400,"error": False, 
                    "success": True, 
                    "message": "Password should have Caps,\
                                Special chars, Numbers", 
                    "data": None})
        except client.exceptions.UserLambdaValidationException as e:
            statusCode = 404
            body = json.dumps({"Code":400,"error": False, 
                    "success": True, 
                    "message": "Email already exists " + str(e), 
                    "data": None})
        
        except Exception as e:
            statusCode = 404
            body = json.dumps({"Code":400,"error": False, 
                    "success": True, 
                    "message": str(e), 
                    "data": None})

        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Message': 'Error on added user'})

    except dynamodb.exceptions.TransactionCanceledException as e:
            statusCode = 404
            body = json.dumps({"Code":400,"error": False, 
                    "success": True, 
                    "message": str(e), 
                    "data": None})
    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message': 'Error on request try again ' + str(e)})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : cors
        },
        'body' : body
    }
    return response