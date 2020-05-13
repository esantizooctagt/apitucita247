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

import os

REGION = 'us-east-1'

secreKey = 'K968G66S4dC1Y5tNA5zKGT5KIjeMcpc8'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def get_secret_hash(username):
    msg = username + '52k0o8239mueu31uu5fihccbbf'
    dig = hmac.new(str('1jnl1mp5jfkjnnm2qomj95b0vtdqi268datf1g55iffam676r83g').encode('utf-8'), 
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
        userName = event['pathParameters']['userName']
        code = event['pathParameters']['code']
        
        if code != '0':
            data = json.loads(event['body'])
            key = secreKey.encode()
            ct_b64 = data['Password'] 
            passDecrypt = decrypt(ct_b64, key)
            password = passDecrypt.decode('utf-8')
            
            if password != '':
                try:
                    client = boto3.client('cognito-idp')
                    response = client.confirm_sign_up(
                                    ClientId='52k0o8239mueu31uu5fihccbbf',
                                    SecretHash=get_secret_hash(userName),
                                    Username= userName,
                                    ConfirmationCode=code,
                                    ForceAliasCreation=False
                                )
                    
                    resp = client.admin_set_user_password(
                                    UserPoolId='us-east-1_gXhBD4bsG',
                                    Username=userName,
                                    Password=password,
                                    Permanent=True
                                )

                    #STATUS 3 PENDIENTE DE VERIFICACION DE CUENTA
                    table = dynamodb.Table('TuCita247')
                    response = table.update_item(
                        Key={
                            # 'PKID': 'BUS#' + companyId,
                            'SKID': 'USERNAME#' + userName
                        },
                        UpdateExpression="set #s = :status",
                        ExpressionAttributeNames={
                            '#s': 'STATUS'
                        },
                        ExpressionAttributeValues={
                            ':status': 1
                        }
                        # ReturnValues="UPDATED_NEW"
                    )
                    
                    statusCode = 200
                    body = json.dumps({'Code': 200, 'Message': 'Account activated successfully'})
                except client.exceptions.UserNotFoundException:
                    statusCode = 404
                    body = json.dumps({"Code":500, "error": True, "success": False, "Message": "Username doesnt exists"})
                except client.exceptions.CodeMismatchException:
                    statusCode = 404 
                    body = json.dumps({"Code":500,"error": True, "success": False, "Message": "Invalid Verification code"})
                except client.exceptions.NotAuthorizedException:
                    statusCode = 404
                    body = json.dumps({"Code":500,"error": True, "success": False, "Message": "User is already confirmed"})
                except Exception as e:
                    statusCode = 404
                    body = json.dumps({"Code":500,"error": True, "success": False, "Message": f"Unknown error {e.__str__()} "})
            else:
                statusCode = 404
                body = json.dumps({"Code": 500, "Message": "You must enter a valid password"})
        else:
            try:
                client = boto3.client('cognito-idp')
                response = client.resend_confirmation_code(
                    ClientId='52k0o8239mueu31uu5fihccbbf',
                    SecretHash=get_secret_hash(userName),
                    Username=userName
                )
                statusCode = 200
                body = json.dumps({'Code': 200, 'Message': 'Account activated successfully'})
            except client.exceptions.UserNotFoundException:
                statusCode = 404
                body = json.dumps({"Code":500,"error": True, "success": False, "message":   "Username doesnt exists"})
            except client.exceptions.InvalidParameterException:
                statusCode = 404
                body = json.dumps({"Code":500,"error": True, "success": False, "message": "User is already confirmed"})
            except Exception as e:
                statusCode = 404
                body = json.dumps({"Code":500,"error": True, "success": False, "message": f"Unknown error {e.__str__()} "})

        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Code': 500, 'Message': 'Error on update user'})
    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message': 'Error on request try again'})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : cors
        },
        'body' : body
    }
    
    return response