import sys
import logging
import json
import decimal
import jwt

import datetime
import dateutil.tz
from datetime import timezone
import time

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
AUTH_KEY = 'INQXG2DJMVZDER3PJ5BVIQKHKQZDAMRQ'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

cognitoId = os.environ['cognitoId']
cognitoClientId = os.environ['cognitoClientId']
cognitoSecret = os.environ['cognitoSecret']

dynamodb = boto3.client('dynamodb', region_name=REGION)
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def default(self, o):
    if isinstance(o, decimal.Decimal):
        if abs(o) % 1 > 0:
            return float(o)
        else:
            return int(o)
    return super(DecimalEncoder, self).default(o)
        
def get_secret_hash(username):
    msg = username + cognitoClientId
    dig = hmac.new(str(cognitoSecret).encode('utf-8'), 
        msg = str(msg).encode('utf-8'), digestmod=hashlib.sha256).digest()
    d2 = base64.b64encode(dig).decode()
    return d2

def initiate_auth(client, username, password):
    try:
        secret_hash = get_secret_hash(username)
        error = ''
        auth = ''
        resp = client.admin_initiate_auth(
                UserPoolId = cognitoId,
                ClientId = cognitoClientId,
                AuthFlow = 'ADMIN_NO_SRP_AUTH',
                AuthParameters = {
                    'USERNAME': username,
                    'SECRET_HASH': secret_hash,
                    'PASSWORD': password,
                },
                ClientMetadata = {
                    'username': username,
                    'password': password,
            })
        auth = resp
    except client.exceptions.NotAuthorizedException:
        error = "The username or password is incorrect"
    except client.exceptions.UserNotConfirmedException:
        error = "User is not confirmed"
    except Exception as e:
        error = e.__str__()

    return auth, error
    
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
    region = context.invoked_function_arn.split(':')[3]
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']

    try:
        data = json.loads(event['body'])
        email = data['Email']
        superAdm = 0
        
        key = secreKey.encode()
        ct_b64 = data['Password'] 
        passDecrypt = decrypt(ct_b64, key)

        client = boto3.client('cognito-idp')
        resp, msg = initiate_auth(client, email, passDecrypt.decode('utf-8'))
        if msg != '':
            error = {
                        'statusCode' : 404,
                        'headers' : {
                            "content-type" : "application/json",
                            "Access-Control-Allow-Origin" : cors
                        },
                        'body' : json.dumps({'Message': msg, "Code": 404, "error": True, "success": False, "data": None})
                    }
            return error

        if resp.get("AuthenticationResult"):
            userNameCognitoEncode = jwt.decode(resp["AuthenticationResult"]["IdToken"], verify=False)
            userNameCognito = userNameCognitoEncode["cognito:username"]
            user, error = getUser(email)
            if error != '':
                statusCode = 404
                body = json.dumps({'Message':'Auth failed','Code':400})
            if user != None:
                business, error = getBusiness(user['PKID'].replace('BUS#',''))
                if business != None and business['STATUS'] == 1:
                    superAdm = int(user['SUPER_ADMIN']) if 'SUPER_ADMIN' in user else 0
                    isAdmin = ''
                    isAdminVal = ''
                    if 'IS_ADMIN' in user:
                        isAdmin = '' if int(user['IS_ADMIN']) == 1 else user['ROLEID']
                        isAdminVal = int(user['IS_ADMIN']) if 'IS_ADMIN' in user else ''
                    
                    recordset = {
                        'User_Id': user['USERID'],
                        'Email': user['GSI1PK'].replace('EMAIL#',''),
                        'Is_Admin': isAdminVal,
                        'Business_Id': user['PKID'].replace('BUS#',''),
                        'Avatar': user['AVATAR'] if 'AVATAR' in user else '',
                        'Role_Id': isAdmin,
                        'Language': user['LANGUAGE'] if 'LANGUAGE' in user else '',
                        'Business_Name': business['NAME'],
                        'UsrCog': userNameCognito,
                        'User_Adm': '',
                        'Email_Adm': '',
                        'Role_Adm':  user['ROLE_ADMIN'] if 'ROLE_ADMIN' in user else '',
                        'Business_Language': business['LANGUAGE'] if 'LANGUAGE' in business else 'en',
                        'Business_Adm': user['PKID'].replace('BUS#','') if 'ROLE_ADMIN' in user else ''
                    }

                    result = { 'Code': 100, 'user' : recordset, 'super_admin': superAdm, 'token' : resp["AuthenticationResult"]["IdToken"], 'access': resp["AuthenticationResult"]["AccessToken"], 'refresh': resp["AuthenticationResult"]["RefreshToken"] }
                    statusCode = 200
                    body = json.dumps(result)
                else:
                    statusCode = 404
                    body = json.dumps({'Message':'Auth failed','Code':400})

        if error != '':
            statusCode = 404
            body = error

    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message': 'Error on request try again ' + str(e)})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "Access-Control-Allow-Origin" : cors
        },
        'body' : body
    }
    return response

def getUser(email):
    res = ''
    error = ''
    try:
        response = dynamodb.query(
            TableName="TuCita247",
            IndexName="TuCita247_Index",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='GSI1PK = :email',
            ExpressionAttributeValues={
                ':email': {'S': 'EMAIL#' + email}
            },
            Limit=1
        )
        if response['Count'] == 0:
            error = json.dumps({'Message':'Auth failed','Code':400})
        if response['Count'] > 0:
            item = response['Items']
            res = json_dynamodb.loads(item[0])
    except Exception as e:
        error = json.dumps(str(e))
    return res, error

def getBusiness(businessId):
    res = ''
    error = ''
    try:
        response = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :businessId AND SKID =:meta',
            ExpressionAttributeValues={
                ':businessId': {'S': 'BUS#' + businessId},
                ':meta': {'S': 'METADATA'}
            },
            Limit=1
        )
        if response['Count'] == 0:
            error = json.dumps({'Message':'Auth failed','Code':400})
        if response['Count'] > 0:
            item = response['Items']
            res = json_dynamodb.loads(item[0])
    except Exception as e:
        error = json.dumps(str(e))
    return res, error