import sys
import logging
import json
import decimal

import pyotp

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
#REGION = 'us-east-1'

secreKey = 'K968G66S4dC1Y5tNA5zKGT5KIjeMcpc8'
AUTH_KEY = 'INQXG2DJMVZDER3PJ5BVIQKHKQZDAMRQ'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.client('dynamodb', region_name='us-east-1')
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def default(self, o):
    if isinstance(o, decimal.Decimal):
        if abs(o) % 1 > 0:
            return float(o)
        else:
            return int(o)
    return super(DecimalEncoder, self).default(o)
        
def get_secret_hash(username):
    msg = username + '42u5aeal715thv1944dohqv2tr'
    dig = hmac.new(str('1jnl1mp5jfkjnnm2qomj95b0vtdqi268datf1g55iffam676r83g').encode('utf-8'), 
        msg = str(msg).encode('utf-8'), digestmod=hashlib.sha256).digest()
    d2 = base64.b64encode(dig).decode()
    return d2

def initiate_auth(client, username, password):
    secret_hash = get_secret_hash(username)
    error = None
    auth = None
    try:
        resp = client.admin_initiate_auth(
                UserPoolId = 'us-east-1_TrUdzLGjb',
                ClientId = '42u5aeal715thv1944dohqv2tr',
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
    countryCode = stage['cloudfront-viewer-country']

    try:
        data = json.loads(event['body'])
        fact, error = getUser(data['UserName'], 0)
        
        if fact == '0':
            key = secreKey.encode()
            ct_b64 = data['Password'] 
            passDecrypt = decrypt(ct_b64, key)
            
            client = boto3.client('cognito-idp')
            resp, msg = initiate_auth(client, data['UserName'], passDecrypt.decode('utf-8'))
            if msg != None:
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
                try:
                    response = client.global_sign_out(
                                    AccessToken=resp["AuthenticationResult"]["AccessToken"]
                                )
                except Exception as e:
                    logger.info(error = e.__str__())
                # return {'message': "success", 
                #         "error": False, 
                #         "success": True, 
                #         "data": {
                #         "id_token": resp["AuthenticationResult"]["IdToken"],
                #         "refresh_token": resp["AuthenticationResult"]["RefreshToken"],
                #         "access_token": resp["AuthenticationResult"]["AccessToken"],
                #         "expires_in": resp["AuthenticationResult"]["ExpiresIn"]
                #         "token_type": resp["AuthenticationResult"]["TokenType"]
                #         }}
            
                user, error = getUser(data['UserName'], 1)
                if error != '':
                    statusCode = 404
                    body = json.dumps({'Message':'Auth failed','Code':400})
                if user != None:
                    company, error = getCompany(user['GS1SK'])
                    if error != '':
                        statusCode = 404
                        body = json.dumps({'Message':'Auth failed','Code':400})
                    else:
                        country = json.loads(company['ADDRESS'])
                        recordset = {
                            'User_Id': user['USERID'],
                            'User_Name': user['USERNAME'].lower(),
                            'Email': user['EMAIL'],
                            'Is_Admin': int(user['IS_ADMIN']),
                            'Company_Id': user['GS1SK'].split('#')[1],
                            'Avatar': user['AVATAR'],
                            'Currency': company['CURRENCY'],
                            'Country': country['Country'], 
                            'Role_Id': '' if int(user['IS_ADMIN']) == 1 else user['ROLEID'],
                            'Store_Id': user['STOREID'],
                            'Language_Id': user['LANGUAGE'],
                            'Cashier_Id': '',
                            'Region': region
                        }
                        result = { 'Code': 100, 'user' : recordset, 'token' : resp["AuthenticationResult"]["IdToken"], 'access': resp["AuthenticationResult"]["AccessToken"] }
                        #context.invoked_function_arn.split(':')[3]
                        statusCode = 200
                        body = json.dumps(result)
                        
        if fact == '1' and data['MFact_Auth'] == '':
            statusCode = 200
            body = json.dumps({'Message': 'Missing Google Authenticator','Code':300})
            
        if fact == '1' and data['MFact_Auth'] != '':
            #Europe/Berlin
            region = ''
            if countryCode == 'GT':
                region = 'America/Guatemala'
            else:
                region = 'Europe/Berlin'
                
            country_date = dateutil.tz.gettz(region)
            country_now = datetime.datetime.now(tz=country_date).timestamp()

            hotp = pyotp.TOTP(AUTH_KEY)
            valFA = hotp.at(country_now)
            
            if valFA != data['MFact_Auth']:
                statusCode = 200
                body = json.dumps({'Message': 'Invalid Google Authenticator','Code': 200})
            else:
                key = secreKey.encode()
                ct_b64 = data['Password'] 
                passDecrypt = decrypt(ct_b64, key)
                
                client = boto3.client('cognito-idp')
                resp, msg = initiate_auth(client, data['UserName'], passDecrypt.decode('utf-8'))
                if msg != None:
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
                    try:
                        response = client.global_sign_out(
                                        AccessToken=resp["AuthenticationResult"]["AccessToken"]
                                    )
                    except Exception as e:
                        logger.info(error = e.__str__())
                    # return {'message': "success", 
                    #         "error": False, 
                    #         "success": True, 
                    #         "data": {
                    #         "id_token": resp["AuthenticationResult"]["IdToken"],
                    #         "refresh_token": resp["AuthenticationResult"]["RefreshToken"],
                    #         "access_token": resp["AuthenticationResult"]["AccessToken"],
                    #         "expires_in": resp["AuthenticationResult"]["ExpiresIn"]
                    #         "token_type": resp["AuthenticationResult"]["TokenType"]
                    #         }}
                
                
                    user, error = getUser(data['UserName'], 1)
                    if error != '':
                        statusCode = 404
                        body = json.dumps({'Message':'Auth failed','Code':400})
                    if user != None:
                        company, error = getCompany(user['GS1SK'])
                        if error != '':
                            statusCode = 404
                            body = json.dumps({'Message':'Auth failed','Code':400})
                        else:
                            country = json.loads(company['ADDRESS'])
                            recordset = {
                                'User_Id': user['USERID'],
                                'User_Name': user['USERNAME'].lower(),
                                'Email': user['EMAIL'],
                                'Is_Admin': int(user['IS_ADMIN']),
                                'Company_Id': user['GS1SK'].split('#')[1],
                                'Avatar': user['AVATAR'],
                                'Currency': company['CURRENCY'],
                                'Country': country['Country'], 
                                'Role_Id': '' if int(user['IS_ADMIN']) == 1 else user['ROLEID'],
                                'Store_Id': user['STOREID'],
                                'Language_Id': user['LANGUAGE'],
                                'Cashier_Id': '',
                                'Region': region
                            }
                            result = { 'Code': 100, 'user' : recordset, 'token' : resp["AuthenticationResult"]["IdToken"], 'access': resp["AuthenticationResult"]["AccessToken"] }
                            #context.invoked_function_arn.split(':')[3]
                            statusCode = 200
                            body = json.dumps(result)

        if error != '':
            statusCode = 404
            body = error

    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message': 'Error on request try again ' + str(e)})
        #str(e)

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "Access-Control-Allow-Origin" : cors
        },
        'body' : body
    }
    return response

def getUser(userName, total):
    res = ''
    error = ''
    try:
        response = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            IndexName="TuCita247_Index",
            KeyConditionExpression='GS1PK = :username',
            ExpressionAttributeValues={
                ':username': {'S': 'USERNAME#' + userName.upper()}
            },
            Limit=1
        )
        logger.info(response)
        if response['Count'] == 0:
            error = json.dumps({'Message':'Auth failed','Code':400})
        if response['Count'] > 0:
            item = response['Items']
            if total == 0:
                res = item[0]['MFACT_AUTH']['S']
            else:
                res = json_dynamodb.loads(item[0])
    except Exception as e:
        error = json.dumps(str(e))
    return res, error

def getCompany(companyId):
    res = ''
    error = ''
    try:
        response = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :companyId AND begins_with ( SKID , :metadata )',
            ExpressionAttributeValues={
                ':companyId': {'S': companyId},
                ':metadata': {'S': 'METADATA#'}
            },
            Limit=1
        )
        logger.info(response)
        if response['Count'] == 0:
            error = json.dumps({'Message':'Auth failed','Code':400})
        if response['Count'] > 0:
            item = response['Items']
            res = json_dynamodb.loads(item[0])
    except Exception as e:
        error = json.dumps(str(e))
    return res, error