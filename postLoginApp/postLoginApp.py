import sys
import logging
import json
import decimal

import datetime
import dateutil.tz
from datetime import timezone
import time

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

secreKey = 'K968G66S4dC1Y5tNA5zKGT5KIjeMcpc8'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.client('dynamodb', region_name='us-east-1')
logger.info("SUCCESS: Connection to DynamoDB succeeded")

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
    # if stage['origin'] != "http://localhost:4200":
    #     cors = os.environ['prodCors']
    # else:
    #     cors = os.environ['devCors']
    cors = "http://localhost:8100"

    try:
        data = json.loads(event['body'])
        phone = data['Phone']

        client = 'CUS#'
        key = secreKey.encode()
        ct_b64 = data['Password'] 
        passDecrypt = decrypt(ct_b64, key)
        response = dynamodb.query(
            TableName = "TuCita247",
            IndexName = "TuCita247",
            ReturnConsumedCapacity ='TOTAL',
            KeyConditionExpression ='PKID = :phone AND begins_with( SKID, :client ) ',
            FilterExpression = 'PASSWORD = :password',
            ExpressionAttributeValues = {
                ':phone': {'S': 'MOB#' + phone},
                ':password' : {'S' : passDecrypt.decode('utf-8')}
            },
            Limit=1
        )
        if response['Count'] == 0:
            error = json.dumps({'Message':'Auth failed','Code':400})
        else:
            for item in response['Items']:
                recordset = {
                    'ClientId': item['SKID'].replace('CUS#',''),
                    'Phone': item['PKID'].replace('MOB#',''),
                    'Name': item['NAME'],
                    'DOB': item['DOB'],
                    'Email': item['EMAIL'],
                    'Gender': item['GENDER'],
                    'Preferences': item['PREFERENCES']
                }
                result = { 'Code': 100, 'user' : recordset, 'token' : '123', 'access': '456' }
            
                statusCode = 200
                body = json.dumps(result)

        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Code': 500, 'Message': 'Error on update user'})
    except Exception as e:
        statusCode = 500
        body = json.dumps({'Code': 500, 'Message': 'Error on request try again ' + str(e)})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "Access-Control-Allow-Origin" : cors
        },
        'body' : body
    }
    return response