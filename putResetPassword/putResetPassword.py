import sys
import logging
import json

import boto3
import botocore.exceptions

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
    cors = "http://localhost:4200"
    # if stage['origin'] != "http://localhost:4200":
    #     cors = os.environ['prodCors']
    # else:
    #     cors = os.environ['devCors']
        
    try:
        statusCode = ''
        data = json.loads(event['body'])
        username = event['pathParameters']['userId']
        code = event['pathParameters']['code']
        
        key = secreKey.encode()
        ct_b64 = data['Password'] 
        passDecrypt = decrypt(ct_b64, key)
        
        client = boto3.client('cognito-idp')
        client.confirm_forgot_password(
            ClientId='42u5aeal715thv1944dohqv2tr',
            # SecretHash=get_secret_hash(username),
            Username=username,
            ConfirmationCode=code,
            Password=passDecrypt.decode('utf-8'),
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