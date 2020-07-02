import sys
import logging
import json

import boto3
import botocore.exceptions

import hmac
import hashlib
import base64
import os

REGION = 'us-east-1'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_secret_hash(username):
    msg = username + '52k0o8239mueu31uu5fihccbbf'
    dig = hmac.new(str('1r2k3dm8748i5dfu632eu8ptai7vocidm01vp3la82nhq91jgqqt').encode('utf-8'), 
        msg = str(msg).encode('utf-8'), digestmod=hashlib.sha256).digest()
    d2 = base64.b64encode(dig).decode()
    return d2
    
def lambda_handler(event, context):
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']

    try:
        statusCode = ''
        data = json.loads(event['body'])
        email = data['Email']
        token = data['RefreshTkn']

        client = boto3.client('cognito-idp')
        try:
            secret_hash = get_secret_hash(email)
            response = client.initiate_auth(
                ClientId = '52k0o8239mueu31uu5fihccbbf',
                AuthFlow = 'REFRESH_TOKEN_AUTH',
                AuthParameters={
                    'REFRESH_TOKEN': token,
                    'SECRET_HASH': secret_hash
                }
            )
            idToken = response["AuthenticationResult"]["IdToken"] if response["AuthenticationResult"]["IdToken"] in response else '' 
            accessToken = response['AuthenticationResult']['AccessToken'] if response["AuthenticationResult"]["AccessToken"] in response else ''
            logger.info(response)
            statusCode = 200
            body = json.dumps({'Message': 'Tokens renew successfully', 'Code': 200, 'TokenId': idToken, 'TokenAccess': accessToken})

        except client.exceptions.UserNotFoundException:
            statusCode = 404
            body = json.dumps({"error": True, "Code": 400, "data": None, "success": False, "Message": "Username doesnt exists"})
        except client.exceptions.InvalidParameterException:
            statusCode = 404
            body = json.dumps({"error": True, "Code": 400, "success": False,"data": None, "Message": f"User <{email}> is not confirmed yet"})
        except client.exceptions.CodeMismatchException:
            statusCode = 404
            body = json.dumps({"error": True, "Code": 400, "success": False, "data": None, "Message": "Invalid Verification code"})
        except client.exceptions.NotAuthorizedException as e:
            statusCode = 404
            body = json.dumps({"error": True, "Code": 400, "success": False, "data": None, "Message": str(e)})
        except Exception as e:
            statusCode = 404
            body = json.dumps({"error": True, "Code": 400, "success": False, "data": None, "Message": f"Uknown    error {e.__str__()} "})

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