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

cognitoId = os.environ['cognitoId']
cognitoClientId = os.environ['cognitoClientId']
cognitoSecret = os.environ['cognitoSecret']

def get_secret_hash(username):
    msg = username + cognitoClientId
    dig = hmac.new(str(cognitoSecret).encode('utf-8'), 
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
            idToken = ''
            accessToken = ''
            secret_hash = get_secret_hash(email)
            response = client.initiate_auth(
                ClientId = cognitoClientId,
                AuthFlow = 'REFRESH_TOKEN_AUTH',
                AuthParameters={
                    'REFRESH_TOKEN': token,
                    'SECRET_HASH': secret_hash
                }
            )
            if response['AuthenticationResult']:
                idToken = response['AuthenticationResult']['IdToken']
                accessToken = response['AuthenticationResult']['AccessToken']
            logger.info(response)
            statusCode = 200
            body = json.dumps({'Message': 'Tokens renew successfully', 'Code': 200, 'token': idToken, 'access': accessToken})

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