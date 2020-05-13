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
import os

REGION = 'us-east-1'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.client('dynamodb', region_name='us-east-1')
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def get_secret_hash(username):
    msg = username + '42u5aeal715thv1944dohqv2tr'
    dig = hmac.new(str('1jnl1mp5jfkjnnm2qomj95b0vtdqi268datf1g55iffam676r83g').encode('utf-8'), 
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

        response = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            IndexName="TuCita247_Index",
            KeyConditionExpression='GS1PK = :email',
            ExpressionAttributeValues={
                ':email': {'S': 'EMAIL#' + data['Email']}
            },
            Limit=1
        )
        userName = ''
        if response['Count'] > 0:
            item = response['Items']
            userName = item[0]['USERNAME']['S']

            if userName != '':
                client = boto3.client('cognito-idp')
                try:
                    response = client.forgot_password(
                        ClientId='42u5aeal715thv1944dohqv2tr',
                        SecretHash=get_secret_hash(userName),
                        Username=userName,
                    )
                    statusCode = 200
                    body = json.dumps({'Message': 'Email send successfully'})

                except client.exceptions.UserNotFoundException:
                    statusCode = 404
                    body = json.dumps({"error": True, "Code": 400, "data": None, "success": False, "Message": "Username doesnt exists"})
                except client.exceptions.InvalidParameterException:
                    statusCode = 404
                    body = json.dumps({"error": True, "Code": 400, "success": False,"data": None, "Message": f"User <{userName}> is not confirmed yet"})
                except client.exceptions.CodeMismatchException:
                    statusCode = 404
                    body = json.dumps({"error": True, "Code": 400, "success": False, "data": None, "Message": "Invalid Verification code"})
                except client.exceptions.NotAuthorizedException:
                    statusCode = 404
                    body = json.dumps({"error": True, "Code": 400, "success": False, "data": None, "Message": "User is already confirmed"})
                except Exception as e:
                    statusCode = 404
                    body = json.dumps({"error": True, "Code": 400, "success": False, "data": None, "Message": f"Uknown    error {e.__str__()} "})
            else:
                statusCode = 404
                body = json.dumps({'Message': 'Invalid Email'})
        else:
            statusCode = 404
            body = json.dumps({'Message': 'Invalid Email'})
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

#     SUBJECT = "Reset Password - Cashier2GO"
#     BODY_TEXT = (f"Click on the link to reset his password\r\n"
#                  "https://portal.cashier2go.com/en/resetpassword/{cod} \r\n"
#                  "This email was sent with Amazon SES using the "
#                  "AWS SDK for Python (Boto).")
#     BODY_HTML = f"""<html>
#                       <head></head>
#                           <body>
#                               <h1>Click on link to reset his password</h1>
#                               <p>This email was sent with
#                                   <a href='https://portal.cashier2go.com/resetpassword/{cod}'>Click Here</a> using the
#                                   <a>Or copy this link https://portal.cashier2go.com/resetpassword/{cod}</a>.
#                               </p>
#                           </body>
#                     </html>"""