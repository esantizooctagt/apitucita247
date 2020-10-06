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

        client = boto3.client('cognito-idp')
        try:
            response = client.forgot_password(
                ClientId=cognitoClientId,
                SecretHash=get_secret_hash(email),
                Username=email,
            )
            statusCode = 200
            body = json.dumps({'Message': 'Email send successfully', 'Code': 200})

        except client.exceptions.UserNotFoundException:
            statusCode = 404
            body = json.dumps({"error": True, "Code": 400, "data": None, "success": False, "Message": "Username doesnt exists"})
        except client.exceptions.InvalidParameterException:
            statusCode = 404
            body = json.dumps({"error": True, "Code": 400, "success": False,"data": None, "Message": f"User <{email}> is not confirmed yet"})
        except client.exceptions.CodeMismatchException:
            statusCode = 404
            body = json.dumps({"error": True, "Code": 400, "success": False, "data": None, "Message": "Invalid Verification code"})
        except client.exceptions.NotAuthorizedException:
            statusCode = 404
            body = json.dumps({"error": True, "Code": 400, "success": False, "data": None, "Message": "User is already confirmed"})
        except Exception as e:
            statusCode = 404
            body = json.dumps({"error": True, "Code": 400, "success": False, "data": None, "Message": f"Uknown    error {e.__str__()} "})

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