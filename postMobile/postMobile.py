import sys
import json
import logging
import random
import os
from twilio.rest import Client

twilioAccountSID = os.environ['twilioAccountSID']
twilioAccountToken = os.environ['twilioAccountToken']
fromNumber = os.environ['fromNumber']

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    stage = event['headers']
    if stage['origin'] != "http://localhost:8100":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']

    response = ''
    verifCode = 0
    verifCode = random.randint(100000, 999999)
    to_number = event['pathParameters']['number']
    to_number = '+19392670007'
    from_number = fromNumber
    bodyStr = 'Your TuCita247 verification code is: ' + str(verifCode)
    
    try:
        account_sid = twilioAccountSID
        auth_token = twilioAccountToken
        client = Client(account_sid, auth_token)
        
        message = client.messages.create(
            from_= from_number,
            to = to_number,
            body = bodyStr
        )
        statusCode = 200
        body = json.dumps({'VerifcationCode': str(verifCode), 'Code': 200})
    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message': str(e), 'Code': 500})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : cors
        },
        'body' : body
    }
    return response