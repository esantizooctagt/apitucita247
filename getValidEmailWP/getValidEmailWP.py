import sys
import logging
import random
import json

import hashlib

import boto3
import botocore.exceptions
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
from dynamodb_json import json_util as json_dynamodb

import os

REGION = 'us-east-1'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.client('dynamodb', region_name=REGION)
ses = boto3.client('ses',region_name=REGION)
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):
    stage = event['headers']
    if stage['origin'] != "http://tucita247.local":
        if stage['origin'] == "https://tucita247.com":
            cors = os.environ['prodCors']
        else:
            cors = os.environ['prodWCors']
    else:
        cors = os.environ['devCors']
    # cors = "*"
        
    try:
        email = event['pathParameters']['email']
        LAN = event['pathParameters']['lan']
        existe = 0
        verCode = ''
        getEmail = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :email',
            ExpressionAttributeValues={
                ':email': {'S': 'EMAIL#'+email}
            }
        )
        for row in json_dynamodb.loads(getEmail['Items']):
            existe = 1

        if existe == 0:
            code = 0
            code = str(random.randint(100000, 999999))
            verCode = hashlib.md5(code.encode("utf")).hexdigest()

            #EMAIL
            SENDER = "Tu Cita 24/7 <no-reply@tucita247.com>"
            RECIPIENT = email

            logger.info("prev send email")
            response = ses.send_templated_email(
                Source=SENDER,
                Destination={
                    'ToAddresses': [
                        RECIPIENT,
                    ],
                },
                Template ='VALIDATEEMAIL_' + LAN, 
                TemplateData='{ "code": "'+ code +'" }'
            )

        statusCode = 200
        body = json.dumps({'Message': 'Email send successfully', 'Existe': existe, 'VerificationCode': verCode, 'Code': 200})

        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Message': 'Error on update appointment', 'Code': 500})
    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message': 'Error on request try again ' + str(e), 'Code': 500})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : cors
        },
        'body' : body
    }
    return response