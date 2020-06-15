import sys
import json
import logging
import random

import os

import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr
from dynamodb_json import json_util as json_dynamodb

from twilio.rest import Client

twilioAccountSID = os.environ['twilioAccountSID']
twilioAccountToken = os.environ['twilioAccountToken']
fromNumber = os.environ['fromNumber']

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.client('dynamodb', region_name='us-east-1')
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):
    # stage = event['headers']
    # cors = stage['origin']

    response = ''
    verifCode = 0
    verifCode = random.randint(100000, 999999)
    to_number = event['pathParameters']['number']
    temp_number = '+19392670007'
    from_number = fromNumber
    bodyStr = 'Your TuCita247 verification code is: ' + str(verifCode)
    
    try:
        account_sid = twilioAccountSID
        auth_token = twilioAccountToken
        client = Client(account_sid, auth_token)
        
        message = client.messages.create(
            from_= from_number,
            to = temp_number,
            body = bodyStr
        )

        details = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :mobile',
            ExpressionAttributeValues={
                ':mobile': {'S': 'MOB#' + to_number.replace('+1','')}
            }
        )
        recordset = {}
        for item in json_dynamodb.loads(details['Items']):
            recordset = {
                'CustomerId': item['SKID'].replace('CUS#',''),
                'Status': item['STATUS'],
                'Name': item['NAME'],
                'Gender': item['GENDER'] if 'GENDER' in item else '',
                'Email': item['EMAIL'] if 'EMAIL' in item else '',
                'Preferences': item['PREFERENCES'] if 'PREFERENCES' in item else '',
                'Disability': item['DISABILITY'] if 'DISABILITY' in item else '',
                'DOB': item['DOB'] if 'DOB' in item else ''
            }

        statusCode = 200
        body = json.dumps({'VerifcationCode': str(verifCode), 'Customer': recordset, 'Code': 200})
    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message': str(e), 'Code': 500})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : "*"
        },
        'body' : body
    }
    return response