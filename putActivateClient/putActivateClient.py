import sys
import logging
import json

import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr

import os

REGION = 'us-east-1'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):
    stage = event['headers']
    # if stage['origin'] != "http://localhost:4200":
    #     cors = os.environ['prodCors']
    # else:
    #     cors = os.environ['devCors']
    cors = "http://localhost:8100"
        
    try:
        statusCode = ''
        data = json.loads(event['body'])
        mobilePhone = data['Phone']

        e = {'#s': 'STATUS'}
        table = dynamodb.Table('TuCita247')
        response = table.update_item(
            Key={
                'PKID': 'MOB#' + mobilePhone,
                'SKID': 'begins_with (SKID, "MOB#")'
            },
            UpdateExpression="set #s = :status",
            ExpressionAttributeNames=e,
            ExpressionAttributeValues={
                ':status': "1"
            }
        )
        statusCode = 200
        body = json.dumps({'Message': 'User activated successfully', 'Code': 200})

        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Message': 'Error on update user', 'Code': 200})
    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message': 'Error on request try again ' + str(e), 'Code': 200 })

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : cors
        },
        'body' : body
    }
    return response