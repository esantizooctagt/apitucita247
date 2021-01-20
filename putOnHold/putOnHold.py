import sys
import logging
import json

import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr
from dynamodb_json import json_util as json_dynamodb

import os

REGION = 'us-east-1'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb', region_name=REGION)
lambdaInv = boto3.client('lambda')
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']
        
    try:
        appId = event['pathParameters']['appId']
        onhold = int(event['pathParameters']['onhold'])
        data = json.loads(event['body'])

        table = dynamodb.Table('TuCita247')
        response = table.update_item(
            Key={
                'PKID': 'APPO#' + appId,
                'SKID': 'APPO#' + appId
            },
            UpdateExpression="set ON_HOLD = :onhold",
            ExpressionAttributeValues={
                ':onhold': onhold
            }
        )

        lambdaInv.invoke(
            FunctionName='PostMessages',
            InvocationType='Event',
            Payload=json.dumps(data)
        )

        statusCode = 200
        body = json.dumps({'Message': 'Appo updated successfully', 'Code': statusCode})

        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Message': 'Error on update appo'})
    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message': 'Error on request try again ' + str(e)})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : cors
        },
        'body' : body
    }
    return response