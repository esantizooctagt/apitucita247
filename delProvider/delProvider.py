import sys
import logging
import json

import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr

import base64

import uuid
import os

REGION = 'us-east-1'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
logger.info("SUCCESS: Connection to DynamoDB succeeded")
    
def lambda_handler(event, context):
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']
        
    try:
        statusCode = ''
        providerId = event['pathParameters']['providerId']
        businessId = event['pathParameters']['businessId']
        locationId = event['pathParameters']['locationId']
        
        table = dynamodb.Table('TuCita247')
        response = table.update_item(
            Key={
                'PKID': 'BUS#' + businessId + '#' + locationId,
                'SKID': 'PRO#' + providerId
            },
            UpdateExpression="SET #s = :status",
            ExpressionAttributeNames={'#s': 'STATUS'},
            ExpressionAttributeValues={
                ':status': 2
            },
            ConditionExpression="attribute_exists(PKID) AND attribute_exists(SKID)",
            ReturnValues="NONE"
        )

        statusCode = 200
        body = json.dumps({'Message': 'Service provider deleted successfully', 'Code': 200})

        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Message': 'Error on deleted service provider'})
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