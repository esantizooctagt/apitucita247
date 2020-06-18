import sys
import json
import logging

import os

import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr
from dynamodb_json import json_util as json_dynamodb

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):
    try:
        customerId = event['pathParameters']['customerId']
        locationId = event['pathParameters']['locationId']

        table = dynamodb.Table('TuCita247')
        details = table.delete_item(
            Key={
                'PKID': 'CUS#' + customerId,
                'SKID': 'FAVS#' + locationId
            }
        )

        statusCode = 200
        body = json.dumps({'Message': 'Favorite delete successfully', 'Code': 200})
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