import sys
import logging
import json

import boto3
import botocore.exceptions
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr

import os

REGION = 'us-east-1'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.client('dynamodb', region_name=REGION)
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):
    stage = event['headers']
    cors = stage['origin']
        
    try:
        businessId = event['pathParameters']['businessId']
        order = event['pathParameters']['order']

        items = []
        rows = {}
        rows = {
            "Update": {
                "TableName": "TuCita247",
                "Key": {
                    "PKID": {"S": 'BUS#' + businessId },
                    "SKID": {"S": 'PLAN' }
                },
                "UpdateExpression":"SET #o = :order, #s = :status",
                "ExpressionAttributeNames":{'#o': 'ORDER','#s': 'STATUS'},
                "ExpressionAttributeValues": {
                    ":order": {"S": order},
                    ":status": {"N", str(0)}
                },
                "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
            },
        }
        items.append(rows)

        rows = {
            "Update": {
                "TableName": "TuCita247",
                "Key": {
                    "PKID": {"S": 'BUS#' + businessId },
                    "SKID": {"S": 'METADATA' }
                },
                "UpdateExpression":"set #s = :status",
                "ExpressionAttributeValues": { 
                    ":status": {"N", str(0)}
                },
                "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
            },
        }
        items.append(rows)

        logger.info(items)
        response = dynamodb.transact_write_items(
            TransactItems = items
        )

        statusCode = 200
        body = json.dumps({'Message': 'Business updated successfully', 'Code': 200})

        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Message': 'Error on update special day', 'Code': 500})
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