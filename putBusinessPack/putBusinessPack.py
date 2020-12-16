import sys
import logging
import json

import boto3
import botocore.exceptions
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr

import datetime
import dateutil.tz
from datetime import timezone

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
        appos = event['pathParameters']['appos']
        order = event['pathParameters']['order']
        
        country_date = dateutil.tz.gettz('America/Puerto_Rico')
        today = datetime.datetime.now(tz=country_date) + datetime.timedelta(days=90)
        dueDate = today.strftime("%Y-%m-%d")

        items = []
        rows = {}
        rows = {
            "Put": {
                "TableName": "TuCita247",
                "Item": {
                    "PKID": {"S": 'BUS#' + businessId },
                    "SKID": {"S": 'PACK#' + dueDate + '#' + order},
                    "ORDER": {"S": order},
                    "AVAILABLE": {"N": str(appos)},
                    "APPOINTMENTS": {"N": str(appos)}
                },
                "ConditionExpression": "attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                "ReturnValuesOnConditionCheckFailure": "NONE"
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