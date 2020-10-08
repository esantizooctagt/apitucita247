import sys
import logging
import json

import boto3
import botocore.exceptions
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
from dynamodb_json import json_util as json_dynamodb

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
    # stage = event['headers']
    # if stage['origin'] != "http://localhost:4200":
    #     cors = os.environ['prodCors']
    # else:
    #     cors = os.environ['devCors']
        
    try:
        businessId = event['pathParameters']['businessId']
        plan = event['pathParameters']['plan']
        appos = event['pathParameters']['appos']
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
                "UpdateExpression":"SET #n = :name, APPOINTMENTS = :appos, AVAILABLE = :appos, #o = :order",
                "ExpressionAttributeNames":{'#n': 'NAME','#o': 'ORDER'},
                "ExpressionAttributeValues": { 
                    ":name": {"S": plan},
                    ":appos": {"N": str(appos)},
                    ":order": {"S": order}
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
                "UpdateExpression":"set GSI2PK = :gsi2pk",
                "ExpressionAttributeValues": { 
                    ":gsi2pk": {"S": "PLAN#"+plan} 
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