import sys
import json
import logging

import os

import datetime
import dateutil.tz
from datetime import timezone

import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr
from dynamodb_json import json_util as json_dynamodb

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.client('dynamodb', region_name=REGION)
dynamodbQuery = boto3.resource('dynamodb', region_name=REGION)
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):    
    try:
        today = datetime.datetime.now()-datetime.timedelta(hours=0,minutes=5)
        currTime = today.strftime("%Y-%m-%d-%H-%M-%S")

        details = dynamodb.query(
            TableName="TuCita247",
            IndexName="TuCita247_CustAppos",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='GSI2PK = :pkid AND GSI2SK <= :currTime',
            ExpressionAttributeValues={
                ':pkid': {'S': 'RES#APPO'},
                ':currTime': {'S': currTime}
            }
        )

        table = dynamodbQuery.Table('TuCita247')
        for item in json_dynamodb.loads(details['Items']):
            details = table.delete_item(
                Key={
                    'PKID': item['PKID'],
                    'SKID': item['SKID']
                }
            )

        statusCode = 200
        body = json.dumps({'OnHold': 'Success', 'Code': 200})

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