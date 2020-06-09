import sys
import json
import logging
import random

import os

import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr
from dynamodb_json import json_util as json_dynamodb

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.client('dynamodb', region_name='us-east-1')
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):
    stage = event['headers']
    if stage['origin'] != "http://localhost:8100":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']
    
    try:
        details = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :ads',
            ExpressionAttributeValues={
                ':ads': {'S': 'ADS#'}
            }
        )
        recordset = {}
        items = []
        for item in json_dynamodb.loads(details['Items']):
            recordset = {
                'AdsId': item['SKID'].replace('ADS#',''),
                'ImgPath': item['IMAGE_PATH']
            }
            items.append(recordset)

        statusCode = 200
        body = json.dumps({'Ads': items, 'Code': 200})
    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message': str(e), 'Code': 500})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : cors
        },
        'body' : body
    }
    return response