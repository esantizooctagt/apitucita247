import sys
import json
import logging
import random

import os

import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr
from dynamodb_json import json_util as json_dynamodb

REGION='us-east-1'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.client('dynamodb', region_name=REGION)
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):
    language = event['pathParameters']['language']

    try:
        response = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :ads',
            ExpressionAttributeValues={
                ':ads': {'S': 'ADS#'}
            }
        )
        recordset = {}
        items = []
        for item in json_dynamodb.loads(response['Items']):
            recordset = {
                'AdsId': item['SKID'].replace('ADS#',''),
                'ImgPath': item['IMAGE_PATH_ESP'] if language == 'es' else item['IMAGE_PATH_ENG']
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
            "access-control-allow-origin" : "*"
        },
        'body' : body
    }
    return response