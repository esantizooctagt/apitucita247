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

dynamodb = boto3.client('dynamodb', region_name=REGION)
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']
        
    try:
        statusCode = ''
        businessId = event['pathParameters']['id']

        response = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :businessId AND SKID = :reasons',
            ExpressionAttributeValues={
                ':businessId': {'S': 'BUS#'+businessId},
                ':reasons': {'S': 'METADATA'}
            }
        )
        for row in json_dynamodb.loads(response['Items']):
            reasons = row['REASONS']

        statusCode = 200
        body = json.dumps({'Message': 'Get reasons successfully', 'Code': 200, 'Reasons': reasons})

        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Message': 'Error on get reasons', 'Code': 500})
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