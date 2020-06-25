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

dynamodb = boto3.client('dynamodb', region_name='us-east-1')
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):
    records =[]
    try:
        customerId = event['pathParameters']['customerId']
        locationId = event['pathParameters']['locationId']
        Favorite = 0
        response = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :customerId AND SKID = :locationId',
            ExpressionAttributeValues={
                ':customerId': {'S': 'CUS#' + customerId},
                ':locationId': {'S': 'FAVS#' + locationId}
            },
        )
        for row in json_dynamodb.loads(response['Items']):
            Favorite = 1
            
        statusCode = 200
        body = json.dumps({'Favorite': Favorite, 'Code': 200})
    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message': 'Error on request try again ' + str(e)})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : "*"
        },
        'body' : body
    }
    return response