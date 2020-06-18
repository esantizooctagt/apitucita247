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
        data = json.loads(event['body'])
        
        customerId = data['CustomerId']
        businessId = data['BusinessId']
        locationId = data['LocationId']

        details = dynamodb.put_item(
            TableName="TuCita247",
            ReturnConsumedCapacity="TOTAL",
            Item={
                'PKID': {'S': ':key'},
                'SKID': {'S': ':skey'},
                'BUSID': {'S': ':businessId'},
                'LOCID': {'S': 'locationId'}
            },
            ExpressionAttributeValues={
                ':key': {'S': 'CUS#' + customerId},
                ':skey': {'S': 'FAVS'},
                ':businessId': {'S': businessId},
                ':locationId': {'S': locationId}
            },
            ConditionExpression="attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
            ReturnValues='NONE'
        )

        statusCode = 200
        body = json.dumps({'Message': 'Location added to favorite successfully', 'Code': 200})
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