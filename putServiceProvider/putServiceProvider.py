import sys
import logging
import json

import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr
from dynamodb_json import json_util as json_dynamodb

import base64

import uuid
import os

REGION = 'us-east-1'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.client('dynamodb', region_name='us-east-1')
logger.info("SUCCESS: Connection to DynamoDB succeeded")
    
def lambda_handler(event, context):
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']
        
    try:
        businessId = event['pathParameters']['businessId']
        serviceId = event['pathParameters']['serviceId']
        providerId = event['pathParameters']['providerId']
        activo = int(event['pathParameters']['activo'])

        if activo == 1:
            response = dynamodb.put_item(
                TableName="TuCita247",
                ReturnConsumedCapacity="TOTAL",
                Item={
                    'PKID': {'S': 'BUS#' + businessId + '#SER#' + serviceId},
                    'SKID': {'S': 'PRO#' + providerId},
                    'GSI1PK': {'S': 'BUS#' + businessId + '#PRO#' + providerId},
                    'GSI1SK': {'S': 'SER#' + serviceId }
                },
                ConditionExpression="attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                ReturnValues='NONE'
            )
        else:
            response = dynamodb.delete_item(
                TableName="TuCita247",
                ReturnConsumedCapacity="TOTAL",
                Key={
                    'PKID': {'S': 'BUS#' + businessId + '#SER#' + serviceId },
                    'SKID': {'S': 'PRO#' + providerId }
                },
                ReturnValues='NONE'
            )
        statusCode = 200
        body = json.dumps({'Message': 'Update data successfully', 'Code': 200})
    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message': 'Error on request try again ' + str(e)})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : cors
        },
        'body' : body
    }
    return response