import sys
import logging
import json

import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr

import os

REGION = 'us-east-1'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb', region_name=REGION)
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):
    try:
        statusCode = ''
        mobile = event['pathParameters']['mobile']
        customerId = event['pathParameters']['customerId']
        language = event['pathParameters']['language']

        table = dynamodb.Table('TuCita247')
        response = table.update_item(
            Key={
                'PKID': 'MOB#' + mobile,
                'SKID': 'CUS#' + customerId
            },
            UpdateExpression="SET #l = :language",
            ExpressionAttributeNames={'#l': 'LANGUAGE'},
            ExpressionAttributeValues={':language': language},
            ConditionExpression="attribute_exists(PKID) AND attribute_exists(SKID)"
        )
        statusCode = 200
        body = json.dumps({'Message': 'User edited successfully', 'Code': 200})
        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Message': 'Error on edit user', 'Code': 400})
    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message': 'Error on request try again ' + str(e), 'Code': 400})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : "*"
        },
        'body' : body
    }
    return response