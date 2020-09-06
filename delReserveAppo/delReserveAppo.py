import sys
import logging
import json

import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr

import base64

import uuid
import os

REGION = 'us-east-1'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
logger.info("SUCCESS: Connection to DynamoDB succeeded")
    
def lambda_handler(event, context):
    try:
        statusCode = ''
        appointmentId = event['pathParameters']['appointmentId']
        customerId = event['pathParameters']['customerId']
        
        table = dynamodb.Table('TuCita247')
        response = table.delete_item(
            Key={
                'PKID': 'RES#CUS#' + customerId,
                'SKID': 'APPO#' + appointmentId
            },
            ConditionExpression="attribute_exists(PKID) AND attribute_exists(SKID)",
            ReturnValues="NONE"
        )

        statusCode = 200
        body = json.dumps({'Message': 'Reserve deleted successfully', 'Code': 200})

        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Message': 'Error on deleted reserve'})
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