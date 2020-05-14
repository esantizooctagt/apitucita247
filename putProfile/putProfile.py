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

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']
        
    try:
        statusCode = ''
        data = json.loads(event['body'])
        userId = event['pathParameters']['id']
        businessId = data['BusinessId']

        table = dynamodb.Table('TuCita247')
        response = table.update_item(
            Key={
                'PKID': 'BUS#' + businessId,
                'SKID': 'EMAIL#' + userId
            },
            UpdateExpression="set FIRST_NAME = :firstName, LAST_NAME = :lastName, PHONE = :phone, MFACT_AUTH = :factor, LANGUAGE = :language",
            ExpressionAttributeValues={
                ':firstName': data['First_Name'],
                ':lastName': data['Last_Name'],
                ':phone': data['Phone'],
                ':factor': data['MFact_Auth'],
                ':language': data['Language']
            }
            # ReturnValues="UPDATED_NEW"
        )
        statusCode = 200
        body = json.dumps({'Message': 'User updated successfully'})

        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Message': 'Error on update user'})
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