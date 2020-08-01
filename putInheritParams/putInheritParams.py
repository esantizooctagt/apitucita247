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

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
logger.info("SUCCESS: Connection to DynamoDB succeeded")
    
def lambda_handler(event, context):
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']
        
    try:
        businessId = event['pathParameters']['businessId']
        locationId = event['pathParameters']['locationId']
        providerId = event['pathParameters']['providerId']
        value = event['pathParameters']['value']
        tipo = event['pathParameters']['tipo']

        table = dynamodb.Table('TuCita247')
        if tipo == 1:
            if providerId == '_':
                response = table.update_item(
                    Key={
                        'PKID': 'BUS#' + businessId,
                        'SKID': 'LOC#' + locationId
                    },
                    UpdateExpression="SET PARENTDAYSOFF = :value",
                    ExpressionAttributeValues={':value': value},
                    ReturnValues="UPDATED_NEW"
                )
            else:
                response = table.update_item(
                    Key={
                        'PKID': 'BUS#' + businessId + '#LOC#' + locationId,
                        'SKID': 'PRO#' + providerId
                    },
                    UpdateExpression="SET PARENTDAYSOFF = :value",
                    ExpressionAttributeValues={':value': value},
                    ReturnValues="UPDATED_NEW"
                )
        else:
            if providerId == '_':
                response = table.update_item(
                    Key={
                        'PKID': 'BUS#' + businessId,
                        'SKID': 'LOC#' + locationId
                    },
                    UpdateExpression="SET PARENTHOURS = :value",
                    ExpressionAttributeValues={':value': value},
                    ReturnValues="UPDATED_NEW"
                )
            else:
                response = table.update_item(
                    Key={
                        'PKID': 'BUS#' + businessId + '#LOC#' + locationId,
                        'SKID': 'PRO#' + providerId
                    },
                    UpdateExpression="SET PARENTHOURS = :value",
                    ExpressionAttributeValues={':value': value},
                    ReturnValues="UPDATED_NEW"
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