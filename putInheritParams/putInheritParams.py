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

dynamodb = boto3.resource('dynamodb', region_name=REGION)
dynamodbQuery = boto3.client('dynamodb', region_name=REGION)
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
        value = int(event['pathParameters']['value'])
        tipo = int(event['pathParameters']['tipo'])

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
                if value == 0:
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
                    business = dynamodbQuery.query(
                        TableName="TuCita247",
                        ReturnConsumedCapacity='TOTAL',
                        KeyConditionExpression='PKID = :businessId AND SKID = :metadata',
                        ExpressionAttributeValues={
                            ':businessId': {'S': 'BUS#' + businessId},
                            ':metadata': {"S": 'METADATA'}
                        }
                    )
                    opeHours = ''
                    for bus in json_dynamodb.loads(business['Items']):
                        opeHours = bus['OPERATIONHOURS']

                    response = table.update_item(
                        Key={
                            'PKID': 'BUS#' + businessId,
                            'SKID': 'LOC#' + locationId
                        },
                        UpdateExpression="SET PARENTHOURS = :value, OPERATIONHOURS = :opeHours",
                        ExpressionAttributeValues={':value': value, ':opeHours': opeHours},
                        ReturnValues="UPDATED_NEW"
                    )
            else:
                if value == 0:
                    response = table.update_item(
                        Key={
                            'PKID': 'BUS#' + businessId + '#LOC#' + locationId,
                            'SKID': 'PRO#' + providerId
                        },
                        UpdateExpression="SET PARENTHOURS = :value",
                        ExpressionAttributeValues={':value': value},
                        ReturnValues="UPDATED_NEW"
                    )
                else:
                    locs = dynamodbQuery.query(
                        TableName="TuCita247",
                        ReturnConsumedCapacity='TOTAL',
                        KeyConditionExpression='PKID = :businessId AND SKID = :metadata',
                        ExpressionAttributeValues={
                            ':businessId': {'S': 'BUS#' + businessId},
                            ':metadata': {"S": 'LOC#' + locationId}
                        }
                    )
                    opeHours = ''
                    for loc in json_dynamodb.loads(locs['Items']):
                        opeHours = loc['OPERATIONHOURS']

                    response = table.update_item(
                        Key={
                            'PKID': 'BUS#' + businessId + '#LOC#' + locationId,
                            'SKID': 'PRO#' + providerId
                        },
                        UpdateExpression="SET PARENTHOURS = :value, OPERATIONHOURS = :opeHours",
                        ExpressionAttributeValues={':value': value, ':opeHours': opeHours},
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