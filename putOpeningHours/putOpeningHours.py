import sys
import logging
import json

import boto3
import botocore.exceptions
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
from dynamodb_json import json_util as json_dynamodb

import datetime
import dateutil.tz
from datetime import timezone

import os

REGION = 'us-east-1'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodbData = boto3.resource('dynamodb', region_name=REGION)
dynamodb = boto3.client('dynamodb', region_name=REGION)
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
        data = json.loads(event['body'])
        opeHours = data['OpeHours']

        table = dynamodbData.Table('TuCita247')
        if businessId != '_' and locationId == '_':
            response = table.update_item(
                Key={
                    'PKID': 'BUS#' + businessId,
                    'SKID': 'METADATA'
                },
                UpdateExpression="SET OPERATIONHOURS = :opeHours",
                ExpressionAttributeValues={
                    ':opeHours': opeHours
                },
                ReturnValues="UPDATED_NEW"
            )

            locs = dynamodb.query(
                TableName='TuCita247',
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :businessId AND begins_with(SKID , :locs)',
                FilterExpression='PARENTHOURS = :parentHours',
                ExpressionAttributeValues={
                    ':businessId': {'S': 'BUS#' + businessId},
                    ':locs': {'S': 'LOC#'},
                    ':parentHours': {'N': str(1)}
                },
            )
            for loc in json_dynamodb.loads(locs['Items']):
                locId = loc['SKID'].replace('LOC#', '')
                servs = dynamodb.query(
                    TableName='TuCita247',
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='PKID = :businessId AND begins_with(SKID , :providerId)',
                    FilterExpression='PARENTHOURS = :parentHours',
                    ExpressionAttributeValues={
                        ':businessId': {'S': 'BUS#' + businessId + '#LOC#' + locId},
                        ':providerId': {'S': 'PRO#'},
                        ':parentHours': {'N': str(1)}
                    },
                )
                for serv in json_dynamodb.loads(servs['Items']):
                    response = table.update_item(
                        Key={
                            'PKID': 'BUS#' + businessId + '#' + locId,
                            'SKID': serv['SKID']
                        },
                        UpdateExpression="SET OPERATIONHOURS = :opeHours",
                        ExpressionAttributeValues={':opeHours': opeHours},
                        ReturnValues="UPDATED_NEW"
                    )
                response = table.update_item(
                    Key={
                        'PKID': 'BUS#' + businessId,
                        'SKID': 'LOC#' + locId
                    },
                    UpdateExpression="SET OPERATIONHOURS = :opeHours",
                    ExpressionAttributeValues={':opeHours': opeHours},
                    ReturnValues="UPDATED_NEW"
                )

        if locationId != '_' and providerId == '_':
            response = table.update_item(
                Key={
                    'PKID': 'BUS#' + businessId,
                    'SKID': 'LOC#' + locationId
                },
                UpdateExpression="SET OPERATIONHOURS = :opeHours",
                ExpressionAttributeValues={
                    ':opeHours': opeHours
                },
                ReturnValues="UPDATED_NEW"
            )
            servs = dynamodb.query(
                TableName='TuCita247',
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :businessId AND begins_with(SKID , :providerId)',
                FilterExpression='PARENTHOURS = :parentHours',
                ExpressionAttributeValues={
                    ':businessId': {'S': 'BUS#' + businessId + '#LOC#' + locationId},
                    ':providerId': {'S': 'PRO#'},
                    ':parentHours': {'N': str(1)}
                },
            )
            for serv in json_dynamodb.loads(servs['Items']):
                response = table.update_item(
                    Key={
                        'PKID': 'BUS#' + businessId + '#' + locationId,
                        'SKID': serv['SKID']
                    },
                    UpdateExpression="SET OPERATIONHOURS = :opeHours",
                    ExpressionAttributeValues={':opeHours': opeHours},
                    ReturnValues="UPDATED_NEW"
                )

        if providerId != '_':
            response = table.update_item(
                Key={
                    'PKID': 'BUS#' + businessId + '#LOC#' + locationId,
                    'SKID': 'PRO#' + providerId
                },
                UpdateExpression="SET OPERATIONHOURS = :opeHours",
                ExpressionAttributeValues={':opeHours': opeHours},
                ReturnValues="UPDATED_NEW"
            )

        statusCode = 200
        body = json.dumps({'Message': 'Opening hours updated successfully', 'Code': 200})

        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Message': 'Error on update special day', 'Code': 500})
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