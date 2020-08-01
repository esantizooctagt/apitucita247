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
    stage = ''
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']

    try:
        businessId = event['pathParameters']['businessId']
        locationId = event['pathParameters']['locationId']
        providerId = event['pathParameters']['providerId']
        record = []
        if locationId == '_':
            response = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :businessId AND begins_with( SKID , :metadata )',
                ExpressionAttributeValues={
                    ':businessId': {'S': 'BUS#' + businessId},
                    ':metadata': {'S': 'METADATA'}
                }
            )
            recordset = {}
            for row in json_dynamodb.loads(response['Items']):
                recordset = {
                    'BusinessId': row['PKID'].replace('BUS#',''),
                    'OperationHours': row['OPERATIONHOURS'] if 'OPERATIONHOURS' in row else ''
                }
                record.append(recordset)

        if providerId == '_' and locationId != '_':
            response = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :businessId AND begins_with( SKID , :locs )',
                ExpressionAttributeValues={
                    ':businessId': {'S': 'BUS#' + businessId},
                    ':locs': {'S': 'LOC#'}
                }
            )
            recordset = {}
            for row in json_dynamodb.loads(response['Items']):
                recordset = {
                    'LocationId': row['SKID'].replace('LOC#',''),
                    'Name': row['NAME'],
                    'OperationHours': row['OPERATIONHOURS'] if 'OPERATIONHOURS' in row else '',
                    'ParentHours': row['PARENTHOURS'] if 'PARENTHOURS' in row else 0
                }
                record.append(recordset)

        if providerId != '_':
            locations = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :businessId AND begins_with( SKID , :metadata )',
                ExpressionAttributeValues={
                    ':businessId': {'S': 'BUS#' + businessId},
                    ':metadata': {'S': 'LOC#'}
                },
            )
            locs = {}
            for location in json_dynamodb.loads(locations['Items']):
                response = dynamodb.query(
                    TableName="TuCita247",
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='PKID = :businessId AND begins_with( SKID , :servs )',
                    ExpressionAttributeValues={
                        ':businessId': {'S': 'BUS#' + businessId + '#' + location['SKID'].replace('LOC#','')},
                        ':servs': {'S': 'PRO#'}
                    }
                )
                recordset = {}
                serv = []
                for row in json_dynamodb.loads(response['Items']):
                    recordset = {
                        'ProviderId': row['SKID'].replace('PRO#',''),
                        'Name': row['NAME'],
                        'OperationHours': row['OPERATIONHOURS'] if 'OPERATIONHOURS' in row else '',
                        'ParentHours': row['PARENTHOURS'] if 'PARENTHOURS' in row else 0
                    }
                    serv.append(recordset)
                locs = {
                    'LocationId': location['SKID'].replace('LOC#',''),
                    'Name': location['NAME'],
                    'Services': serv
                }
                record.append(locs)

        statusCode = 200
        body = json.dumps({'Code': 200, 'Data': record})
    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message':'Error on request try again'+ str(e)})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : cors
        },
        'body' : body
    }
    return response