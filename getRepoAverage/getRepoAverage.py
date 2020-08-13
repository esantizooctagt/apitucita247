import sys
import logging
import json

import datetime
import dateutil.tz
from datetime import timezone, timedelta

import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr
from dynamodb_json import json_util as json_dynamodb

import os

REGION = 'us-east-1'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

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
        dateIni = event['pathParameters']['dateIni']
        dateFin = event['pathParameters']['dateFin']
        lastItem = event['pathParameters']['lastItem']

        if lastItem == '_':
            if locationId == '_':
                response = dynamodb.query(
                    TableName="TuCita247",
                    IndexName="TuCita247_Index",
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='GSI1PK = :businessId AND GSI1SK between :dateIni AND :dateFin',
                    ExpressionAttributeValues={
                        ':businessId': {'S': 'BUS#' + businessId},
                        ':dateIni': {'S': dateIni},
                        ':dateFin': {'S': dateFin}
                    },
                    Limit=10
                )
            if providerId == '_' and locationId != '_':
                response = dynamodb.query(
                    TableName="TuCita247",
                    IndexName="TuCita247_CustAppos",
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='GSI2PK = :key AND GSI2SK between :dateIni AND :dateFin',
                    ExpressionAttributeValues={
                        ':key': {'S': 'BUS#' + businessId + '#LOC#' + locationId},
                        ':dateIni': {'S': dateIni},
                        ':dateFin': {'S': dateFin}
                    },
                    Limit=10
                )
            if providerId != '_':
                response = dynamodb.query(
                    TableName="TuCita247",
                    IndexName="TuCita247_Appos",
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='GSI3PK = :key AND GSI3SK between :dateIni AND :dateFin',
                    ExpressionAttributeValues={
                        ':key': {'S': 'BUS#' + businessId + '#LOC#' + locationId + '#PRO#' + providerId},
                        ':dateIni': {'S': dateIni},
                        ':dateFin': {'S': dateFin}
                    },
                    Limit=10
                )
        else:
            if locationId == '_':
                response = dynamodb.query(
                    TableName="TuCita247",
                    IndexName="TuCita247_Index",
                    ExclusiveStartKey= lastItem,
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='GSI1PK = :businessId AND GSI1SK between :dateIni AND :dateFin',
                    ExpressionAttributeValues={
                        ':businessId': {'S': 'BUS#' + businessId},
                        ':dateIni': {'S': dateIni},
                        ':dateFin': {'S': dateFin}
                    },
                    Limit=10
                )
            if providerId == '_' and locationId != '_':
                response = dynamodb.query(
                    TableName="TuCita247",
                    IndexName="TuCita247_CustAppos",
                    ExclusiveStartKey= lastItem,
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='GSI2PK = :key AND GSI2SK between :dateIni AND :dateFin',
                    ExpressionAttributeValues={
                        ':key': {'S': 'BUS#' + businessId + '#LOC#' + locationId},
                        ':dateIni': {'S': dateIni},
                        ':dateFin': {'S': dateFin}
                    },
                    Limit=10
                )
            if providerId != '_':
                response = dynamodb.query(
                    TableName="TuCita247",
                    IndexName="TuCita247_Appos",
                    ExclusiveStartKey= lastItem,
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='GSI3PK = :key AND GSI3SK between :dateIni AND :dateFin',
                    ExpressionAttributeValues={
                        ':key': {'S': 'BUS#' + businessId + '#LOC#' + locationId + '#PRO#' + providerId},
                        ':dateIni': {'S': dateIni},
                        ':dateFin': {'S': dateFin}
                    },
                    Limit=10
                )

        record = []
        recordset = {}
        for row in json_dynamodb.loads(response['Items']):
            recordset = {
                'Date': row['SKID'].replace('DT#',''),
                'Qty': row['QTY_APPOS'],
                'Time': row['TIME_APPO']
            }
            record.append(recordset)
        
        if 'LastEvaluatedKey' in response:
            lastItem = json_dynamodb.loads(response['LastEvaluatedKey'])

        statusCode = 200
        body = json.dumps({'Result': record,'lastItem': lastItem, 'Code': 200})

        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Message': 'No data for this service provider', 'Code': 500})
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