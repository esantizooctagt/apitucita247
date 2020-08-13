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
                    IndexName="TuCita247_Rep01",
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='GSI5PK = :businessId AND GSI5SK between :dateIni AND :dateFin',
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
                    IndexName="TuCita247_Rep02",
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='GSI6PK = :key AND GSI6SK between :dateIni AND :dateFin',
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
                    IndexName="TuCita247_Rep03",
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='GSI7PK = :key AND GSI7SK between :dateIni AND :dateFin',
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
                    IndexName="TuCita247_Rep01",
                    ExclusiveStartKey= lastItem,
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='GSI5PK = :businessId AND GSI5SK between :dateIni AND :dateFin',
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
                    IndexName="TuCita247_Rep02",
                    ExclusiveStartKey= lastItem,
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='GSI6PK = :key AND GSI6SK between :dateIni AND :dateFin',
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
                    IndexName="TuCita247_Rep03",
                    ExclusiveStartKey= lastItem,
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='GSI7PK = :key AND GSI7SK between :dateIni AND :dateFin',
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
            priority = ''
            if 'DISABILITY' in row:
                priority = 'Senior' if row['DISABILITY'] == 1 else 'Pregnant' if row['DISABILITY'] == 2 else 'Disability'

            recordset = {
                'BookId': row['PKID'].replace('APPO#',''),
                'Name': row['NAME'],
                'Phone': row['PHONE'],
                'Door': row['DOOR'],
                'Qty': row['PEOPLE_QTY'],
                'Type': 'Auto' if row['TYPE'] == 1 else 'Walk-In',
                'Date': str(row['DATE_APPO'])[0:10],
                'CheckIn': str(row['TIMECHECKIN'])[0:10] if 'TIMECHECKIN' in row else '',
                'CheckOut': str(row['TIMECHECKOUT'])[0:10] if 'TIMECHECKOUT' in row else '',
                'Priority': priority
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