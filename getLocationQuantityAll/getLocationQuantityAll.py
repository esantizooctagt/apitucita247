import sys
import logging
import json

import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr
from dynamodb_json import json_util as json_dynamodb

import datetime
import dateutil.tz
from datetime import timezone

from decimal import *

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

        if locationId == '_':
            response = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :businessId AND begins_with ( SKID , :locationId )',
                ExpressionAttributeValues={
                    ':businessId': {'S': 'BUS#' + businessId},
                    ':locationId': {'S': 'LOC#'}
                }
            )
        else:
            response = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :businessId AND SKID = :locationId',
                ExpressionAttributeValues={
                    ':businessId': {'S': 'BUS#' + businessId},
                    ':locationId': {'S': 'LOC#' + locationId}
                }
            )

        qtyPeople = 0
        totLocation = 0
        data = []
        for row in json_dynamodb.loads(response['Items']):
            items = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :key01 AND begins_with(SKID, :provs)',
                ExpressionAttributeValues={
                    ':key01': {'S': 'BUS#' + businessId + '#' + row['SKID']},
                    ':provs': {'S': 'PRO#'}
                }
            )
            services = []
            for item in json_dynamodb.loads(items['Items']):
                recordset = {
                    'Quantity': row['PEOPLE_CHECK_IN'] if 'PEOPLE_CHECK_IN' in row else 0,
                    'TotLocation': row['MAX_CUSTOMER'] if 'MAX_CUSTOMER' in row else 0,
                    'PerLocation': ((row['PEOPLE_CHECK_IN'] if 'PEOPLE_CHECK_IN' in row else 0)/(row['MAX_CUSTOMER'] if 'MAX_CUSTOMER' in row else 0))*100,
                    'ProviderId': item['SKID'].replace('PRO#',''),
                    'Name': item['NAME']
                }
                services.append(recordset)
            
            record = {
                'Name': row['NAME'],
                'LocationId': row['SKID'].replace('CUS#',''),
                'Services': services,
            }
            data.append(record)

        resultSet = { 
            'Code': 200,
            'Data': data
        }
        statusCode = 200
        body = json.dumps(resultSet)
    
        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Code': 500, 'Message': 'Error on load number of people in location'})
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