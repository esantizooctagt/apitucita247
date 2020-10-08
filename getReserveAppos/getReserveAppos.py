import sys
import json
import logging

import os

import datetime
import dateutil.tz
from datetime import timezone

import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr
from dynamodb_json import json_util as json_dynamodb

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.client('dynamodb', region_name=REGION)
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):    
    try:
        customerId = event['pathParameters']['customerId']
        details = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :customerId',
            ExpressionAttributeValues={
                ':customerId': {'S': 'RES#CUS#' + customerId}
            }
        )
        recordset = {}
        record = []
        for item in json_dynamodb.loads(details['Items']):
            recordset = {
                'AppointmentId': item['SKID'].replace('APPO#',''),
                'NameBusiness': item['BUSINESS_NAME'],
                'ServiceName': item['SERVICE_NAME'],
                'Disability': item['DISABILITY'] if 'DISABILITY' in item else 0,
                'PeopleQty': item['PEOPLE_QTY'] if 'PEOPLE_QTY' in item else 0,
                'Address': item['BUSINESS_ADDR'],
                'DateAppo': item['DATE_APPO'],
                'Door': item['DOOR'] if 'DOOR' in item else '',
                'OnBehalf': item['ON_BEHALF'] if 'ON_BEHALF' in item else 0
            }
            record.append(recordset)

        statusCode = 200
        body = json.dumps({'Appointments': record, 'Code': 200})

    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message': str(e), 'Code': 500})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : "*"
        },
        'body' : body
    }
    return response