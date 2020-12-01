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

REGION = 'us-east-1'

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
        # RES#BUS#12345#LOC#9065a9e82c914582a0f75b63dfcbab01#PRO#10001
        for item in json_dynamodb.loads(details['Items']):
            businessId = item['GSI1PK'].split('#')[2]
            locationId = item['GSI1PK'].split('#')[4]
            providerId = item['GSI1PK'].split('#')[6]

            provs = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :busLoc AND begins_with(SKID, :prov)',
                ExpressionAttributeValues={
                    ':busLoc': {'S': 'BUS#' + businessId + '#LOC#' + locationId},
                    ':prov': {'S': 'PRO#'}
                }
            )
            count = 0
            for prov in json_dynamodb.loads(provs['Items']):
                count = count + 1
                if prov['SKID'].replace('PRO#','') == providerId:
                    provName = prov['NAME']
            if count == 1:
                provName = ''

            servs = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :businessId AND begins_with(SKID, :serv)',
                ExpressionAttributeValues={
                    ':businessId': {'S': 'BUS#' + businessId},
                    ':serv': {'S': 'SER#'}
                }
            )
            count = 0
            for serv in json_dynamodb.loads(servs['Items']):
                count = count + 1

            if count == 1:
                servName = ''
            else:
                servName = item['SERVICE_NAME']

            recordset = {
                'AppointmentId': item['SKID'].replace('APPO#',''),
                'NameBusiness': item['BUSINESS_NAME'],
                'Name': item['NAME'],
                'ServiceName': servName,
                'ProviderName': provName,
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