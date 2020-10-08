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
        country_date = dateutil.tz.gettz('America/Puerto_Rico')
        today = datetime.datetime.now(tz=country_date)
        dateOpe = today.strftime("%Y-%m-%d-00-00")

        dateHoy = dateOpe
        details = dynamodb.query(
            TableName="TuCita247",
            IndexName="TuCita247_CustAppos",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='GSI2PK = :customerId AND GSI2SK >= :dateHoy',
            ExpressionAttributeValues={
                ':customerId': {'S': 'CUS#' + customerId},
                ':dateHoy': {'S': '1#DT#' + dateHoy}
            }
        )
        recordset = {}
        record = []
        for item in json_dynamodb.loads(details['Items']):
            locs = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :businessId AND SKID = :locationId',
                ExpressionAttributeValues={
                    ':businessId': {'S': 'BUS#' + item['GSI1PK'].split('#')[1]},
                    ':locationId': {'S': 'LOC#' + item['GSI1PK'].split('#')[3] }
                },
                Limit = 1
            )
            for locations in json_dynamodb.loads(locs['Items']):
                Address = locations['ADDRESS']

            bus = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :businessId AND SKID = :meta',
                ExpressionAttributeValues={
                    ':businessId': {'S': 'BUS#' + item['GSI1PK'].split('#')[1]},
                    ':meta': {'S': 'METADATA' }
                },
                Limit = 1
            )
            for business in json_dynamodb.loads(bus['Items']):
                Name = business['NAME']

            recordset = {
                'AppointmentId': item['PKID'].replace('APPO#',''),
                'Status': item['STATUS'],
                'Address': Address,
                'NameBusiness': Name,
                'Name': item['NAME'],
                'Phone': item['PHONE'],
                'DateAppo': item['DATE_APPO'],
                'Door': item['DOOR'] if 'DOOR' in item else '',
                'OnBehalf': item['ON_BEHALF'] if 'ON_BEHALF' in item else 0,
                'PeopleQty': item['PEOPLE_QTY'] if 'PEOPLE_QTY' in item else 0,
                'QRCode': item['QRCODE'] if 'QRCODE' in item else '',
                'Disability': item['DISABILITY'] if 'DISABILITY' in item else 0,
                'UnRead': item['UNREAD'] if 'UNREAD' in item else '',
                'Ready': item['READY'] if 'READY' in item else 0
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