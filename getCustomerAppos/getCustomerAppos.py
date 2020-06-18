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

dynamodb = boto3.client('dynamodb', region_name='us-east-1')
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):    
    try:
        customerId = event['pathParameters']['customerId']
        country_date = dateutil.tz.gettz('America/Puerto_Rico')
        today = datetime.datetime.now(tz=country_date)
        dateOpe = today.strftime("%Y-%m-%d-00-00")

        dateHoy = ''
        details = dynamodb.query(
            TableName="TuCita247",
            IndexName="TuCita247_CustAppos",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='GSI2PK = :customerId AND GSI2SK BETWEEN :dateHoy AND :excludeCancel',
            ExpressionAttributeValues={
                ':customerId': {'S': 'CUS#' + customerId},
                ':dateHoy': {'S': '1#DT#' + dateHoy},
                ':excludeCancel': {'S': '5#'}
            }
        )
        recordset = {}
        record = []
        for item in json_dynamodb.loads(details['Items']):
            recordset = {
                'AppointmentId': item['PKID'].replace('APPO#',''),
                'Status': item['STATUS'],
                'Name': item['NAME'],
                'Phone': item['PHONE'],
                'DateAppo': item['DATE_APPO'],
                'Door': item['DOOR'] if 'DOOR' in item else '',
                'OnBehalf': item['ON_BEHALF'] if 'ON_BEHALF' in item else 0,
                'PeopleQty': item['PEOPLE_QTY'] if 'PEOPLE_QTY' in item else 0,
                'QRCode': item['QRCODE'] if 'QRCODE' in item else '',
                'Disability': item['DISABILITY'] if 'DISABILITY' in item else 0,
                'UnRead': item['UNREAD'] if 'UNREAD' in item else ''
            }
            record.append(recordset)

        statusCode = 200
        body = json.dumps({'Appointments': recordset, 'Code': 200})
        
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