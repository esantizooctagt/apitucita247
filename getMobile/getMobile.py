import sys
import json
import logging

import os

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
    response = ''
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']

    try:
        mobile = event['pathParameters']['mobile']
        details = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :mobile AND begins_with(SKID, :customer)',
            ExpressionAttributeValues={
                ':customer': {'S': 'CUS#'},
                ':mobile': {'S': 'MOB#'+mobile}
            }
        )
        recordset = {}
        for item in json_dynamodb.loads(details['Items']):
            recordset = {
                'CustomerId': item['SKID'].replace('CUS#',''),
                'Status': item['STATUS'],
                'Name': item['NAME'],
                'Gender': item['GENDER'] if 'GENDER' in item else '',
                'Email': item['EMAIL_COMM'] if 'EMAIL_COMM' in item else item['EMAIL'] if 'EMAIL' in item else '',
                'Preferences': item['PREFERENCES'] if 'PREFERENCES' in item else 0,
                'Disability': item['DISABILITY'] if 'DISABILITY' in item else '',
                'DOB': item['DOB'] if 'DOB' in item else ''
            }

        statusCode = 200
        body = json.dumps({'Customer': recordset, 'Code': 200})
    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message': str(e), 'Code': 500})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : cors
        },
        'body' : body
    }
    return response