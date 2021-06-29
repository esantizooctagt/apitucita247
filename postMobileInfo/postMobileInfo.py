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
    
    data = json.loads(event['body'])
    to_number = event['pathParameters']['number']
    
    try:
        details = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :mobile AND begins_with ( SKID , :customer )',
            ExpressionAttributeValues={
                ':mobile': {'S': 'MOB#' + to_number},
                ':customer': {'S': 'CUS#'}
            }
        )
        recordset = {}
        for item in json_dynamodb.loads(details['Items']):
            recordset = {
                'CustomerId': item['SKID'].replace('CUS#',''),
                'Status': item['STATUS'],
                'Name': item['NAME'],
                'Gender': item['GENDER'] if 'GENDER' in item else '',
                'Custom': item['CUSTOM'] if 'CUSTOM' in item else '',
                'Email': item['EMAIL'] if 'EMAIL' in item else '',
                'Preferences': item['PREFERENCES'] if 'PREFERENCES' in item else '',
                'Disability': item['DISABILITY'] if 'DISABILITY' in item else '',
                'DOB': item['DOB'] if 'DOB' in item else '',
                'Mobile': to_number,
                'Language': item['LANGUAGE'] if 'LANGUAGE' in item else 'en'
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
            "access-control-allow-origin" : "*"
        },
        'body' : body
    }
    return response