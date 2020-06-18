import sys
import json
import logging

import os

import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr
from dynamodb_json import json_util as json_dynamodb

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.client('dynamodb', region_name='us-east-1')
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):
    response = ''
    
    try:
        customerId = event['pathParameters']['customerId']
        details = dynamodb.query(
            TableName="TuCita247",
            IndexName="TuCita247_Index",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='GSI1PK = :customerId AND GSI1SK = :customerId',
            ExpressionAttributeValues={
                ':customerId': {'S': 'CUS#' + customerId}
            }
        )
        recordset = {}
        for item in json_dynamodb.loads(details['Items']):
            recordset = {
                'CustomerId': item['SKID'].replace('CUS#',''),
                'Status': item['STATUS'],
                'Name': item['NAME'],
                'Gender': item['GENDER'] if 'GENDER' in item else '',
                'Email': item['EMAIL'] if 'EMAIL' in item else '',
                'Preferences': item['PREFERENCES'] if 'PREFERENCES' in item else '',
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
            "access-control-allow-origin" : "*"
        },
        'body' : body
    }
    return response