import sys
import logging
import json

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
    try:
        statusCode = ''
        businessId = event['pathParameters']['businessId']
        locationId = event['pathParameters']['locationId']

        response = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :key01 AND begins_with( SKID , :providers )',
            FilterExpression='#s = :stat',
            ExpressionAttributeNames={'#s': 'STATUS'},
            ExpressionAttributeValues={
                ':key01': {'S': 'BUS#' + businessId + '#LOC#' + locationId},
                ':providers': {'S': 'PRO#'},
                ':stat' : {'N': '1'}
            }
        )
        recordset = {}
        providers = []
        for row in json_dynamodb.loads(response['Items']):
            recordset = {
                'ProviderId': row['SKID'].replace('PRO#',''),
                'Name': row['NAME']
            }
            providers.append(recordset)
        
        statusCode = 200
        body = json.dumps({'Code': 200, 'Providers': providers})
    
        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Code': 500, 'Message': 'Error on load locations by user'})
    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message': 'Error on request try again ' + str(e)})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : "*"
        },
        'body' : body
    }
    
    return response