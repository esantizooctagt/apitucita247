import sys
import logging
import json

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
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']

    try:
        serviceId = event['pathParameters']['serviceId']
        businessId = event['pathParameters']['businessId']

        provider = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :businessId AND SKID = :serviceId',
            ExpressionAttributeValues={
                ':businessId': {'S': 'BUS#' + businessId},
                ':serviceId': {'S': 'SER#' + serviceId}
            },
            Limit =1
        )
        recordset = ''
        for item in json_dynamodb.loads(provider['Items']):
            recordset = {
                'ServiceId': item['SKID'].replace('SER#',''),
                'Name': item['NAME'],
                'TimeService': item['TIME_SERVICE'],
                'BufferTime': item['BUFFER_TIME'] if 'BUFFER_TIME' in item else 0,
                'CustomerPerTime': item['CUSTOMER_PER_TIME'],
                'CustomerPerBooking': item['CUSTOMER_PER_BOOKING'],
                'Color': item['COLOR'],
                'Status': int(item['STATUS'])
            }
    
        statusCode = 200
        body = json.dumps(recordset)

        if statusCode == '':
            statusCode = 404
            body = json.dumps({"Message": "No more rows", "Code": 404})
    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message': 'Error on request try again ' +str(e)})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : cors
        },
        'body' : body
    }
    return response