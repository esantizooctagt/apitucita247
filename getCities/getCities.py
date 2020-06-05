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

dynamodb = boto3.client('dynamodb', region_name='us-east-1')
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):
    body = ''
    cors = ''
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']

    records =[]
    try:
        country = event['pathParameters']['country']

        response = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :country',
            ExpressionAttributeValues={
                ':country': {'S': 'COUNTRY#' + country}
            }
        )
        for row in json_dynamodb.loads(response['Items']):
            recordset = {
                'CityId': row['SKID'].replace('CITY#',''),
                'Name': row['NAME']
            }
            records.append(recordset)
        
        lastItem = ''
        while 'LastEvaluatedKey' in response:
            lastItem = json_dynamodb.loads(response['LastEvaluatedKey'])

            response = dynamodb.query(
                TableName="TuCita247",
                ExclusiveStartKey= lastItem,
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :country',
                ExpressionAttributeValues={
                    ':country': {'S': 'COUNTRY#' + country}
                }
            )
            for row in json_dynamodb.loads(response['Items']):
                recordset = {
                    'CityId': row['SKID'].replace('CITY#',''),
                    'Name': row['NAME']
                }
                records.append(recordset)
        
        statusCode = 200
        body = json.dumps(records)
    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message': 'Error on request try again ' + str(e)})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "Access-Control-Allow-Origin" : cors
        },
        'body' : body
    }
    return response