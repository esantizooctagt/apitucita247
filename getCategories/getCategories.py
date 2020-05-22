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
    records =[]
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']
        
    try:
        e = {'#s': 'STATUS'}
        f = '#s = :stat'
        category = event['pathParameters']['name']
        response = dynamodb.query(
            TableName="TuCita247",
            IndexName="TuCita247_Index",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='GSI1PK = :categories AND begins_with( GSI1SK, :name )',
            ExpressionAttributeNames=e,
            FilterExpression=f,
            ExpressionAttributeValues={
                ':businessId': {'S': 'PARAM#CAT'},
                ':name': {'S': category },
                ':stat' : {'N': '1'}
            },
            Limit=5
        )
        items = json_dynamodb.loads(response['Items'])
        for row in items:
            recordset = {
                'CategoryId': row['PKID'].replace('CAT#',''),
                'Name': row['NAME']
            }
            records.append(recordset)
        
        statusCode = 200
        body = json.dumps(records)
    except: #Exception as e:
        statusCode = 500
        body = json.dumps({'Message': 'Error on request try again'})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : cors
        },
        'body' : body
    }
    return response