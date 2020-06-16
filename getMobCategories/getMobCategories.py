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
    try:
        language = event['pathParameters']['language']

        e = {'#s': 'STATUS'}
        f = '#s = :stat'
        response = dynamodb.query(
            TableName="TuCita247",
            IndexName="TuCita247_Index",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='GSI1PK = :categories AND GSI1SK = :categories',
            ExpressionAttributeNames=e,
            FilterExpression=f,
            ExpressionAttributeValues={
                ':categories': {'S': 'CAT#'},
                ':stat' : {'N': '1'}
            },
        )
        items = json_dynamodb.loads(response['Items'])
        for row in items:
            recordset = {
                'CategoryId': row['PKID'].replace('CAT#',''),
                'Name': row['NAME_ENG'] if language == 'EN' else row['NAME_ESP'],
                'Icon': row['ICON'],
                'Imagen': row['IMG_CAT']
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
            "access-control-allow-origin" : "*"
        },
        'body' : body
    }
    return response