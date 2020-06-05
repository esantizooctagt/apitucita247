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
        response = dynamodb.query(
            TableName="TuCita247",
            IndexName="TuCita247_Index",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :business',
            ExpressionAttributeValues={
                ':business': {'S': 'PARENT#BUS'}
            }
        )
        for row in json_dynamodb.loads(response['Items']):
            recordset = {
                'BusinessId': row['GSI1SK'][len(row['GSI1SK'])-row['GSI1SK'].index('#', beg = 0, end = len(row['GSI1SK']))+1:],
                'Name': row['GSI1SK'][0:row['GSI1SK'].index('#', beg = 0, end = len(row['GSI1SK']))+1]
            }
            records.append(recordset)
        
        lastItem = ''
        while 'LastEvaluatedKey' in response:
            lastItem = json_dynamodb.loads(response['LastEvaluatedKey'])

            response = dynamodb.query(
                TableName="TuCita247",
                IndexName="TuCita247_Index",
                ExclusiveStartKey= lastItem,
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :business',
                ExpressionAttributeValues={
                    ':business': {'S': 'PARENT#BUS'}
                }
            )
            for row in json_dynamodb.loads(response['Items']):
                recordset = {
                    'BusinessId': row['GSI1SK'][len(row['GSI1SK'])-row['GSI1SK'].index('#', beg = 0, end = len(row['GSI1SK']))+1:],
                    'Name': row['GSI1SK'][0:row['GSI1SK'].index('#', beg = 0, end = len(row['GSI1SK']))+1]
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