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
    body = ''
    cors = ''
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']

    try:
        response = dynamodb.query(
            TableName="TuCita247",
            IndexName="TuCita247_Rep01",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='GSI5PK = :metadata AND begins_with( GSI5SK , :businessId )',
            ExpressionAttributeValues={
                ':metadata': {'S': 'METADATA'},
                ':businessId': {'S': 'BUS#'}
            }
        )
        
        recordset = {}
        record = []
        for row in json_dynamodb.loads(response['Items']):
            recordset = {
                'BusinessId': row['PKID'].replace('BUS#',''),
                'Name': row['NAME']
            }
            record.append(recordset)
            
        statusCode = 200
        body = json.dumps({'Code': 200, 'Business': record})
    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message':'Error on request try again'+ str(e)})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : cors
        },
        'body' : body
    }
    return response