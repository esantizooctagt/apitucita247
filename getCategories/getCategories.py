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
    records =[]
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']
        
    try:
        language = event['pathParameters']['language']
        e = {'#s': 'STATUS'}
        f = '#s = :stat'
        response = dynamodb.query(
            TableName="TuCita247",
            IndexName="TuCita247_Index",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='GSI1PK = :key AND begins_with( GSI1SK , :categories )',
            ExpressionAttributeNames=e,
            FilterExpression=f,
            ExpressionAttributeValues={
                ':key': {'S': 'CAT#'},
                ':categories': {'S': 'CAT#'},
                ':stat' : {'N': '1'}
            },
        )
        for row in json_dynamodb.loads(response['Items']):
            recordset = {
                'CategoryId': row['PKID']+'#'+row['SKID'] if row['PKID'] != row['SKID'] else row['PKID'],
                'Name': row['NAME_ENG'] if language.upper() == 'EN' else row['NAME_ESP']
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
            "access-control-allow-origin" : cors
        },
        'body' : body
    }
    return response