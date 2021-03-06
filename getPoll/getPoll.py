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
        pollId = event['pathParameters']['pollId']

        master = dynamodb.query(
            TableName="TuCita247",
            IndexName="TuCita247_Index",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='GSI1PK = :polls AND GSI1SK = :polls',
            ExpressionAttributeValues={
                ':polls': {'S': 'POLL#' + pollId}
            },
            Limit =1
        )
        recordset = ''
        for item in json_dynamodb.loads(master['Items']):
            recordset = {
                'PollId': item['GSI1SK'].replace('POLL#',''),
                'Name': item['NAME'],
                'LocationId': item['LOCATIONID'],
                'DatePoll': item['DATE_POLL'],
                'DateFinPoll': item['DATE_FIN_POLL'],
                'Status': int(item['STATUS']),
                'Happy': item['HAPPY'],
                'Neutral': item['NEUTRAL'],
                'Angry': item['ANGRY']
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