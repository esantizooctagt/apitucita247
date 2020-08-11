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

dynamodb = boto3.client('dynamodb', region_name='us-east-1')
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']

    records =[]
    try:
        businessId = event['pathParameters']['businessId']

        response = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :businessId AND begins_with ( SKID, :services )',
            ExpressionAttributeNames={'#s': 'STATUS'},
            ExpressionAttributeValues={':businessId': {'S': 'BUS#' + businessId}, ':stat': {'N': '2'}, ':services': {'S':'SER#'}},
            FilterExpression='#s < :stat'
        )
        recordset = {}
        for row in json_dynamodb.loads(response['Items']):
            recordset = {
                'ServiceId': row['SKID'].replace('SER#',''),
                'Name': row['NAME'],
                'Color': row['COLOR']
            }
            records.append(recordset)

        lastItem = ''
        recordset = {}
        while 'LastEvaluatedKey' in response:
            lastItem = json_dynamodb.loads(response['LastEvaluatedKey'])

            serv = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                ExclusiveStartKey= lastItem,
                KeyConditionExpression='PKID = :businessId AND begins_with ( SKID, :services )',
                ExpressionAttributeNames={'#s': 'STATUS'},
                ExpressionAttributeValues={':businessId': {'S': 'BUS#' + businessId}, ':stat': {'N': '2'}, ':services': {'S':'SER#'}},
                FilterExpression='#s < :stat'
            )
            recordset = {}
            for row in json_dynamodb.loads(serv['Items']):
                recordset = {
                    'ServiceId': row['SKID'].replace('SER#',''),
                    'Name': row['NAME'],
                    'Color': row['COLOR']
                }
                records.append(recordset)
            
        statusCode = 200
        body = json.dumps(records)
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