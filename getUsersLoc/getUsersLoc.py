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

        e = {'#s': 'STATUS'}
        a = {':businessId': {'S': 'BUS#' + businessId}, ':stat': {'N': '2'}, ':users': {'S':'USER#'}}
        f = '#s < :stat'

        response = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :businessId AND begins_with ( SKID, :users )',
            ExpressionAttributeNames=e,
            ExpressionAttributeValues=a,
            FilterExpression=f
        )

        recordset ={}
        for row in json_dynamodb.loads(response['Items']):
            recordset = {
                'UserId': row['USERID'],
                'First_Name': row['FIRST_NAME'],
                'Last_Name': row['LAST_NAME'],
                'Email': row['GSI1PK'].replace('EMAIL#',''),
                'LocationId': row['LOCATIONID'],
                'Door': row['DOOR'],
            }
            records.append(recordset)
            
        lastItem = ''
        while 'LastEvaluatedKey' in response:
            lastItem = json_dynamodb.loads(response['LastEvaluatedKey'])
            lastItem = lastItem['SKID'].replace('USER#','')

            response = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                ExclusiveStartKey= lastItem,
                KeyConditionExpression='PKID = :businessId AND begins_with ( SKID, :users )',
                ExpressionAttributeNames=e,
                ExpressionAttributeValues=a,
                FilterExpression=f
            )

            for row in json_dynamodb.loads(response['Items']):
                recordset = {
                    'UserId': row['USERID'],
                    'First_Name': row['FIRST_NAME'],
                    'Last_Name': row['LAST_NAME'],
                    'Email': row['GSI1PK'].replace('EMAIL#',''),
                    'LocationId': row['LOCATIONID'],
                    'Door': row['DOOR'],
                }
                records.append(recordset)

        resultSet = { 
            'users': records
        }
        statusCode = 200
        body = json.dumps(resultSet)
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