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
        items = event['pathParameters']['items']
        lastItem = event['pathParameters']['lastItem']
        salir = 0

        e = {'#s': 'STATUS'}
        a = {':businessId': {'S': 'BUS#' + businessId}, ':stat': {'N': '2'}, ':users': {'S':'USER#'}}
        f = '#s < :stat'

        if lastItem == '_':
            lastItem = ''
        else:
            if lastItem == '':
                salir = 1
            else:
                lastItem = {'PKID': {'S': 'BUS#' + businessId },'SKID': {'S': 'USER#' + lastItem }}

        if salir == 0:
            if lastItem == '':
                response = dynamodb.query(
                    TableName="TuCita247",
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='PKID = :businessId AND begins_with ( SKID, :users )',
                    ExpressionAttributeNames=e,
                    ExpressionAttributeValues=a,
                    FilterExpression=f,
                    Limit=items
                )
            else:
                response = dynamodb.query(
                    TableName="TuCita247",
                    ReturnConsumedCapacity='TOTAL',
                    ExclusiveStartKey= lastItem,
                    KeyConditionExpression='PKID = :businessId AND begins_with ( SKID, :users )',
                    ExpressionAttributeNames=e,
                    ExpressionAttributeValues=a,
                    FilterExpression=f,
                    Limit=items
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

            if 'LastEvaluatedKey' in response:
                lastItem = json_dynamodb.loads(response['LastEvaluatedKey'])
                lastItem = lastItem['SKID'].replace('USER#','')

            resultSet = { 
                'lastItem': lastItem,
                'users': records
            }
        
            statusCode = 200
            body = json.dumps(resultSet)
        else:
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