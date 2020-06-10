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
    stage = event['headers']
    cors = stage['origin']

    records =[]
    try:
        country = event['pathParameters']['country']
        language = event['pathParameters']['language']

        response = dynamodb.query(
            TableName="TuCita247",
            IndexName="TuCita247_Index",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='GSI1PK = :country',
            ExpressionAttributeValues={
                ':country': {'S': 'COUNTRY#' + country}
            }
        )
        for row in json_dynamodb.loads(response['Items']):
            recordset = {
                'Parent': row['PKID'].replace('COUNTRY#' + country,''),
                'City': row['SKID'],
                'Name': row['NAME_ENG'] if language == 'EN' else row['NAME_ESP']
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
                KeyConditionExpression='GSI1PK = :country',
                ExpressionAttributeValues={
                    ':country': {'S': 'COUNTRY#' + country}
                }
            )
            for row in json_dynamodb.loads(response['Items']):
                recordset = {
                    'Parent': row['PKID'].replace('COUNTRY#' + country,''),
                    'City': row['SKID'],
                    'Name': row['NAME_ENG'] if language == 'EN' else row['NAME_ESP']
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