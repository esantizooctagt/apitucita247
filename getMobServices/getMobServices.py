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
    try:
        statusCode = ''
        businessId = event['pathParameters']['businessId']
        providerId = event['pathParameters']['providerId']

        response = dynamodb.query(
            TableName="TuCita247",
            IndexName="TuCita247_Index",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='GSI1PK = :key01 AND begins_with( GSI1SK , :services )',
            ExpressionAttributeValues={
                ':key01': {'S': 'BUS#' + businessId + '#PRO#' + providerId},
                ':services': {'S': 'SER#'}
            }
        )
        recordset = {}
        services = []
        e = {'#s': 'STATUS'}
        f = '#s = :stat'
        for row in json_dynamodb.loads(response['Items']):
            serv = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :key01 AND SKID = :service',
                ExpressionAttributeValues={
                    ':key01': {'S': 'BUS#' + businessId},
                    ':service': {'S': row['GSI1SK']},
                    ':stat' : {'N': '1'}
                },
                ExpressionAttributeNames=e,
                FilterExpression=f,
            )
            for item in json_dynamodb.loads(serv['Items']):
                recordset = {
                    'ServiceId': item['SKID'].replace('SER#',''),
                    'Name': item['NAME']
                }
                services.append(recordset)
        
        statusCode = 200
        body = json.dumps({'Code': 200, 'Services': services})
    
        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Code': 500, 'Message': 'Error on load locations by user'})
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