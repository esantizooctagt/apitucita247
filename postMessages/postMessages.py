import sys
import logging
import json

import boto3
import botocore.exceptions
from botocore.exceptions import ClientError
from dynamodb_json import json_util as json_dynamodb

import os

REGION = 'us-east-1'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.client('dynamodb', region_name=REGION)
api_client = boto3.client('apigatewaymanagementapi', endpoint_url='https://1wn0vx0tva.execute-api.us-east-1.amazonaws.com/prod')
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):
    try:
        businessId = ''
        businessId = event['BusinessId'] if 'BusinessId' in event else ''
        if businessId != '':
            toConnections = dynamodb.query(
                TableName="Messages",
                IndexName="Messages_01",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='GSI1PK = :businessId AND begins_with(GSI1SK, :connection)',
                ExpressionAttributeValues={
                    ':businessId': {'S': businessId},
                    ':connection': {'S': '1'}
                }
            )
            data=json.dumps(event)
            result = json.loads(data.replace('\'','"'))
    
            for item in json_dynamodb.loads(toConnections['Items']):
                connectionId = item['PKID']
                try:
                    api_client.post_to_connection(Data=json.dumps(result), ConnectionId=connectionId)
                except ClientError as e:
                    logger.error(e)
                    statusCode = 500
                    body = json.dumps({'Message': str(e), 'Code': 404})
                
        customerId = ''
        customerId = event['CustomerId'] if 'CustomerId' in event else ''
        if customerId != '':
            toCustomers = dynamodb.query(
                TableName="Messages",
                IndexName="Messages_02",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='GSI2PK = :customerId AND begins_with(GSI2SK, :connection)',
                ExpressionAttributeValues={
                    ':customerId': {'S': customerId},
                    ':connection': {'S': '1'}
                }
            )
            data=json.dumps(event)
            result = json.loads(data.replace('\'','"'))
            
            for item in json_dynamodb.loads(toCustomers['Items']):
                connectionId = item['PKID']
                try:
                    api_client.post_to_connection(Data=json.dumps(result), ConnectionId=connectionId)
                except ClientError as e:
                    logger.error(e)
                    statusCode = 500
                    body = json.dumps({'Message': str(e), 'Code': 404})
        statusCode = 200
        body = json.dumps({'Message': 'OK', 'Code': 200})
    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message': 'Error ' + str(e), 'Code': 500})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : "*"
        },
        'body' : body
    }
    return response