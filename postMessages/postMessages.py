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

APIID = os.environ['apiId']
dynamodb = boto3.client('dynamodb', region_name=REGION)
api_client = boto3.client('apigatewaymanagementapi', endpoint_url='https://'+APIID+'.execute-api.'+REGION+'.amazonaws.com/prod')
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):
    try:
        businessId = ''
        data = ''
        customerId = ''
        process = 0
        if 'BusinessId' in event or 'CustomerId' in event: 
            process = 1
        if process == 1:
            businessId = event['BusinessId'] if 'BusinessId' in event else ''
            customerId = event['CustomerId'] if 'CustomerId' in event else ''
        else:
            data=json.loads(event['body'])
            businessId = data['BusinessId'] if 'BusinessId' in data else  ''
            customerId = data['CustomerId'] if 'CustomerId' in data else  ''
            
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
            if process == 1:
                data=json.dumps(event)
                result = json.loads(data.replace('\'','"'))
            else:
                result=data
    
            for item in json_dynamodb.loads(toConnections['Items']):
                connectionId = item['PKID']
                try:
                    api_client.post_to_connection(Data=json.dumps(result), ConnectionId=connectionId)
                except ClientError as e:
                    logger.error(e)
                    statusCode = 500
                    body = json.dumps({'Message': str(e), 'Code': 404})
                
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
            if process == 1:
                data=json.dumps(event)
                result = json.loads(data.replace('\'','"'))
            else:
                result=data
            
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