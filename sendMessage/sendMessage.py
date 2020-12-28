import json
import logging

import os

import boto3
from botocore.exceptions import ClientError
from dynamodb_json import json_util as json_dynamodb

import datetime
import dateutil.tz
from datetime import timezone

REGION='us-east-1'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

APIID = os.environ['apiId']
dynamodbData = boto3.resource('dynamodb', region_name=REGION)
dynamodb = boto3.client('dynamodb', region_name=REGION)
api_client = boto3.client('apigatewaymanagementapi', endpoint_url='https://'+APIID+'.execute-api.'+REGION+'.amazonaws.com/prod')
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):
    # TODO implement
    connectionId = event['requestContext']['connectionId']

    sndMsg = json.loads(event['body'])
    data = sndMsg['msg'].replace('\'','"')
    data = json.loads(data)
    statusCode = 0
    try:
        table = dynamodbData.Table('Messages')
        dataMessage = table.update_item(
            Key={
                'PKID': connectionId,
                'SKID': connectionId
            },
            UpdateExpression="SET MSGS = list_append(MSGS,:data)",
            ExpressionAttributeValues={
                ':data': [json.dumps(data)]
            },
            ReturnValues="UPDATED_NEW"
        )
        
        toConnections = dynamodb.query(
            TableName="Messages",
            IndexName="Messages_01",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='GSI1PK = :businessId AND begins_with(GSI1SK, :connection)',
            ExpressionAttributeValues={
                ':businessId': {'S': data['BusinessId']},
                ':connection': {'S': '1'}
            }
        )
        for item in json_dynamodb.loads(toConnections['Items']):
            connectionId = item['PKID']
            try:
                api_client.post_to_connection(Data=json.dumps(data), ConnectionId=connectionId)
            except ClientError as e:
                logger.error(e)
                statusCode = 500
                body = json.dumps({'Message': str(e), 'Code': 404})
        
        toCustomers = dynamodb.query(
            TableName="Messages",
            IndexName="Messages_02",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='GSI2PK = :customerId AND begins_with(GSI2SK, :connection)',
            ExpressionAttributeValues={
                ':customerId': {'S': data['CustomerId']},
                ':connection': {'S': '1'}
            }
        )
        for item in json_dynamodb.loads(toCustomers['Items']):
            connectionId = item['PKID']
            try:
                api_client.post_to_connection(Data=json.dumps(data), ConnectionId=connectionId)
            except ClientError as e:
                logger.error(e)
                statusCode = 500
                body = json.dumps({'Message': str(e), 'Code': 404})
        
        if statusCode == 0:
            statusCode = 200
            body = json.dumps({'Message': connectionId, 'Code': 200})
        
    except ClientError as e:
        logger.error(e)
        statusCode = 500
        body = json.dumps({'Message': str(e), 'Code': 500})
            
    return { 'statusCode': statusCode, 'body': body }