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

dynamodb = boto3.client('dynamodb', region_name=REGION)
api_client = boto3.client('apigatewaymanagementapi')
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):
    # TODO implement
    country_date = dateutil.tz.gettz('America/Puerto_Rico')
    today = datetime.datetime.now(tz=country_date)
    opeDate = today.strftime("%Y%m%d%H%M")
        
    connectionId = event['requestContext']['connectionId']
    businessId = connectionId
    customerId = ''
    skey = '0#'
    if 'queryStringParameters' in event:
        if 'businessId' in event['queryStringParameters']:
            businessId = event['queryStringParameters']['businessId']
            skey = '1#'+connectionId
        if 'customerId' in event['queryStringParameters']:
            customerId = event['queryStringParameters']['customerId']
            skey = '1#'+connectionId
    try:
        if customerId == '':
            dataMessage = dynamodb.put_item(
                TableName="Messages",
                ReturnConsumedCapacity="TOTAL",
                Item={
                    'PKID': {'S': connectionId},
                    'SKID': {'S': connectionId},
                    'GSI1PK': {'S': businessId},
                    'GSI1SK': {'S': skey},
                    'MSGS': {'L': []}, 
                    'DATEOPE': {'S': opeDate}
                },
                ConditionExpression="attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                ReturnValues='NONE'
            )
        else:
            dataMessage = dynamodb.put_item(
                TableName="Messages",
                ReturnConsumedCapacity="TOTAL",
                Item={
                    'PKID': {'S': connectionId},
                    'SKID': {'S': connectionId},
                    'GSI2PK': {'S': customerId},
                    'GSI2SK': {'S': skey},
                    'MSGS': {'L': []}, 
                    'DATEOPE': {'S': opeDate}
                },
                ConditionExpression="attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                ReturnValues='NONE'
            )
        statusCode = 200
    except ClientError as e:
        logger.error(e)
        statusCode = 500
        
    return { 'statusCode': statusCode }
