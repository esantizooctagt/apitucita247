import json
import logging

import os

import boto3
from botocore.exceptions import ClientError
from dynamodb_json import json_util as json_dynamodb

REGION='us-east-1'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.client('dynamodb', region_name=REGION)
api_client = boto3.client('apigatewaymanagementapi')
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):
    # TODO implement
    connectionId = event['requestContext']['connectionId']
    logger.info(f'message event["requestContext"]["connectionId"]: {connectionId}')

    try:
        dataMessage = dynamodb.delete_item(
            TableName="Messages", 
            Key={
                'PKID': {'S': connectionId},
                'SKID': {'S': connectionId},
                }
        )
        statusCode = 200
    except ClientError as e:
        logger.error(e)
        statusCode = 500

    return { 'statusCode': statusCode }
