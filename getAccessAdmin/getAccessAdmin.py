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

dynamodb = boto3.client('dynamodb', region_name=REGION)
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']
        
    try:
        businessId = event['pathParameters']['businessId']
        roleId = event['pathParameters']['roleId']

        response = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :businessId AND begins_with(SKID , :access)',
            ExpressionAttributeValues={
                ':businessId': {'S': 'BUS#' + businessId},
                ':access': {'S': 'ACCESS#' + roleId + '#'}
            }
        )
        access = []
        for row in response['Items']:
            recordset = {
                'AppId': row['SKID'].replace('ACCESS#' + roleId + '#', ''),
                'Access': row['LEVEL_ACCESS'] 
            }
            access.append(recordset)

        statusCode = 200
        body = json.dumps({'Code': 200, 'Access': access})                    
    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message': 'Error on request try again'})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : cors
        },
        'body' : body
    }
    return response