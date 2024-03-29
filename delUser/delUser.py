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

dynamodb = boto3.resource('dynamodb', region_name=REGION)
dynamodbQry = boto3.client('dynamodb', region_name=REGION)
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']
        
    try:
        statusCode = ''
        isAdmin = 0
        userId = event['pathParameters']['id']
        businessId = event['pathParameters']['businessId']

        getUser = dynamodbQry.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :businessId AND SKID = :userId',
            ExpressionAttributeValues={
                ':businessId': {'S': 'BUS#'+businessId},
                ':userId': {'S': 'USER#'+userId}
            }
        )
        for item in json_dynamodb.loads(getUser['Items']):
            isAdmin = item['IS_ADMIN'] if 'IS_ADMIN' in item else 0
        
        if isAdmin == 0:
            table = dynamodb.Table('TuCita247')
            response = table.delete_item(
                Key={
                    'PKID': 'BUS#' + businessId,
                    'SKID': 'USER#' + userId
                }
            )
            statusCode = 200
            body = json.dumps({'Code': 200, 'Message': 'User deleted successfully'})
        else:
            statusCode = 200
            body = json.dumps({'Code': 400, 'Message': "Admin user can't be deleted"})

        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Message': 'Error on delete user'})
    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message': 'Error on request try again ' + str(e)})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : cors
        },
        'body' : body
    }
    return response