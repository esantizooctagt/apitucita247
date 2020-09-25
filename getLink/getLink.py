import sys
import logging
import json

import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr

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
        Link = event['pathParameters']['link']
        response = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :link',
            ExpressionAttributeValues={
                ':link': {'S': 'LINK#' + Link}
            }
        )
        if response == None:
            statusCode = 404
            body = json.dumps({'Message':'No valid entry found for link'})
        else:
            if response['Count'] > 0:
                recordset = {
                    'Available' : 0
                }
            else:
                recordset = {
                    'Available' : 1
                }
                
            statusCode = 200
            body = json.dumps(recordset)
    except:
        statusCode = 500
        body = json.dumps({'Message':'Error on request try again'})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : cors
        },
        'body' : body
    }
    return response