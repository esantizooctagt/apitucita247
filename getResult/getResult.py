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

dynamodb = boto3.client('dynamodb', region_name='us-east-1')
logger.info("SUCCESS: Connection to DynamoDB succeeded")

cloudsearch = boto3.client('tucita247')
logger.info("SUCCESS: Connection to CloudSearch")

def lambda_handler(event, context):
    # stage = event['headers']
    # if stage['origin'] != "http://localhost:4200":
    #     cors = os.environ['prodCors']
    # else:
    #     cors = os.environ['devCors']
    cors = "http://localhost:8100"

    try:
        response = cloudsearch.search(
            query="(and NAME:'inge')",
            queryParser='structured',
            returnFields='NAME, PKID, SKID, GSI1PK, GSI1SK',
            size=10
        )
                
        statusCode = 200
        body = json.dumps(response)
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