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

cloudsearch = boto3.client('cloudsearchdomain', endpoint_url="https://search-tucita247-djl3mvkaapbmo5zjxat7pcnepu.us-east-1.cloudsearch.amazonaws.com")
logger.info("SUCCESS: Connection to CloudSearch")

def lambda_handler(event, context):
    # DELETE VPC FROM LAMBDA FUNCTION NEEDS CONNECTION TO INTERNET TO CALL ENDPOINT_URL
    try:
        queryStr = event['pathParameters']['search']

        response = cloudsearch.search(
            query=queryStr,
            queryParser='simple',
            # returnFields='_all',
            # size=10
        )
                
        statusCode = 200
        body = json.dumps(response)
    except botocore.exceptions.EndpointConnectionError as e:
        statusCode = 500
        body = json.dumps({'Message':'Error on request try again ' + str(e)})
    except:
        statusCode = 500
        body = json.dumps({'Message':'Error on request try again'})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : "*"
        },
        'body' : body
    }
    return response