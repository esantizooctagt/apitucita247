import sys
import logging
import json

import boto3
import botocore.exceptions
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
from dynamodb_json import json_util as json_dynamodb

import datetime
import dateutil.tz
from datetime import timezone

import os

REGION = 'us-east-1'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']
        
    try:
        statusCode = ''
        locationId = event['pathParameters']['id']
        businessId = event['pathParameters']['businessId']
        
        country_date = dateutil.tz.gettz('America/Puerto_Rico')
        today = datetime.datetime.now(tz=country_date)
        dateOpe = today.strftime("%Y-%m-%d-%H-%M-%S")

        table = dynamodb.Table('TuCita247')
        response = table.update_item(
            Key={
                'PKID': 'BUS#' + businessId,
                'SKID': 'LOC#' + locationId
            },
            UpdateExpression="SET PEOPLE_CHECK_IN = :qty, OPEN_DATE = :closed, #o = :open", 
            ExpressionAttributeValues= {':qty': 0, ':closed': '', ':initVal': 1, ':open': 0},
            ExpressionAttributeNames={'#o': 'OPEN'},
            ConditionExpression='#o = :initVal',
            ReturnValues="UPDATED_NEW"
        )

        statusCode = 200
        body = json.dumps({'Message': 'Location closed successfully', 'Code': 200, 'Business': json_dynamodb.loads(response['Attributes'])})

        logger.info(response)
        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Message': 'Error on closed location', 'Code': 500})
    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message': 'Error on request try again ' + str(e), 'Code': 500})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : cors
        },
        'body' : body
    }
    return response