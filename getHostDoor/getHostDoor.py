import sys
import logging
import json

import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr
from dynamodb_json import json_util as json_dynamodb

import datetime
import dateutil.tz
from datetime import timezone

import os

REGION = 'us-east-1'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.client('dynamodb', region_name='us-east-1')
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']
    
    country_date = dateutil.tz.gettz('America/Puerto_Rico')
    today = datetime.datetime.now(tz=country_date)
    dateOpe = today.strftime("%Y-%m-%d")

    try:
        businessId = event['pathParameters']['businessId']
        userId = event['pathParameters']['userId']

        response = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :businessId AND begins_with( SKID , :userId )',
            ExpressionAttributeValues={
                ':businessId': {'S': 'BUS#' + businessId },
                ':userId': {'S': 'USER#'+ userId }
            }
        )
        recordset = {}
        locationId = ''
        door = ''
        locations = json_dynamodb.loads(response['Items'])
        for row in locations:
            locationId = row['LOCATIONID'],
            door = row['DOOR']
        
        locs = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :businessId AND SKID = :locationId )',
            ExpressionAttributeValues={
                ':businessId': {'S': 'BUS#' + businessId },
                ':locationId': {'S': 'LOC#'+ locationId },
                ':actualDate': {'S': dateOpe }
            },
            FilterExpression='begins_with ( OPEN_DATE , :actualDate )'
        )
        for item in json_dynamodb.loads(locs['Items']):
            recordset = {
                'LocationId': locationId,
                'Door': door,
                'Open': item['OPEN']
            }
        if recordset != {}:
            recordset = {
                'LocationId': locationId,
                'Door': door,
                'Open': 2
            }

        statusCode = 200
        body = json.dumps({'Code': 200, 'Locs': recordset})
    
        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Code': 500, 'Message': 'Error on load locations by user'})
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