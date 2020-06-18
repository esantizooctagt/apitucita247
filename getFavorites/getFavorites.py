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

dynamodb = boto3.client('dynamodb', region_name='us-east-1')
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):
    records =[]
    try:
        customerId = event['pathParameters']['customerId']
        response = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :customerId AND SKID = :favs',
            ExpressionAttributeValues={
                ':customerId': {'S': 'CUS#' + customerId},
                ':favs': {'S': 'FAVS'}
            },
        )
        for row in json_dynamodb.loads(response['Items']):
            locAddress = ''
            location = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :businessId AND SKID = :locationId',
                ExpressionAttributeValues={
                    ':businessId': {'S': 'BUS#' + row['BUSID']},
                    ':locationId': {'S': 'LOC#' + row['LOCID']}
                },
                Limit = 1
            )
            for loc in json_dynamodb.loads(location['Items']):
                locAddress = loc['ADDRESS']

            business = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :businessId AND SKID = :metadata',
                ExpressionAttributeValues={
                    ':businessId': {'S': 'BUS#' + row['BUSID']},
                    ':metadata': {'S': 'METADATA'}
                },
                Limit = 1
            )
            recordset = {}
            for business in json_dynamodb.loads(business['Items']):
                recordset = {
                    'Name': business['NAME'],
                    'Imagen': business['IMGBUSINESS'],
                    'LongDescrip': business['LONGDESCRIPTION'],
                    'ShortDescrip': business['SHORTDESCRIPTION'],
                    'Location': locAddress
                }
                records.append(recordset)

        statusCode = 200
        body = json.dumps(records)
    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message': 'Error on request try again ' + str(e)})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : "*"
        },
        'body' : body
    }
    return response