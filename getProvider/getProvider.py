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

dynamodb = boto3.client('dynamodb', region_name='us-east-1')
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']

    try:
        providerId = event['pathParameters']['providerId']
        businessId = event['pathParameters']['businessId']

        provider = dynamodb.query(
            TableName="TuCita247",
            IndexName="TuCita247_Index",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='GSI1PK = :businessId AND GSI1SK = :providerId',
            ExpressionAttributeValues={
                ':businessId': {'S': 'BUS#' + businessId},
                ':providerId': {'S': 'PRO#' + providerId}
            },
            Limit =1
        )
        for item in json_dynamodb.loads(provider['Items']):
            recordset = {
                'ProviderId': item['SKID'].replace('PRO#',''),
                'Name': item['NAME'],
                'LocationId': item['PKID'].replace('BUS#' + businessId + '#LOC#', ''),
                'Status': int(item['STATUS'])
            }
    
        statusCode = 200
        body = json.dumps(recordset)

        if statusCode == '':
            statusCode = 404
            body = json.dumps({"Message": "No more rows", "Code": 404})
    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message': 'Error on request try again ' +str(e)})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : cors
        },
        'body' : body
    }
    return response