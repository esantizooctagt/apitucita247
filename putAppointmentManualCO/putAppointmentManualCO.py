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

dynamodb = boto3.client('dynamodb', region_name=REGION)
lambdaInv = boto3.client('lambda')
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']
        
    try:
        statusCode = ''
        peopleQty=0
        businessId = event['pathParameters']['businessId']
        locationId = event['pathParameters']['locationId']
        qty = int(event['pathParameters']['qtyGuests'])

        guests = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :businessId AND SKID = :locationId',
            ExpressionAttributeValues={
                ':businessId': {'S': 'BUS#' + businessId},
                ':locationId': {'S': 'LOC#' + locationId}
            }
        )
        for row in json_dynamodb.loads(guests['Items']):
            peopleQty = row['PEOPLE_CHECK_IN']

        if peopleQty-qty < 0:
            qty = peopleQty
        
        items = []
        recordset = {
            "Update": {
                "TableName": "TuCita247",
                "Key": {
                    "PKID": {"S": 'BUS#' + businessId}, 
                    "SKID": {"S": 'LOC#' + locationId}, 
                },
                "UpdateExpression": "SET PEOPLE_CHECK_IN = PEOPLE_CHECK_IN - :increment",
                "ExpressionAttributeValues": { 
                    ":increment": {"N": str(qty)},
                    ":cero": {"N": str(0)}
                },
                "ConditionExpression": "attribute_exists(PKID) AND attribute_exists(SKID) AND PEOPLE_CHECK_IN >= :cero",
                "ReturnValuesOnConditionCheckFailure": "ALL_OLD" 
            }
        }
        items.append(recordset)
        tranAppo = dynamodb.transact_write_items(
            TransactItems = items
        )

        data = {
            'BusinessId': businessId,
            'LocationId': locationId,
            'Guests': qty,
            'Tipo': 'MOVE',
            'To': 'CHECKOUT'
        }
        lambdaInv.invoke(
            FunctionName='PostMessages',
            InvocationType='Event',
            Payload=json.dumps(data)
        )
            
        statusCode = 200
        body = json.dumps({'Message': 'Manual Check Out successfully', 'Code': 200})

        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Message': 'Error on update check out', 'Code': 500})
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