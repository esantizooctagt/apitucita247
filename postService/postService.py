import sys
import logging
import json

import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr
from dynamodb_json import json_util as json_dynamodb

import base64

import uuid
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
        
    try:
        statusCode = ''
        serviceId = str(uuid.uuid4()).replace("-","")
        data = json.loads(event['body'])

        items = []
        recordset = {}
        if data['ServiceId'] == '':
            response = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :businessId AND SKID = :serviceId',
                ExpressionAttributeValues={
                    ':businessId': {'S': 'BUS#' + data['BusinessId']},
                    ':serviceId': {'S': 'SER#' + data['ServiceId']}
                }
            )
            recordset = {
                "Put": {
                    "TableName": "TuCita247",
                    "Item": {
                        "PKID": {"S": 'BUS#' + data['BusinessId']},
                        "SKID": {"S": 'SER#' + serviceId},
                        "NAME": {"S": data['Name']},
                        "TIME_SERVICE": {"N": str(data['TimeService'])},
                        "CUSTOMER_PER_TIME": {"N": data['CustomerPerTime']},
                        "STATUS": {"N": str(data['Status'])}
                    },
                    "ConditionExpression": "attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                    "ReturnValuesOnConditionCheckFailure": "NONE"
                },
            }
        else:
            serviceId = data['ServiceId']
            recordset = {
                "Update": {
                    "TableName": "TuCita247",
                    "Key": {
                        "PKID": {"S": 'BUS#' + data['BusinessId']},
                        "SKID": {"S": 'SER#' + serviceId}
                    },
                    "UpdateExpression": "SET #n = :name, TIME_SERVICE = :timeService, CUSTOMER_PER_TIME = :customerPerTime, #s = :status",
                    "ExpressionAttributeValues": {
                        ':name': {'S': data['Name']},
                        ':timeService': {'S': data['TimeService']},
                        ':customerPerTime': {'S': data['CustomerPerTime']},
                        ':status': {'N': str(data['Status'])}
                    },
                    "ExpressionAttributeNames": {'#s': 'STATUS','#n': 'NAME'},
                    "ConditionExpression": "attribute_exists(PKID) AND attribute_exists(SKID)",
                    "ReturnValuesOnConditionCheckFailure": "NONE"
                },
            }
        items.append(recordset)
        
        logger.info(items)
        response = dynamodb.transact_write_items(
            TransactItems = items
        )
        statusCode = 200
        body = json.dumps({'Message': 'Service added successfully', 'ServiceId': serviceId, 'Code': 200})

        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Message': 'Error on added service'})
    except dynamodb.exceptions.TransactionCanceledException as e:
            statusCode = 404
            body = json.dumps({"Code":400,"error": False, 
                    "success": True, 
                    "message": str(e), 
                    "data": None})
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