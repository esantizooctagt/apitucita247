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

import base64

import uuid
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
        statusCode = ''
        serviceId = str(uuid.uuid4()).replace("-","")

        country_date = dateutil.tz.gettz('America/Puerto_Rico')
        today = datetime.datetime.now(tz=country_date)
        dateOpe = today.strftime("%Y-%m-%d-%H-%M-%S")

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
                        "BUFFER_TIME": {"N": str(data['BufferTime'])},
                        "CUSTOMER_PER_TIME": {"N": data['CustomerPerTime']},
                        "CUSTOMER_PER_BOOKING": {"N": data['CustomerPerBooking']},
                        "COLOR": {"S": data['Color']},
                        "STATUS": {"N": str(data['Status'])},
                        "CREATED_DATE": {"S": str(dateOpe)}
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
                    "UpdateExpression": "SET #n = :name, TIME_SERVICE = :timeService, BUFFER_TIME = :bufferTime, CUSTOMER_PER_TIME = :customerPerTime, CUSTOMER_PER_BOOKING = :customerPerBooking, #s = :status, COLOR = :color, MODIFIED_DATE = :mod_date",
                    "ExpressionAttributeValues": {
                        ':name': {'S': data['Name']},
                        ':timeService': {'N': str(data['TimeService'])},
                        ':bufferTime': {'N': str(data['BufferTime'])},
                        ':customerPerTime': {'N': str(data['CustomerPerTime'])},
                        ':customerPerBooking': {'N': str(data['CustomerPerBooking'])},
                        ':status': {'N': str(data['Status'])},
                        ':color': {'S': data['Color']},
                        ":mod_date": {"S": str(dateOpe)}
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