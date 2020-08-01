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
                KeyConditionExpression='PKID = :businessId AND SKID = :locationId',
                ExpressionAttributeValues={
                    ':businessId': {'S': 'BUS#' + data['BusinessId']},
                    ':locationId': {'S': 'LOC#' + data['LocationId']}
                }
            )
            for row in json_dynamodb.loads(response['Items']):
                opeHours = row['OPERATIONHOURS'] if 'OPERATIONHOURS' in row else ''
                daysOff = row['DAYS_OFF'] if 'DAYS_OFF' in row else []

            resDays = []
            for day in daysOff:
                resDays.append(json.loads('{"S": "' + day + '"}'))

            recordset = {
                "Put": {
                    "TableName": "TuCita247",
                    "Item": {
                        "PKID": {"S": 'BUS#' + data['BusinessId'] + '#' + data['LocationId']},
                        "SKID": {"S": 'PRO#' + serviceId},
                        "GSI1PK": {"S": 'BUS#' + data['BusinessId']},
                        "GSI1SK": {"S": 'PRO#' + serviceId},
                        "NAME": {"S": data['Name']},
                        "CUSTOMER_PER_BUCKET": {"S": data['CustomerPerBucket']},
                        "OPERATIONHOURS": {"S": opeHours},
                        "DAYS_OFF": {"L": resDays},
                        "PARENTDAYSOFF": {"N": str(1)},
                        "PARENTHOURS": {"N": str(1)},
                        "BUCKET_INTERVAL": {"N": str(1)},
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
                        "PKID": {"S": 'BUS#' + data['BusinessId'] + '#' + data['LocationId']},
                        "SKID": {"S": 'PRO#' + serviceId}
                    },
                    "UpdateExpression": "SET #n = :name, CUSTOMER_PER_BUCKET = :customerperbucket, #s = :status",
                    "ExpressionAttributeValues": {
                        ':name': {'S': data['Name']},
                        ':customerperbucket': {'S': data['CustomerPerBucket']},
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
        body = json.dumps({'Message': 'Service provider added successfully', 'ServiceId': serviceId, 'Code': 200})

        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Message': 'Error on added service provider'})
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