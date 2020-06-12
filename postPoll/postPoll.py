import sys
import logging
import json

import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr

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
        pollId = str(uuid.uuid4()).replace("-","")
        data = json.loads(event['body'])

        items = []
        recordset = {}
        if data['PollId'] == '':
            recordset = {
                "Put": {
                    "TableName": "TuCita247",
                    "Item": {
                        "PKID": {"S": 'BUS#' + data['BusinessId']},
                        "SKID": {"S": 'POLL#' + pollId},
                        "GSI1PK": {"S": 'POLL#' + pollId},
                        "GSI1SK": {"S": 'POLL#' + pollId},
                        "GSI2PK": {"S": 'BUS#' + data['BusinessId'] + '#LOC#' + data['LocationId']},
                        "GSI2SK": {"S": str(data['Status']) + '#DT#' + data['DatePoll']},
                        "NAME": {"S": data['Name']},
                        "LOCATIONID": {"S": data['LocationId']},
                        "DATE_POLL": {"S": data['DatePoll']},
                        "DATE_FIN_POLL": {"S": data['DateFinPoll']},
                        "HAPPY": {"N": "0"},
                        "NEUTRAL": {"N": "0"},
                        "ANGRY": {"N": "0"},
                        "STATUS": {"N": str(data['Status'])}
                    },
                    "ConditionExpression": "attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                    "ReturnValuesOnConditionCheckFailure": "NONE"
                },
            }
        else:
            pollId = data['PollId']
            recordset = {
                "Update": {
                    "TableName": "TuCita247",
                    "Key": {
                        "PKID": {"S": 'BUS#' + data['BusinessId']},
                        "SKID": {"S": 'POLL#' + pollId}
                    },
                    "UpdateExpression": "SET #n = :name, GSI2PK = :key2, GSI2SK = :key3, LOCATIONID = :locationId, DATE_POLL = :datePoll, DATE_FIN_POLL = :dateFinPoll, #s = :status",
                    "ExpressionAttributeValues": {
                        ':name': {'S': data['Name']},
                        ':key2': {'S': 'BUS#' + data['BusinessId'] + '#LOC#' + data['LocationId']},
                        ':key3': {'S': str(data['Status']) + '#DT#' + data['DatePoll']},
                        ':locationId': {'S': data['LocationId']},
                        ':datePoll': {'S': data['DatePoll']},
                        ':dateFinPoll': {'S': data['DateFinPoll']},
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
        body = json.dumps({'Message': 'Poll added successfully', 'PollId': pollId, 'Code': 200})

        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Message': 'Error on added poll'})
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