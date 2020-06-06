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
                        "GSI2SK": {"S": 'DT#' + data['DatePoll']},
                        "NAME": {"S": data['Name']},
                        "LOCATIONID": {"S": data['LocationId']},
                        "DATE_POLL": {"S": data['DatePoll']},
                        "STATUS": {"N": str(data['Status'])}
                    },
                    "ConditionExpression": "attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                    "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
                },
            }
        else:
            recordset = {
                "Put": {
                    "TableName": "TuCita247",
                    "Item": {
                        "PKID": {"S": 'BUS#'+data['BusinessId']},
                        "SKID": {"S": 'POLL#' + pollId},
                        "GSI1PK": {"S": 'POLL#' + pollId},
                        "GSI1SK": {"S": 'POLL#' + pollId},
                        "GSI2PK": {"S": 'BUS#' + data['BusinessId'] + '#LOC#' + data['LocationId']},
                        "GSI2SK": {"S": 'DT#' + data['DatePoll']},
                        "NAME": {"S": data['NAME']},
                        "LOCATIONID": {"S": data['LocationId']},
                        "DATE_POLL": {"S": data['DatePoll']},
                        "STATUS": {"N": str(data['Status'])}
                    },
                    "ConditionExpression": "attribute_exists(PKID) AND attribute_exists(SKID)",
                    "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
                },
            }
        items.append(recordset)
        line = 0
        for quest in data['Questions']:
            if quest['QuestionId'] == '':
                recordset = {
                    "Put": {
                        "TableName": "TuCita247",
                        "Item": {
                            "PKID": {"S": 'POLL#' + pollId + '#ITEM#' + line},
                            "SKID": {"S": 'POLL#' + pollId + '#ITEM#' + line},
                            "GSI1PK": {"S": 'POLL#' + pollId},
                            "GSI1SK": {"S": 'ITEM#' + line},
                            "DESCRIPTION": {"S": quest['Description']},
                            "HAPPY": {"N": str(0)},
                            "NEUTRAL": {"N": str(0)},
                            "ANGRY": {"N": str(0)}
                        },
                        "ConditionExpression": "attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                        "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
                    }
                }
            else:
                recordset = {
                    "Put": {
                        "TableName": "TuCita247",
                        "Item": {
                            "PKID": {"S": 'POLL#' + pollId + '#ITEM#' + line},
                            "SKID": {"S": 'POLL#' + pollId + '#ITEM#' + line},
                            "GSI1PK": {"S": 'POLL#' + pollId},
                            "GSI1SK": {"S": 'ITEM#' + line},
                            "DESCRIPTION": {"S": quest['Description']}
                        },
                        "ConditionExpression": "attribute_exists(PKID) AND attribute_exists(SKID)",
                        "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
                    }
                }
            items.append(recordset)
            
        logger.info(items)
        response = dynamodb.transact_write_items(
            TransactItems = items
        )
        
        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Message': 'Error on added user'})
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