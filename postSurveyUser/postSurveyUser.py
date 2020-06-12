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
        data = json.loads(event['body'])

        items = []
        recordset = {}
        response = dynamodb.query(
                TableName="TuCita247",
                IndexName="TuCita247_Index",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='GSI1PK = :key AND GSI1SK = :key',
                ExpressionAttributeValues={
                    ':key': {'S': 'CUS#' + data['CustomerId']}
                }
            )

        if response['Count'] > 0:
            for item in data['Questions']:
                recordset = {
                    "Put": {
                        "TableName": "TuCita247",
                        "Item": {
                            "PKID": {"S": 'SUR#' + data['SurveyId'] + '#ITEM#' + item['QuestionId']},
                            "SKID": {"S": 'CUS#' + data['CustomerId']},
                            "GSI1PK": {"S": 'SUR#' + data['SurveyId']},
                            "GSI1SK": {"S": 'CUS#' + data['CustomerId']},
                            "HAPPY": {"N": str(item['Happy'])},
                            "NEUTRAL": {"N": str(item['Neutral'])},
                            "ANGRY": {"N": str(item['Angry'])}
                        },
                        "ConditionExpression": "attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                        "ReturnValuesOnConditionCheckFailure": "NONE"
                    },
                }
                items.append(recordset)

                recordset = {
                    "Update": {
                        "TableName": "TuCita247",
                        "Key": {
                            "PKID": {"S": 'SUR#' + data['SurveyId'] + '#ITEM#' + item['QuestionId']},
                            "SKID": {"S": 'SUR#' + data['SurveyId'] + '#ITEM#' + item['QuestionId']}
                        },
                        "UpdateExpression": "SET HAPPY = HAPPY + :happy, NEUTRAL = NEUTRAL + :neutral, ANGRY = ANGRY + :angry",
                        "ExpressionAttributeValues": {
                            ':happy': {'N': str(item['Happy'])},
                            ':neutral': {'N': str(item['Neutral'])},
                            ':angry': {'N': str(item['Angry'])}
                        },
                        "ConditionExpression": "attribute_exists(PKID) AND attribute_exists(SKID)",
                        "ReturnValuesOnConditionCheckFailure": "NONE"
                    }
                }
                items.append(recordset)
                
            logger.info(items)
            response = dynamodb.transact_write_items(
                TransactItems = items
            )
            statusCode = 200
            body = json.dumps({'Message': 'Survey saved successfully', 'Code': 200})
        else:
            statusCode = 404
            body = json.dumps({'Message': 'Customer no valid', 'Code': 404})

        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Message': 'Error on save survey', 'Code': 404})
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