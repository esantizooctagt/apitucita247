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
        surveyId = str(uuid.uuid4()).replace("-","")
        data = json.loads(event['body'])

        items = []
        recordset = {}
        if data['SurveyId'] == '':
            recordset = {
                "Put": {
                    "TableName": "TuCita247",
                    "Item": {
                        "PKID": {"S": 'BUS#' + data['BusinessId']},
                        "SKID": {"S": 'SUR#' + surveyId},
                        "GSI1PK": {"S": 'SUR#' + surveyId},
                        "GSI1SK": {"S": 'SUR#' + surveyId},
                        "GSI2PK": {"S": 'BUS#' + data['BusinessId'] + '#LOC#' + data['LocationId']},
                        "GSI2SK": {"S": 'DT#' + data['DateSurvey']},
                        "NAME": {"S": data['Name']},
                        "LOCATIONID": {"S": data['LocationId']},
                        "DATE_SURVEY": {"S": data['DateSurvey']},
                        "STATUS": {"N": str(data['Status'])}
                    },
                    "ConditionExpression": "attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                    "ReturnValuesOnConditionCheckFailure": "NONE"
                },
            }
        else:
            surveyId = data['SurveyId']
            recordset = {
                "Update": {
                    "TableName": "TuCita247",
                    "Key": {
                        "PKID": {"S": 'BUS#' + data['BusinessId']},
                        "SKID": {"S": 'SUR#' + surveyId}
                    },
                    "UpdateExpression": "SET #n = :name, GSI2PK = :key2, GSI2SK = :key3, LOCATIONID = :locationId, DATE_SURVEY = :dateSurvey, #s = :status",
                    "ExpressionAttributeValues": {
                        ':name': {'S': data['Name']},
                        ':key2': {'S': 'BUS#' + data['BusinessId'] + '#LOC#' + data['LocationId']},
                        ':key3': {'S': 'DT#' + data['DateSurvey']},
                        ':locationId': {'S': data['LocationId']},
                        ':dateSurvey': {'S': data['DateSurvey']},
                        ':status': {'N': str(data['Status'])}
                    },
                    "ExpressionAttributeNames": {'#s': 'STATUS','#n': 'NAME'},
                    "ConditionExpression": "attribute_exists(PKID) AND attribute_exists(SKID)",
                    "ReturnValuesOnConditionCheckFailure": "NONE"
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
                            "PKID": {"S": 'SUR#' + surveyId + '#ITEM#' + str(line)},
                            "SKID": {"S": 'SUR#' + surveyId + '#ITEM#' + str(line)},
                            "GSI1PK": {"S": 'SUR#' + surveyId},
                            "GSI1SK": {"S": 'ITEM#' + str(line)},
                            "DESCRIPTION": {"S": quest['Description']},
                            "HAPPY": {"N": str(0)},
                            "NEUTRAL": {"N": str(0)},
                            "ANGRY": {"N": str(0)},
                            "STATUS": {"N": str(1)}
                        },
                        "ConditionExpression": "attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                        "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
                    }
                }
            else:
                if int(quest['Status']) == 1:
                    recordset = {
                        "Update": {
                            "TableName": "TuCita247",
                            "Key": {
                                "PKID": {"S": 'SUR#' + surveyId + '#ITEM#' + quest['QuestionId']},
                                "SKID": {"S": 'SUR#' + surveyId + '#ITEM#' + quest['QuestionId']}
                            },
                            "UpdateExpression": "SET DESCRIPTION = :description, #s = :status",
                            "ExpressionAttributeValues": {
                                ':description': {'S': quest['Description']},
                                ':status': {'N': str(quest['Status'])}
                            },
                            "ExpressionAttributeNames": {'#s': 'STATUS'},
                            "ConditionExpression": "attribute_exists(PKID) AND attribute_exists(SKID)",
                            "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
                        }
                    }
                else:
                    recordset = {
                        "Delete": {
                            "TableName": "TuCita247",
                            "Key": {
                                "PKID": {"S": 'SUR#' + surveyId + '#ITEM#' + quest['QuestionId']},
                                "SKID": {"S": 'SUR#' + surveyId + '#ITEM#' + quest['QuestionId']}
                            },
                            "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
                        }
                    }
                line = int(quest['QuestionId'])
                
            items.append(recordset)
            line = line + 1
            
        logger.info(items)
        response = dynamodb.transact_write_items(
            TransactItems = items
        )
        statusCode = 200
        body = json.dumps({'Message': 'Survey added successfully', 'Code': 200})

        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Message': 'Error on added survey'})
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