import sys
import logging
import json

import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr
from dynamodb_json import json_util as json_dynamodb

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
        roleId = str(uuid.uuid4()).replace("-","")
        data = json.loads(event['body'])

        recordList = {}
        i=0
        for items in data['Access']:
            recordset = {
                            "Put": {
                                "TableName": "TuCita247",
                                "Item": {
                                    "PKID": {"S": 'BUS#'+data['BusinessId']},
                                    "SKID": {"S": 'ACCESS#'+roleId+'#'+items['ApplicationId']},
                                    "LEVEL_ACCESS": {"N": str(items['Level_Access'])}
                                },
                                "ConditionExpression": "attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                                "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
                            }
                        }
            recordList[i] = recordset
            i=i+1
        
        items = []
        rows = {}
        rows = {
            "Update": {
                "TableName": "TuCita247",
                "Key": {
                    "PKID": {"S": 'BUS#'+data['BusinessId']},
                    "SKID": {"S": 'ROL#' + roleId }
                },
                "UpdateExpression":"set NAME = :name, STATUS = :status",
                "ExpressionAttributeValues": { 
                    ":name": data['Name'],
                    ":status": data['Status'] 
                },
                "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
            },
        },
        {
            "Delete":{
                "TableName":"TuCita247",
                "Key": {
                    "PKID": {"S": 'BUS#'+data['BusinessId']},
                    "SKID": {"S": 'begins_with (SKID , ACCESS#' + roleId + ')'}
                },
                "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
            },
        }
        items.append(rows)

        for x in range(i):
            items.append(recordList[x])    

        response = dynamodb.transact_write_items(
            TransactItems = items
            
        )
        logger.info(response)
        statusCode = 200
        body = json.dumps({'Message': 'Role added successfully'})
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