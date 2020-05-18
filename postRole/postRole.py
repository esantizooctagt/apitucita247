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

def listToDict(lst):
    op = {lst[i]: lst[i + 1] for i in range(0, len(lst), 2)}
    return op

def lambda_handler(event, context):
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']
        
    try:
        roleId = str(uuid.uuid4()).replace("-","")
        data = json.loads(event['body'])
        records = []
        logger.info(data)
        for items in data['Access']:
            recordset = {
                    "Put": {
                        "TableName": "TuCita247",
                        "Item": {
                            "PKID": {"S": 'BUS#'+data['BusinessId']},
                            "SKID": {"S": 'ACCESS#'+roleId+'#'+items['ApplicationId']},
                            "LEVEL_ACCESS": {"N": items['Level_Access']}
                        },
                        "ConditionExpression": "attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                        "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
                    }
                }
            records.append(recordset)

        elements = listToDict(records)
        logger.info(elements)
        response = dynamodb.transact_write_items(
            TransactItems=[
                {
                    "Put": {
                        "TableName": "TuCita247",
                        "Item": {
                            "PKID": {"S": 'BUS#'+data['BusinessId']},
                            "SKID": {"S": 'ROL#' + roleId },
                            "NAME": {"S": data['Name']},
                            "STATUS": {"N": "1"}
                        },
                        "ConditionExpression": "attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                        "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
                    },
                },
                elements
            ]
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