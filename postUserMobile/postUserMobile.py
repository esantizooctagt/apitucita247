import sys
import logging
import json

import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr

import uuid
import os
import random

REGION = 'us-east-1'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.client('dynamodb', region_name='us-east-1')
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def cleanNullTerms(d):
   clean = {}
   for k, v in d.items():
      if isinstance(v, dict):
         nested = cleanNullTerms(v)
         if len(nested.keys()) > 0:
            clean[k] = nested
      elif v is not None:
         clean[k] = v
   return clean

def lambda_handler(event, context):
    stage = event['headers']
    cors = stage['origin']

    try:
        statusCode = ''
        userId = str(uuid.uuid4()).replace("-","")
        data = json.loads(event['body'])

        items = []
        recordset = {}
        recordset = {
                    "Put": {
                        "TableName": "TuCita247",
                        "Item": {
                            "PKID": {"S": 'MOB#' + data['Phone'] },
                            "SKID": {"S": 'CUS#' + userId },
                            "GSI1PK": {"S": 'CUS#' + userId},
                            "GSI1SK": {"S": 'CUS#' + userId},
                            "NAME": {"S": data['Name']},
                            "EMAIL": {"S": str(data['Email']) if str(data['Email']) != '' else None},
                            "DOB": {"S": str(data['DOB']) if str(data['DOB']) != '' else None},
                            "GENDER": {"S": str(data['Gender']) if str(data['Gender']) != '' else None},
                            "PREFERENCES": {"S": str(data['Preferences']) if str(data['Preferences']) != '' else None},
                            "DISABILITY": {"S": str(data['Disability']) if str(data['Disability']) != '' else None},
                            "STATUS": {"N": "1"}
                        },
                        "ConditionExpression": "attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                        "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
                    },
                }
        items.append(cleanNullTerms(recordset))

        recordset = {
                    "Put": {
                        "TableName": "TuCita247",
                        "Item": {
                            "PKID": {"S": 'MOB#' + data['Phone']},
                            "SKID": {"S": 'MOB#' + data['Phone']}
                        },
                        "ConditionExpression": "attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                        "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
                    }
                }
        items.append(cleanNullTerms(recordset))

        response = dynamodb.transact_write_items(
            TransactItems=[
                items
            ]
        )

        statusCode = 200
        body = json.dumps({'Message': 'User added successfully', 'Code': 200})

        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Message': 'Error on added user', 'Code': 400})
    except dynamodb.exceptions.TransactionCanceledException as e:
            statusCode = 404
            body = json.dumps({"Code":400,"error": False, 
                    "success": True, 
                    "message": str(e), 
                    "data": None})
    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message': 'Error on request try again ' + str(e), 'Code': 400})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : cors
        },
        'body' : body
    }
    return response