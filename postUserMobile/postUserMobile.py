import sys
import logging
import json

import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr
from dynamodb_json import json_util as json_dynamodb

import uuid
import os
import random

REGION = 'us-east-1'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.client('dynamodb', region_name=REGION)
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
    try:
        statusCode = ''
        customerId = str(uuid.uuid4()).replace("-","")
        data = json.loads(event['body'])

        items = []
        recordset = {}

        if data['CustId'] != '':
            oldRecord = {}
            oldItems = []
            oldRecord = {
                "Delete": {
                    "TableName": "TuCita247",
                    "Item": {
                        "PKID": {"S": 'MOB#' + data['Phone']},
                        "SKID": {"S": 'MOB#' + data['Phone']}
                    },
                    "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
                    }
                }
            oldItems.append(cleanNullTerms(oldRecord))

            oldRecord = {
                "Delete": {
                    "TableName": "TuCita247",
                    "Key": {
                        "PKID": {"S": 'MOB#' + data['Phone']},
                        "SKID": {"S": 'CUS#' + data['CustId']}
                    },
                    "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
                    }
                }
            oldItems.append(cleanNullTerms(oldRecord))

            responseOld = dynamodb.transact_write_items(
                TransactItems = oldItems
            )

        recordset = {
            "Put": {
                "TableName": "TuCita247",
                "Item": {
                    "PKID": {"S": 'MOB#' + data['Phone'] },
                    "SKID": {"S": 'CUS#' + customerId},
                    "GSI1PK": {"S": 'CUS#' + customerId},
                    "GSI1SK": {"S": 'CUS#' + customerId},
                    "NAME": {"S": data['Name']},
                    "EMAIL": {"S": str(data['Email']) if str(data['Email']) != '' else None},
                    "DOB": {"S": str(data['DOB']) if str(data['DOB']) != '' else None},
                    "GENDER": {"S": str(data['Gender']) if str(data['Gender']) != '' else None},
                    "PREFERENCES": {"N": str(data['Preferences']) if str(data['Preferences']) != '' else None},
                    "DISABILITY": {"N": str(data['Disability']) if str(data['Disability']) != '' else None},
                    "PLAYERID": {"S": data['PlayerId'] if data['PlayerId'] != '' else None},
                    "STATUS": {"N": "1"}
                },
                "ConditionExpression": "attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
                }
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
            TransactItems = items
        )

        response = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :mobile AND SKID = :customer',
            ExpressionAttributeValues={
                ':mobile': {'S': 'MOB#' + data['Phone'] },
                ':customer': {'S': 'CUS#' + customerId}
            },
            Limit = 1
        )
        recordset = {}
        for item in json_dynamodb.loads(response['Items']):
            recordset = {
                'CustomerId': item['SKID'].replace('CUS#',''),
                'Status': item['STATUS'],
                'Name': item['NAME'],
                'Gender': item['GENDER'] if 'GENDER' in item else '',
                'Email': item['EMAIL'] if 'EMAIL' in item else '',
                'Preferences': item['PREFERENCES'] if 'PREFERENCES' in item else '',
                'Disability': item['DISABILITY'] if 'DISABILITY' in item else '',
                'DOB': item['DOB'] if 'DOB' in item else '',
                'Mobile': data['Phone']
            }

        statusCode = 200
        body = json.dumps({'Message': 'User added successfully','Customer': recordset, 'Code': 200})

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
            "access-control-allow-origin" : "*"
        },
        'body' : body
    }
    return response