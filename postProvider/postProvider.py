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
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']
        
    try:
        statusCode = ''
        providerId = str(uuid.uuid4()).replace("-","")
        data = json.loads(event['body'])

        items = []
        recordset = {}
        if data['ProviderId'] == '':
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

            for service in data['Services']:
                if int(service['Selected']) == 1:
                    recordset = {
                        "Put": {
                            "TableName": "TuCita247",
                            "Item": {
                                "PKID": {"S": 'BUS#' + data['BusinessId'] + '#SER#' + service['ServiceId']},
                                "SKID": {"S": 'PRO#' + providerId},
                                "GSI1PK": {"S": 'BUS#' + data['BusinessId'] + '#PRO#' + providerId},
                                "GSI1SK": {"S": 'SER#' + service['ServiceId']}
                            },
                            "ConditionExpression": "attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                            "ReturnValuesOnConditionCheckFailure": "NONE"
                        },
                    }
                    items.append(cleanNullTerms(recordset))

            recordset = {
                "Put": {
                    "TableName": "TuCita247",
                    "Item": {
                        "PKID": {"S": 'BUS#' + data['BusinessId'] + '#LOC#' + data['LocationId']},
                        "SKID": {"S": 'PRO#' + providerId},
                        "GSI1PK": {"S": 'BUS#' + data['BusinessId']},
                        "GSI1SK": {"S": 'PRO#' + providerId},
                        "NAME": {"S": data['Name']},
                        "OPERATIONHOURS": {"S": opeHours},
                        "DAYS_OFF": {"L": resDays if resDays != [] else None},
                        "PARENTDAYSOFF": {"N": str(1)},
                        "PARENTHOURS": {"N": str(1)},
                        "STATUS": {"N": str(data['Status'])}
                    },
                    "ConditionExpression": "attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                    "ReturnValuesOnConditionCheckFailure": "NONE"
                },
            }
        else:
            providerId = data['ProviderId']
            for service in data['Services']:
                response = dynamodb.query(
                    TableName="TuCita247",
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='PKID = :businessId AND SKID = :prov',
                    ExpressionAttributeValues={
                        ':businessId': {'S': 'BUS#' + data['BusinessId'] + '#SER#' + service['ServiceId']},
                        ':prov': {'S': 'PRO#' + providerId}
                    }
                )
                count = 0
                for row in json_dynamodb.loads(response['Items']):
                    count = 1
                    if int(service['Selected']) == 0:
                        recordset = {
                            "Delete": {
                                "TableName": "TuCita247",
                                "Key": {
                                    "PKID": {"S": 'BUS#' + data['BusinessId'] + '#SER#' + service['ServiceId']},
                                    "SKID": {"S": 'PRO#' + providerId}
                                },
                                "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
                            }
                        }
                        items.append(cleanNullTerms(recordset))

                if count == 0 and int(service['Selected']) == 1:
                    recordset = {
                        "Put": {
                            "TableName": "TuCita247",
                            "Item": {
                                "PKID": {"S": 'BUS#' + data['BusinessId'] + '#SER#' + service['ServiceId']},
                                "SKID": {"S": 'PRO#' + providerId},
                                "GSI1PK": {"S": 'BUS#' + data['BusinessId'] + '#PRO#' + providerId},
                                "GSI1SK": {"S": 'SER#' + service['ServiceId']}
                            },
                            "ConditionExpression": "attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                            "ReturnValuesOnConditionCheckFailure": "NONE"
                        },
                    }
                    items.append(cleanNullTerms(recordset))

            recordset = {
                "Update": {
                    "TableName": "TuCita247",
                    "Key": {
                        "PKID": {"S": 'BUS#' + data['BusinessId'] + '#LOC#' + data['LocationId']},
                        "SKID": {"S": 'PRO#' + providerId}
                    },
                    "UpdateExpression": "SET #n = :name,  #s = :status",
                    "ExpressionAttributeValues": {
                        ':name': {'S': data['Name']},
                        ':status': {'N': str(data['Status'])}
                    },
                    "ExpressionAttributeNames": {'#s': 'STATUS','#n': 'NAME'},
                    "ConditionExpression": "attribute_exists(PKID) AND attribute_exists(SKID)",
                    "ReturnValuesOnConditionCheckFailure": "NONE"
                },
            }
        items.append(cleanNullTerms(recordset))
        
        logger.info(items)
        response = dynamodb.transact_write_items(
            TransactItems = items
        )
        statusCode = 200
        body = json.dumps({'Message': 'Service provider added successfully', 'ProviderId': providerId, 'Code': 200})

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