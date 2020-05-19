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
        data = json.loads(event['body'])
        roleId = data['RoleId']
        
        recordList = {}
        i=0
        response = dynamodb.query(
            TableName = "TuCita247",
            ReturnConsumedCapacity = 'TOTAL',
            KeyConditionExpression = 'PKID = :businessId AND begins_with( SKID , :roles )',
            ExpressionAttributeValues = {
                ':businessId': {'S': 'BUS#' + data['BusinessId']},
                ':roles': {'S': 'ACCESS#' + roleId}
            }
        )

        appAct = []
        y=0
        for apps in response['Items']:
            row = json_dynamodb.loads(apps)
            appAct.append(row['SKID'].replace('ACCESS#'+roleId,''))
            y=y+1
        
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
                    "PKID": {"S": 'BUS#' + data['BusinessId'] },
                    "SKID": {"S": 'ROL#' + roleId }
                },
                "UpdateExpression":"set #n = :name, #s = :status",
                "ExpressionAttributeNames": { '#n': 'NAME', '#s': 'STATUS' },
                "ExpressionAttributeValues": { 
                    ":name": {"S": data['Name']},
                    ":status": {"N": str(data['Status'])} 
                },
                "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
            },
        }
        items.append(rows)
        
        for j in range(y):
            rows = {
                "Delete":{
                    "TableName":"TuCita247",
                    "Key": {
                        "PKID": {"S": 'BUS#'+data['BusinessId']},
                        "SKID": {"S": 'begins_with (SKID , :role)'}
                    },
                    "ExpressionAttributeValues": { 
                        ":role": {"S": str('ACCESS#' + roleId + '#' + appAct[j])}
                    },
                    "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
                },
            }
            items.append(rows)
            logger.info(rows)

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