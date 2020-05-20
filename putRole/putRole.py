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
        updApp = []
        y=0
        for apps in response['Items']:
            row = json_dynamodb.loads(apps)
            appId = row['SKID'].replace('ACCESS#'+roleId+'#','')
            encontro = 0
            for items in data['Access']:
                if items['ApplicationId'] == appId:
                    encontro = 1
                    updApp.append(items['ApplicationId'])
                    break
            if encontro == 0:
                appAct.append(row['SKID'].replace('ACCESS#'+roleId+'#',''))
                y=y+1
        
        for items in data['Access']:
            entro = 0
            for upApp in updApp:
                if upApp == items['ApplicationId']:
                    entro = 1
                    break
            
            if entro == 0:
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
            else:
                recordset = {
                                "Put": {
                                    "TableName": "TuCita247",
                                    "Item": {
                                        "PKID": {"S": 'BUS#'+data['BusinessId']},
                                        "SKID": {"S": 'ACCESS#'+roleId+'#'+items['ApplicationId']},
                                        "LEVEL_ACCESS": {"N": str(items['Level_Access'])}
                                    },
                                    "ConditionExpression": "attribute_exists(PKID) AND attribute_exists(SKID)",
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
            deletes = {}
            deletes = {
                "Delete":{
                    "TableName":"TuCita247",
                    "Key": {
                        "PKID": {"S": 'BUS#'+data['BusinessId']},
                        "SKID": {"S": 'ACCESS#' + roleId + '#' + appAct[j]}
                    },
                    "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
                },
            }
            items.append(deletes)
            logger.info(deletes)
            
        for x in range(i):
            items.append(recordList[x])    
        
        logger.info(items)
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