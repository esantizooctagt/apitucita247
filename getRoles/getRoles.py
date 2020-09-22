import sys
import logging
import json

import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr
from dynamodb_json import json_util as json_dynamodb

import os

REGION = 'us-east-1'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.client('dynamodb', region_name=REGION)
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):
    records =[]
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']
        
    try:

        businessId = event['pathParameters']['businessId']
        items = int(event['pathParameters']['items'])
        lastItem = event['pathParameters']['lastItem']
        search = event['pathParameters']['search']
        salir = 0

        e = {'#s': 'STATUS'}
        # a = {':businessId': {'S': 'BUS#' + businessId}, ':stat': {'N': '2'}, ':roles': {'S':'ROL#'}, ':super': {'N': '0'}}
        f = '#s < :stat and SUPER_ADMIN = :super'
        if search != '_':
            e = {'#s': 'STATUS', '#n': 'NAME'}
            f = '#s < :stat and begins_with (#n , :search) and SUPER_ADMIN = :super'
            # a = {':businessId': {'S': 'BUS#' + businessId}, ':stat': {'N': '2'}, ':roles': {'S':'ROL#'}, ':search': {'S': search}, ':super': {'N': '0'}}

        if lastItem == '_':
            lastItem = ''
        else:
            if lastItem == '':
                salir = 1
            else:
                lastItem = {'PKID': {'S': 'BUS#' + businessId },'SKID': {'S': 'ROL#' + lastItem }}

        if salir == 0:
            if lastItem == '':
                response = dynamodb.query(
                    TableName="TuCita247",
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='PKID = :businessId AND begins_with ( SKID , :role )',
                    ExpressionAttributeNames=e,
                    FilterExpression=f,
                    ExpressionAttributeValues={
                        ':businessId': {'S': 'BUS#' + businessId},
                        ':role': {'S': 'ROL#'},
                        ':stat' : {'N': '2'},
                        ':super': {'N': '0'}
                    },
                    Limit=items
                )
            else:
                response = dynamodb.query(
                    TableName="TuCita247",
                    ReturnConsumedCapacity='TOTAL',
                    ExclusiveStartKey= lastItem,
                    KeyConditionExpression='PKID = :businessId AND begins_with ( SKID , :role )',
                    ExpressionAttributeNames=e,
                    FilterExpression=f,
                    ExpressionAttributeValues={
                        ':businessId': {'S': 'BUS#' + businessId},
                        ':role': {'S': 'ROL#'},
                        ':stat' : {'N': '2'},
                        ':super': {'N': '0'}
                    },
                    Limit=items
                )
        recordset = {}
        for row in json_dynamodb.loads(response['Items']):
            recordset = {
                'Role_Id': row['SKID'].replace('ROL#',''),
                'Business_Id': row['PKID'].replace('BUS#',''),
                'Name': row['NAME']
            }
            records.append(recordset)
        
        if 'LastEvaluatedKey' in response:
            lastItem = json_dynamodb.loads(response['LastEvaluatedKey'])
            lastItem = lastItem['SKID'].replace('POLL#','')

        resultSet = { 
            'lastItem': lastItem,
            'roles': records
        }

        statusCode = 200
        body = json.dumps(resultSet)
    except: #Exception as e:
        statusCode = 500
        body = json.dumps({'Message': 'Error on request try again'})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : cors
        },
        'body' : body
    }
    return response