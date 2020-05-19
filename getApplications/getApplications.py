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

dynamodb = boto3.client('dynamodb', region_name='us-east-1')
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):
    stage = ''
    roleId = ''
    body = ''
    cors = ''
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']

    records =[]
    try:
        roleId = event['pathParameters']['id']
        businessId = event['pathParameters']['businessId']
        
        e = {'#s': 'STATUS'}
        f = '#s = :stat'
        if roleId == '0':
            response = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :apps',
                ExpressionAttributeNames=e,
                FilterExpression=f,
                ExpressionAttributeValues={
                    ':apps': {'S': 'APPS'},
                    ':stat' : {'N': '1'}
                }
            )
            items = json_dynamodb.loads(response['Items'])
            for row in items:
                recordset = {
                    'ApplicationId': row['SKID'],
                    'Name': row['NAME'],
                    'Active': 0,
                    'Icon': row['ICON'],
                    'Route': row['ROUTE']
                }
                records.append(recordset)
        elif roleId == '1':
            response = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :apps',
                ExpressionAttributeNames=e,
                FilterExpression=f,
                ExpressionAttributeValues={
                    ':apps': {'S': 'APPS'},
                    ':stat' : {'N': '1'}
                }
            )
            items = json_dynamodb.loads(response['Items'])
            for row in items:
                recordset = {
                    'ApplicationId': row['SKID'],
                    'Name': row['NAME'],
                    'Active': 1,
                    'Icon': row['ICON'],
                    'Route': row['ROUTE']
                }
                records.append(recordset)
        else:
            response = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :apps',
                ExpressionAttributeNames=e,
                FilterExpression=f,
                Limit=1,
                ExpressionAttributeValues={
                    ':apps': {'S': 'APPS'},
                    ':stat' : {'N': '1'}
                }
            )
            #  AND SKID = :appId     ':appId': {'S': app['SKID'].replace('ACCESS#'+roleId+'#','')},
            for line in response['Items']:
                apps = json_dynamodb.loads(line)
                response02 = dynamodb.query(
                    TableName="TuCita247",
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='PKID = :businessId AND begins_with( SKID , :roleId )',
                    ExpressionAttributeValues={
                        ':businessId': {'S': 'BUS#' + businessId},
                        ':roleId': {'S': 'ACCESS#' + roleId + "#" + apps['SKID']}
                        }
                )
                for row in items:
                    items = json_dynamodb.loads(response02['Items'])
                    recordset = {
                        'ApplicationId': apps['SKID'],
                        'Name': apps['NAME'],
                        'Level_Access': row['LEVEL_ACCESS'],
                        'Icon': apps['ICON'],
                        'Route': apps['ROUTE']
                    }
                if response02['Count'] == 0:
                     recordset = {
                        'ApplicationId': apps['SKID'],
                        'Name': apps['NAME'],
                        'Level_Access': 0,
                        'Icon': apps['ICON'],
                        'Route': apps['ROUTE']
                    }
                records.append(recordset)
 
        statusCode = 200
        body = json.dumps(records)
    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message': 'Error on request try again ' + str(e)})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "Access-Control-Allow-Origin" : cors
        },
        'body' : body
    }
    return response