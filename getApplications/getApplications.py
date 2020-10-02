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
        language = event['pathParameters']['language']
        
        e = {'#s': 'STATUS'}
        f = '#s = :stat'
        if roleId == '0' or roleId == '1':
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
            for row in json_dynamodb.loads(response['Items']):
                recordset = {
                    'ApplicationId': row['SKID'],
                    'Name': row['NAME'] if language == 'EN' else row['NAME_ESP'],
                    'Active': 0 if roleId == '0' else 1,
                    'Icon': row['ICON'],
                    'Route': row['ROUTE'],
                    'OrderApp': row['ORDERAPP']
                }
                records.append(recordset)
        else:
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
            for apps in json_dynamodb.loads(response['Items']):
                response02 = dynamodb.query(
                    TableName="TuCita247",
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='PKID = :businessId AND begins_with( SKID , :roleId )',
                    ExpressionAttributeValues={
                        ':businessId': {'S': 'BUS#' + businessId},
                        ':roleId': {'S': 'ACCESS#' + roleId + "#" + apps['SKID']}
                        }
                )
                
                if response02['Count'] == 0:
                    recordset = {
                        'ApplicationId': apps['SKID'],
                        'Name': apps['NAME'] if language == 'EN' else apps['NAME_ESP'],
                        'Level_Access': '0',
                        'Active': '0',
                        'OrderApp': apps['ORDERAPP']
                    }
                    records.append(recordset)
                    
                for row in response02['Items']:
                    items = json_dynamodb.loads(row)
                    recordset = {
                        'ApplicationId': apps['SKID'],
                        'Name': apps['NAME'] if language == 'EN' else apps['NAME_ESP'],
                        'Level_Access': str(items['LEVEL_ACCESS']),
                        'Active': '1',
                        'Route': apps['ROUTE'],
                        'OrderApp': apps['ORDERAPP']
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