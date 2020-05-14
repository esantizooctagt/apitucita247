import sys
import logging
import json

import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr
# from dynamodb_json import json_util as json_dynamodb

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
    cors = "http://localhost:4200"
    # if stage['origin'] != "http://localhost:4200":
    #     cors = os.environ['prodCors']
    # else:
    #     cors = os.environ['devCors']

    records =[]
    try:
        roleId = event['pathParameters']['id']
        businessId = event['pathParameters']['businessId']
        if roleId == '0':
            e = {'#s': 'STATUS'}
            f = '#s = :stat'
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
            for row in response['Items']:
                recordset = {
                    'Application_Id': row['APPLICATIONID'],
                    'Name': row['NAME'],
                    'Active': row['ACTIVE'],
                    'Icon': row['ICON'],
                    'Route': row['ROUTE']
                }
                records.append(recordset)
        else:
            response = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :businessId AND begins_with( SKID , :roleId )',
                ExpressionAttributeValues={
                    ':businessId': {'S': 'BUS#' + businessId},
                    ':roleId': {'S': 'ROL#' + roleId}
                }
            )
            for line in response['Items']:
                response = dynamodb.query(
                    TableName="TuCita247",
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='PKID = :apps AND SKID = :appId',
                    ExpressionAttributeNames=e,
                    FilterExpression=f,
                    Limit=1,
                    ExpressionAttributeValues={
                        ':apps': {'S': 'APPS'},
                        ':appId': {'S': line['SKID'].replace('ROL#'+roleId+'#','')},
                        ':stat' : {'N': '1'}
                    }
                )
                for row in response['Items']:
                    recordset = {
                        'Application_Id': row['APPLICATIONID'],
                        'Name': row['NAME'],
                        'Active': row['ACTIVE'],
                        'Icon': row['ICON'],
                        'Route': row['ROUTE']
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