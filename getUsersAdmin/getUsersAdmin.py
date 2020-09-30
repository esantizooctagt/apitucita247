import sys
import logging
import json
import math
import decimal
import os

import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr
from dynamodb_json import json_util as json_dynamodb

REGION = 'us-east-1'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.client('dynamodb', region_name=REGION)
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):
    items = 0
    search = ''
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']
    records =[]
    try:
        businessId = event['pathParameters']['businessId']
        items = int(event['pathParameters']['perPage'])
        search = event['pathParameters']['searchValue']
        lastItem = event['pathParameters']['lastItem']
        
        if search == '_':
            search = ''
            e = {'#s': 'STATUS'}
            a = {':businessId': {'S': 'BUS#' + businessId},':userId': {'S': 'USER#'},':stat' : {'N': '2'}, ':super' : {'N': '1'}}
            f = '#s <> :stat and SUPER_ADMIN = :super'
        else:
            e = {'#s': 'STATUS','#n': 'FIRST_NAME'}
            a = {':businessId': {'S': 'BUS#' + businessId},':userId': {'S': 'USER#'},':search': {'S': search}, ':stat' : {'N': '2'}, ':super' : {'N': '1'}}
            f = '#s <> :stat AND contains ( #n , :search ) and SUPER_ADMIN = :super'
            
        if lastItem == '_':
            lastItem = ''
        else:
            lastItem = {'PKID': {'S': 'BUS#' + businessId },'SKID': {'S': 'USER#' + lastItem }}

        if lastItem == '':
            response = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :businessId AND begins_with( SKID , :userId )',
                ExpressionAttributeNames=e,
                ExpressionAttributeValues=a,
                FilterExpression=f,
                Limit=items
            )
        else:
            response = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                ExclusiveStartKey= lastItem,
                KeyConditionExpression='PKID = :businessId AND begins_with( SKID , :userId )',
                ExpressionAttributeNames=e,
                ExpressionAttributeValues=a,
                FilterExpression=f,
                Limit=items
            )
        
        for row in response['Items']:
            row = json_dynamodb.loads(row)
            recordset = {
                'User_Id': row['USERID'],
                'First_Name': row['FIRST_NAME'],
                'Last_Name': row['LAST_NAME'],
                'Email': row['GSI1PK'].replace('EMAIL#',''),
                'Business_Id': row['PKID'].replace('BUS#',''),
                'Status': row['STATUS']
            }
            records.append(recordset)
            
        if records != []:
            lastItem = ''
            if 'LastEvaluatedKey' in response:
                lastItem = json_dynamodb.loads(response['LastEvaluatedKey'])
                lastItem = lastItem['SKID'].replace('USER#','')
                
            resultSet = { 
                'lastItem': lastItem,
                'users': records
            }
            statusCode = 200
            body = json.dumps(resultSet)
        else:
            statusCode = 404
            body = json.dumps({'Message':'No valid users on your request'})
    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message': 'Error on request try again ' +str(e)})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : cors
        },
        'body' : body
    }
    return response