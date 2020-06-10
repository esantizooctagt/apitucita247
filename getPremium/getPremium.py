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
    stage = event['headers']
    cors = stage['origin']
    
    try:
        planId = event['pathParameters']['planId']

        response = dynamodb.query(
            TableName="TuCita247",
            IndexName="TuCita247_CustAppos",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='GSI2PK = :key',
            ExpressionAttributeValues={
                ':key': {'S': 'PLAN#' + planId}
            }
        ) 

        record = []
        recordset = {}
        for row in json_dynamodb.loads(response['Items']):
            recordset = {
                'BusinessId': row['PKID'].replace('BUS#',''),
                'Name': row['NAME'],
                'Description': row['SHORTDESCRIPTION'],
                'ImgPath': row['IMGBUSINESS']
            }
            record.append(recordset)
        
        while 'LastEvaluatedKey' in response:
            lastItem = json_dynamodb.loads(response['LastEvaluatedKey'])
            
            response = dynamodb.query(
                TableName="TuCita247",
                IndexName="TuCita247_CustAppos",
                ExclusiveStartKey= lastItem,
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='GSI2PK = :key',
                ExpressionAttributeValues={
                    ':key': {'S': 'PLAN#' + planId}
                }
            )

            recordset = {}
            for row in json_dynamodb.loads(response['Items']):
                recordset = {
                    'BusinessId': row['PKID'].replace('BUS#',''),
                    'Name': row['NAME'],
                    'Description': row['SHORTDESCRIPTION'],
                    'ImgPath': row['IMGBUSINESS']
                }
                record.append(recordset)

        resultSet = { 
            'Code': 200,
            'Business': record
        }
        statusCode = 200
        body = json.dumps(resultSet)
    
        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Code': 500, 'Message': 'Error on load business premium'})
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