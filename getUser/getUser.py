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
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']
        
    try:
        userId = event['pathParameters']['id']
        businessId = event['pathParameters']['businessId']
        logger.info("prev query")
        response = dynamodb.query(
            TableName="TuCita247",
            IndexName="TuCita247_Index",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :businessId AND SKID = :userId',
            ExpressionAttributeValues={
                ':businessId': {'S': 'BUS#' + businessId},
                ':userId': {'S': 'USER#' + userId}
            },
            Limit=1
        )
        logger.info(response)
        for row in response['Items']:
            record = json_dynamodb.loads(row)
            recordset = {
                'User_Id': record['USERID'],
                'Email': record['GSI1PK'].replace('EMAIL#',''),
                'First_Name': record['FIRST_NAME'],
                'Last_Name': record['LAST_NAME'],
                'Avatar': record['AVATAR'],
                'Location_Id': record['LOCATIONID'],
                'Is_Admin': record['IS_ADMIN'],
                'Company_Id': record['PKID'].replace('BUS#',''),
                'Status': record['STATUS'],
                'Role_Id': '' if record['IS_ADMIN'] == 1 else record['ROLEID'],
                'MFact_Auth': record['MFACT_AUTH'],
                'Language_Id': record['LANGUAGE']
            }
        statusCode = 200
        body = json.dumps(recordset)
                    
    except Exception as e:
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