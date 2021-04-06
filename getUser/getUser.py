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
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']
        
    try:
        userId = event['pathParameters']['id']
        businessId = event['pathParameters']['businessId']
        response = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :businessId AND SKID = :userId',
            ExpressionAttributeValues={
                ':businessId': {'S': 'BUS#' + businessId},
                ':userId': {'S': 'USER#' + userId}
            }
        )
        recordset = ''
        for record in json_dynamodb.loads(response['Items']):
            recordset = {
                'User_Id': record['USERID'],
                'Email': record['GSI1PK'].replace('EMAIL#',''),
                'First_Name': record['FIRST_NAME'],
                'Last_Name': record['LAST_NAME'],
                'Avatar': record['AVATAR'] if "AVATAR" in record else '',
                'Phone': record['PHONE'],
                'CountryCode': record['COUNTRY'] if 'COUNTRY' in record else '',
                'Is_Admin': record['IS_ADMIN'],
                'Business_Id': record['PKID'].replace('BUS#',''),
                'Status': record['STATUS'],
                'Role_Id': '' if record['IS_ADMIN'] == 1 else record['ROLEID'],
                'Location_Id': record['LOCATIONID'] if 'LOCATIONID' in record else '', 
                'Language': '' if "LANGUAGE" not in record else record['LANGUAGE']
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