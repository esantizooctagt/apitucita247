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
    businessId = ''
    body = ''
    cors = ''
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']

    try:
        businessId = event['pathParameters']['id']
        response = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :businessId AND begins_with( SKID , :metadata )',
            ExpressionAttributeValues={
                ':businessId': {'S': 'BUS#' + businessId},
                ':metadata': {'S': 'METADATA'}
            }
        )
        itemsbusiness = json_dynamodb.loads(response['Items'])
       
       
        response = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :businessId AND begins_with( SKID , :category )',
            ExpressionAttributeValues={
                ':businessId': {'S': 'BUS#' + businessId},
                ':category': {'S': 'CAT#'}
            }
        )
        items = json_dynamodb.loads(response['Items'])
        records = []
        recordset1 = {}
        for row in items:
            recordset1 = {
                'CategoryId': row['SKID'].replace('CAT#',''),
                'Name': row['NAME']
            } 
            records.append(recordset1)
        
        recordset = {}
        for row in itemsbusiness:
            recordset = {
                'Business_Id': row['PKID'].replace('BUS#',''),
                'Name': row['NAME'],
                'Country': row['COUNTRY'],
                'Address': row['ADDRESS'],
                'City': row['CITY'],
                'ZipCode': row['ZIPCODE'],
                'Geolocation': row['GEOLOCATION'],
                'Phone': row['PHONE'],
                'WebSite': row['WEBSITE'],
                'Facebook': row['FACEBOOK'],
                'Twitter': row['TWITTER'],
                'Instagram': row['INSTAGRAM'],
                'Email': row['EMAIL'],
                'OperationHours': row['OPERATIONHOURS'],
                'Categories': records,
                'Tags': row['TAGS'],
                'Status': row['STATUS']
            }
            
        statusCode = 200
        body = json.dumps(recordset)
    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message':'Error on request try again'+ str(e)})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : cors
        },
        'body' : body
    }
    return response