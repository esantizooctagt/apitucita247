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
        businessId = event['pathParameters']['id']
        response = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :businessId AND begins_with( SKID , :metadata )',
            ExpressionAttributeValues={
                ':businessId': {'S': 'BUS#' + businessId},
                ':metadata': {'S': 'METADATA#'}
            },
            Limit=1
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

        records =[]
        for row in items:
            recordset1 = {
                'Category_Id': row['SKID'].replace('CAT#',''),
                'Name': row['NAME']
            } 
            records.append(recordset1)
         
        for row in itemsbusiness:
            recordset = {
                'Business_Id': row['SKID'].replace('BUS#',''),
                'Name': row['NAME'],
                'Address': row['ADDRESS'],
                'City': row['CITY'],
                'Contact': row['CONTACT'],
                'Country': row['COUNTRY'],
                'GeoLocation': row['GEOLOCATION'],
                'Categories':  records
            }
        
        statusCode = 200
        body = json.dumps(recordset)
    except:
        statusCode = 500
        body = json.dumps({'Message':'Error on request try again'})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : cors
        },
        'body' : body
    }
    return response