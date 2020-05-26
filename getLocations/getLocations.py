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
        businessId = event['pathParameters']['businessId']
        response = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :businessId AND begins_with( SKID , :locations )',
            ExpressionAttributeValues={
                ':businessId': {'S': 'BUS#' + businessId},
                ':locations': {'S': 'LOC#'}
            }
        )
        locations = json_dynamodb.loads(response['Items'])
               
        recordset = {}
        records = []
        for row in locations:
            recordset = {
                'BusinessId': row['PKID'].replace('BUS#',''),
                'LocationId': row['SKID'].replace('LOC#',''),
                'Name': row['NAME'],
                'Address': row['ADDRESS'],
                'Geolocation': row['GEOLOCATION'],
                'ParentLocation': row['PARENT_LOCATION'],
                'TotalPiesTransArea': row['TOTAL_TRANSITABLE_AREA'],
                'LocationDensity': row['LOCATION_DENSITY'],
                'MaxNumberEmployeesLocation': row['MAX_NUMBER_EMPLOYEES_LOC'],
                'MaxConcurrentCustomerLocation': row['MAX_CUSTOMER_LOC'],
                'Open': row['OPEN'],
                'BucketInterval': row['BUCKET_INTERVAL'],
                'TotalCustPerBucketInter': row['CUSTOMER_PER_BUCKET'],
                'OperationHours': row['OPERATIONHOURS'],
                'Doors': row['DOORS'],
                'Status': row['STATUS']
            }
            records.append(recordset)
        
        locs = {
            'Locations': records
        }
            
        statusCode = 200
        body = json.dumps(locs)
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