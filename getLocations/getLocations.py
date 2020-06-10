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
        country = event['pathParameters']['country']
        language = event['pathParameters']['language']
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
            sectors = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :city',
                ExpressionAttributeValues={
                    ':city': {'S': 'COUNTRY#' + country + '#CITY#' + row['CITY']}
                }
            )
            items = []
            for det in json_dynamodb.loads(sectors['Items']):
                rows = {
                    'SectorId': det['SKID'].replace('SECTOR#',''),
                    'Name': det['NAME_ENG'] if language == 'EN' else det['NAME_ESP']
                }
                items.append(rows)

            recordset = {
                'BusinessId': row['PKID'].replace('BUS#',''),
                'LocationId': row['SKID'].replace('LOC#',''),
                'Name': row['NAME'],
                'Address': row['ADDRESS'],
                'City': row['CITY'],
                'Sector': row['SECTOR'] if 'SECTOR' in row else '0',
                'Geolocation': row['GEOLOCATION'],
                'ParentLocation': row['PARENT_LOCATION'],
                'TotalPiesTransArea': row['TOTAL_TRANSITABLE_AREA'],
                'LocationDensity': row['LOCATION_DENSITY'],
                'MaxNumberEmployeesLocation': row['MAX_NUMBER_EMPLOYEES_LOC'],
                'MaxConcurrentCustomerLocation': row['MAX_CUSTOMER_LOC'],
                'BucketInterval': row['BUCKET_INTERVAL'],
                'TotalCustPerBucketInter': row['CUSTOMER_PER_BUCKET'],
                'OperationHours': row['OPERATIONHOURS'],
                'Doors': row['DOORS'],
                'Status': row['STATUS'],
                'Sectors': items
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