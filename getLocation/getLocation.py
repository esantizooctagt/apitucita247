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
        businessId = event['pathParameters']['businessId']
        locationId = event['pathParameters']['locationId']
        country = event['pathParameters']['country']
        language = event['pathParameters']['language']

        response = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :businessId AND SKID = :locations',
            ExpressionAttributeValues={
                ':businessId': {'S': 'BUS#' + businessId},
                ':locations': {'S': 'LOC#' + locationId}
            }
        )
        recordset = ''
        for row in json_dynamodb.loads(response['Items']):
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
                'Name': row['NAME'] if 'NAME' in row else '',
                'Address': row['ADDRESS'] if 'ADDRESS' in row else '',
                'City': row['CITY'] if 'CITY' in row else '',
                'ZipCode': row['ZIPCODE'] if 'ZIPCODE' in row else '',
                'Sector': row['SECTOR'] if 'SECTOR' in row else '0',
                'Geolocation': row['GEOLOCATION'] if 'GEOLOCATION' in row else '',
                'ParentLocation': row['PARENT_LOCATION'] if 'PARENT_LOCATION' in row else '',
                'MaxConcurrentCustomer': row['MAX_CUSTOMER'] if 'MAX_CUSTOMER' in row else 0,
                'Doors': row['DOORS'] if 'DOORS' in row else '',
                'Status': row['STATUS'] if 'STATUS' in row else 0,
                'ManualCheckOut': row['MANUAL_CHECK_OUT'] if 'MANUAL_CHECK_OUT' in row else 0,
                'TimeZone': row['TIME_ZONE'] if 'TIME_ZONE' in row else '',
                'Sectors': items
            }
            
        statusCode = 200
        body = json.dumps({'Code': 200, 'Data': recordset})
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