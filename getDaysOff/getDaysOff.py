import sys
import logging
import json

import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr
from dynamodb_json import json_util as json_dynamodb

from datetime import datetime

import os

REGION = 'us-east-1'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.client('dynamodb', region_name=REGION)
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def validDayOff(daysOff, start_date, end_date):
    validDays = []
    for d in daysOff:
        if d >= start_date and d <= end_date:
            validDays.append(d.strftime("%Y-%m-%d"))
    return validDays

def lambda_handler(event, context):
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']
        
    try:
        businessId = event['pathParameters']['businessId']
        locationId = event['pathParameters']['locationId']
        providerId = event['pathParameters']['providerId']
        year = str(event['pathParameters']['year'])

        start_date = datetime.strptime(year+'-01-01', '%Y-%m-%d')
        end_date = datetime.strptime(year+'-12-31', '%Y-%m-%d')
        if locationId == '_':
            response = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :businessId AND SKID = :metadata',
                ExpressionAttributeValues={
                    ':businessId': {'S': 'BUS#' + businessId},
                    ':metadata': {'S': 'METADATA'}
                },
            )
            record = []
            recordset = {}
            for row in json_dynamodb.loads(response['Items']):
                daysOffBus = [datetime.strptime(date, '%Y-%m-%d') for date in row['DAYS_OFF']] if 'DAYS_OFF' in row else []
                daysOffBus = validDayOff(daysOffBus, start_date, end_date)
                recordset = {
                    'BusinessId': row['PKID'].replace('BUS#',''),
                    'DaysOff': daysOffBus
                }
                record.append(recordset)

        if providerId == '_' and locationId != '_':
            locations = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :businessId AND begins_with( SKID , :metadata )',
                ExpressionAttributeValues={
                    ':businessId': {'S': 'BUS#' + businessId},
                    ':metadata': {'S': 'LOC#'}
                },
            )
            record = []
            recordset = {}
            for location in json_dynamodb.loads(locations['Items']):
                daysOffLoc = [datetime.strptime(date, '%Y-%m-%d') for date in location['DAYS_OFF']] if 'DAYS_OFF' in location else []
                daysOffLoc = validDayOff(daysOffLoc, start_date, end_date)
                recordset = {
                    'LocationId': location['SKID'].replace('LOC#',''),
                    'Name': location['NAME'],
                    'DaysOff': daysOffLoc,
                    'ParentDaysOff': location['PARENTDAYSOFF'] if 'PARENTDAYSOFF' in location else 0
                }
                record.append(recordset)
        
        if providerId != '_':
            locations = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :businessId AND begins_with( SKID , :metadata )',
                ExpressionAttributeValues={
                    ':businessId': {'S': 'BUS#' + businessId},
                    ':metadata': {'S': 'LOC#'}
                },
            )
            record = []
            locs = {}
            for location in json_dynamodb.loads(locations['Items']):
                servicesData = dynamodb.query(
                    TableName="TuCita247",
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='PKID = :businessId AND begins_with( SKID , :metadata )',
                    ExpressionAttributeValues={
                        ':businessId': {'S': 'BUS#' + businessId + '#LOC#' + location['SKID'].replace('LOC#','')},
                        ':metadata': {'S': 'PRO#'}
                    },
                )
                services = []
                recordset = {}
                for service in json_dynamodb.loads(servicesData['Items']):
                    daysOffServ = [datetime.strptime(date, '%Y-%m-%d') for date in service['DAYS_OFF']] if 'DAYS_OFF' in service else []
                    daysOffServ = validDayOff(daysOffServ, start_date, end_date)
                    recordset = {
                        'ProviderId': service['SKID'].replace('PRO#',''),
                        'Name': service['NAME'],
                        'DaysOff': daysOffServ,
                        'ParentDaysOff': service['PARENTDAYSOFF'] if 'PARENTDAYSOFF' in service else 0
                    }
                    services.append(recordset)
                
                locs = {
                    'LocationId': location['SKID'].replace('LOC#',''),
                    'Name': location['NAME'],
                    'Services': services
                }
                record.append(locs)
        
        statusCode = 200
        body = json.dumps({'Code': 200, 'Data': record})
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