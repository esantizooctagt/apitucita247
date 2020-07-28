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

dynamodb = boto3.client('dynamodb', region_name='us-east-1')
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
        businessId = str(event['pathParameters']['businessId'])
        year = str(event['pathParameters']['year'])

        start_date = datetime.strptime(year+'-01-01', '%Y-%m-%d')
        end_date = datetime.strptime(year+'-12-31', '%Y-%m-%d')
        response = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :businessId AND SKID = :metadata',
            ExpressionAttributeValues={
                ':businessId': {'S': 'BUS#' + businessId},
                ':metadata': {'S': 'METADATA'}
            },
        )
        for row in json_dynamodb.loads(response['Items']):
            locations = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :businessId AND begins_with( SKID , :metadata )',
                ExpressionAttributeValues={
                    ':businessId': {'S': 'BUS#' + businessId},
                    ':metadata': {'S': 'LOC#'}
                },
            )
            locs = []
            rowsLocs = {}
            for location in json_dynamodb.loads(locations['Items']):
                services = dynamodb.query(
                    TableName="TuCita247",
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='PKID = :businessId AND begins_with( SKID , :metadata )',
                    ExpressionAttributeValues={
                        ':businessId': {'S': 'BUS#' + businessId + '#' + location['SKID'].replace('LOC#','')},
                        ':metadata': {'S': 'SER#'}
                    },
                )
                servs = []
                rowsServs = {}
                for service in json_dynamodb.loads(services['Items']):
                    daysOffServ = [datetime.strptime(date, '%Y-%m-%d') for date in service['DAYS_OFF']] if 'DAYS_OFF' in service else []
                    daysOffServ = validDayOff(daysOffServ, start_date, end_date)
                    rowsServs = {
                        'ServiceId': service['SKID'].replace('SER#',''),
                        'DaysOff': daysOffServ,
                        'ParentDaysOff': service['PARENTDAYSOFF'] if 'PARENTDAYSOFF' in service else 0
                    }
                    servs.append(rowsServs)

                daysOffLoc = [datetime.strptime(date, '%Y-%m-%d') for date in location['DAYS_OFF']] if 'DAYS_OFF' in location else []
                daysOffLoc = validDayOff(daysOffLoc, start_date, end_date)
                rowsLocs = {
                    'LocationId': location['SKID'].replace('LOC#',''),
                    'DaysOff': daysOffLoc,
                    'ParentDaysOff': location['PARENTDAYSOFF'] if 'PARENTDAYSOFF' in location else 0,
                    'Services': servs
                }
                locs.append(rowsLocs)

            daysOffBus = [datetime.strptime(date, '%Y-%m-%d') for date in row['DAYS_OFF']] if 'DAYS_OFF' in row else []
            daysOffBus = validDayOff(daysOffBus, start_date, end_date)
            recordset = {
                'BusinessId': row['PKID'].replace('BUS#',''),
                'DaysOff': daysOffBus,
                'Locations': locs
            }
        
        statusCode = 200
        body = json.dumps({'Code': 200, 'Business': json.dumps(recordset)})
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