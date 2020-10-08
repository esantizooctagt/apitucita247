import sys
import logging
import json

import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr
from dynamodb_json import json_util as json_dynamodb

import datetime
import dateutil.tz
from datetime import timezone

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
    
    country_date = dateutil.tz.gettz('America/Puerto_Rico')
    today = datetime.datetime.now(tz=country_date)
    dateOpe = today.strftime("%Y-%m-%d")

    try:
        statusCode = ''
        businessId = event['pathParameters']['businessId']
        userId = event['pathParameters']['userId']

        response = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :businessId AND begins_with( SKID , :userId )',
            ExpressionAttributeValues={
                ':businessId': {'S': 'BUS#' + businessId },
                ':userId': {'S': 'USER#'+ userId }
            }
        )
        recordset = {}
        locationId = ''
        door = '' 
        for row in json_dynamodb.loads(response['Items']):
            locationId = row['LOCATIONID'] if 'LOCATIONID' in row else ''
            door = row['DOOR'] if 'DOOR' in row else ''
            locations = []
            if locationId != '':
                locs = dynamodb.query(
                    TableName="TuCita247",
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='PKID = :businessId AND SKID = :locationId',
                    ExpressionAttributeValues={
                        ':businessId': {'S': 'BUS#' + businessId},
                        ':locationId': {'S': 'LOC#' + locationId}
                    }
                )
                providers = []
                for loc in json_dynamodb.loads(locs['Items']):
                    provs = dynamodb.query(
                        TableName="TuCita247",
                        ReturnConsumedCapacity='TOTAL',
                        KeyConditionExpression='PKID = :businessId AND begins_with(SKID , :locationId)',
                        ExpressionAttributeValues={
                            ':businessId': {'S': 'BUS#' + businessId + '#LOC#'+ locationId },
                            ':locationId': {'S': 'PRO#' }
                        }
                    )
                    for item in json_dynamodb.loads(provs['Items']):
                        recordset = {
                            'ProviderId': item['SKID'].replace('PRO#',''),
                            'Name': item['NAME']
                        }
                        providers.append(recordset)

                    if 'OPEN_DATE' in loc and 'OPEN' in loc:
                        if loc['OPEN_DATE'][0:10] < dateOpe and loc['OPEN'] == 1:
                            open = 1
                            closed = 1
                        if loc['OPEN_DATE'][0:10] == dateOpe or loc['OPEN_DATE'] == '':
                            open = loc['OPEN']
                            closed = 0
                    else:
                        open = 0
                        closed = 0
                    
                    recordset = {
                        'LocationId': locationId,
                        'Providers': providers,
                        'Door': door,
                        'Name': loc['NAME'],
                        'MaxCustomers': loc['MAX_CUSTOMER'],
                        'ManualCheckOut': loc['MANUAL_CHECK_OUT'],
                        'Open': open,
                        'Closed': closed
                    }
                    locations.append(recordset)
            else:
                locs = dynamodb.query(
                    TableName="TuCita247",
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='PKID = :businessId AND begins_with(SKID, :locs)',
                    ExpressionAttributeValues={
                        ':businessId': {'S': 'BUS#' + businessId },
                        ':locs': {'S': 'LOC#'}
                    }
                )
                for loc in json_dynamodb.loads(locs['Items']):
                    if 'OPEN_DATE' in loc and 'OPEN' in loc:
                        if loc['OPEN_DATE'][0:10] < dateOpe and loc['OPEN'] == 1:
                            open = 1
                            closed = 1
                        if loc['OPEN_DATE'][0:10] == dateOpe or loc['OPEN_DATE'] == '':
                            open = loc['OPEN']
                            closed = 0
                    else:
                        open = 0
                        closed = 0

                    serv = dynamodb.query(
                        TableName="TuCita247",
                        ReturnConsumedCapacity='TOTAL',
                        KeyConditionExpression='PKID = :key01 AND begins_with(SKID, :provs)',
                        ExpressionAttributeValues={
                            ':key01': {'S': 'BUS#' + businessId + '#' + loc['SKID']},
                            ':provs': {'S': 'PRO#'}
                        }
                    )
                    providers = []
                    for result in json_dynamodb.loads(serv['Items']):
                        recordset = {
                            'ProviderId': result['SKID'].replace('PRO#',''),
                            'Name': result['NAME']
                        }
                        providers.append(recordset)

                    recordset = {
                        'LocationId': loc['SKID'].replace('LOC#',''),
                        'Providers': providers,
                        'Door': door,
                        'Name': loc['NAME'],
                        'MaxCustomers': loc['MAX_CUSTOMER'],
                        'ManualCheckOut': loc['MANUAL_CHECK_OUT'],
                        'Open': open,
                        'Closed': closed
                    }
                    locations.append(recordset)

            statusCode = 200
            body = json.dumps({'Code': 200, 'Locs': locations})
    
        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Code': 500, 'Message': 'Error on load locations by user'})
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