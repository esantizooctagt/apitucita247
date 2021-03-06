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
            locationId = row['LOCATIONID'] if 'LOCATIONID' in row else '0'
            door = row['DOOR'] if 'DOOR' in row else ''
            locations = []
            if locationId != '0' and locationId != '':
                locs = dynamodb.query(
                    TableName="TuCita247",
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='PKID = :businessId AND SKID = :locationId',
                    FilterExpression='#s = :stat',
                    ExpressionAttributeNames={'#s': 'STATUS'},
                    ExpressionAttributeValues={
                        ':businessId': {'S': 'BUS#' + businessId},
                        ':locationId': {'S': 'LOC#' + locationId},
                        ':stat' : {'N': '1'}
                    }
                )
                providers = []
                for loc in json_dynamodb.loads(locs['Items']):
                    provs = dynamodb.query(
                        TableName="TuCita247",
                        ReturnConsumedCapacity='TOTAL',
                        KeyConditionExpression='PKID = :businessId AND begins_with(SKID , :locationId)',
                        FilterExpression='#s = :stat',
                        ExpressionAttributeNames={'#s': 'STATUS'},
                        ExpressionAttributeValues={
                            ':businessId': {'S': 'BUS#' + businessId + '#LOC#'+ locationId },
                            ':locationId': {'S': 'PRO#' },
                            ':stat' : {'N': '1'}
                        }
                    )
                    for item in json_dynamodb.loads(provs['Items']):
                        recordset = {
                            'ProviderId': item['SKID'].replace('PRO#',''),
                            'Name': item['NAME']
                        }
                        providers.append(recordset)

                    recordset = {
                        'LocationId': locationId,
                        'Providers': providers,
                        'Door': door,
                        'Name': loc['NAME'],
                        'MaxCustomers': loc['MAX_CUSTOMER'],
                        'ManualCheckOut': loc['MANUAL_CHECK_OUT'],
                        'TimeZone': loc['TIME_ZONE'] if 'TIME_ZONE' in loc else 'America/Puerto_Rico',
                        'Open': loc['OPEN']
                    }
                    locations.append(recordset)
            else:
                locs = dynamodb.query(
                    TableName="TuCita247",
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='PKID = :businessId AND begins_with(SKID, :locs)',
                    FilterExpression='#s = :stat',
                    ExpressionAttributeNames={'#s': 'STATUS'},
                    ExpressionAttributeValues={
                        ':businessId': {'S': 'BUS#' + businessId },
                        ':locs': {'S': 'LOC#'},
                        ':stat' : {'N': '1'}
                    }
                )
                for loc in json_dynamodb.loads(locs['Items']):
                    serv = dynamodb.query(
                        TableName="TuCita247",
                        ReturnConsumedCapacity='TOTAL',
                        KeyConditionExpression='PKID = :key01 AND begins_with(SKID, :provs)',
                        FilterExpression='#s = :stat',
                        ExpressionAttributeNames={'#s': 'STATUS'},
                        ExpressionAttributeValues={
                            ':key01': {'S': 'BUS#' + businessId + '#' + loc['SKID']},
                            ':provs': {'S': 'PRO#'},
                            ':stat' : {'N': '1'}
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
                        'TimeZone': loc['TIME_ZONE'] if 'TIME_ZONE' in loc else 'America/Puerto_Rico',
                        'Open': loc['OPEN']
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