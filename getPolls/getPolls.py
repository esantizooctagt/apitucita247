import sys
import logging
import json

import os

import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr
from dynamodb_json import json_util as json_dynamodb

REGION = 'us-east-1'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.client('dynamodb', region_name=REGION)
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def findLocation(locId, locs):
    for loc in locs:
        if loc['LocationId'] == locId:
            return loc['Name']
    return ''

def lambda_handler(event, context):
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']

    records =[]
    try:
        businessId = event['pathParameters']['businessId']
        items = int(event['pathParameters']['items'])
        lastItem = event['pathParameters']['lastItem']
        search = event['pathParameters']['search']
        salir = 0

        e = {'#s': 'STATUS'}
        a = {':businessId': {'S': 'BUS#' + businessId}, ':stat': {'N': '2'}, ':polls': {'S':'POLL#'}}
        f = '#s < :stat'
        if search != '_':
            e = {'#s': 'STATUS', '#n': 'NAME'}
            f = '#s < :stat and begins_with (#n , :search)'
            a = {':businessId': {'S': 'BUS#' + businessId}, ':stat': {'N': '2'}, ':polls': {'S':'POLL#'}, ':search': {'S': search}}

        if lastItem == '_':
            lastItem = ''
        else:
            if lastItem == '':
                salir = 1
            else:
                lastItem = {'PKID': {'S': 'BUS#' + businessId },'SKID': {'S': 'POLL#' + lastItem }}

        if salir == 0:
            if lastItem == '':
                response = dynamodb.query(
                    TableName="TuCita247",
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='PKID = :businessId AND begins_with ( SKID, :polls )',
                    ExpressionAttributeNames=e,
                    ExpressionAttributeValues=a,
                    FilterExpression=f,
                    Limit=items
                )
            else:
                response = dynamodb.query(
                    TableName="TuCita247",
                    ReturnConsumedCapacity='TOTAL',
                    ExclusiveStartKey= lastItem,
                    KeyConditionExpression='PKID = :businessId AND begins_with ( SKID, :polls )',
                    ExpressionAttributeNames=e,
                    ExpressionAttributeValues=a,
                    FilterExpression=f,
                    Limit=items
                )

            locs = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :businessId AND begins_with ( SKID, :locs )',
                ExpressionAttributeValues={
                    ':businessId': {'S': 'BUS#' + businessId}, 
                    ':locs': {'S': 'LOC#'}
                }
            )
            locations = []
            for item in json_dynamodb.loads(locs['Items']):
                data = {
                    'LocationId': item['SKID'].replace('LOC#',''),
                    'Name': item['NAME']
                }
                locations.append(data)

            recordset ={}
            lastItem = ''
            for row in json_dynamodb.loads(response['Items']):
                recordset = {
                    'PollId': row['SKID'].replace('POLL#',''),
                    'Name': row['NAME'],
                    'LocationId': row['LOCATIONID'],
                    'Location': findLocation(row['LOCATIONID'], locations),
                    'DatePoll': row['DATE_POLL'],
                    'DateFinPoll': row['DATE_FIN_POLL'],
                    'Status': row['STATUS']
                }
                records.append(recordset)

            if 'LastEvaluatedKey' in response:
                lastItem = json_dynamodb.loads(response['LastEvaluatedKey'])
                lastItem = lastItem['SKID'].replace('POLL#','')

            resultSet = { 
                'lastItem': lastItem,
                'polls': records
            }
        
            statusCode = 200
            body = json.dumps(resultSet)
        else:
            statusCode = 404
            body = json.dumps({"Message": "No more rows", "Code": 404})
    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message': 'Error on request try again ' +str(e)})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : cors
        },
        'body' : body
    }
    return response