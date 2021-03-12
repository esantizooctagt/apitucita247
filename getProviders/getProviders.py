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

def findLoc(locs, locId):
    for item in range(len(locs)):
        if locs[item]['LocationId'] == locId:
            return locs[item]['Name']
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
        a = {':businessId': {'S': 'BUS#' + businessId}, ':stat': {'N': '2'}, ':services': {'S':'PRO#'}}
        f = '#s < :stat'
        if search != '_':
            e = {'#s': 'STATUS', '#n': 'NAME'}
            f = '#s < :stat and begins_with (#n , :search)'
            a = {':businessId': {'S': 'BUS#' + businessId}, ':stat': {'N': '2'}, ':services': {'S':'PRO#'}, ':search': {'S': search}}

        if lastItem == '_':
            lastItem = ''
        else:
            if lastItem == '':
                salir = 1
            else:
                dataLoc = dynamodb.query(
                    TableName="TuCita247",
                    IndexName="TuCita247_Index",
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='GSI1PK = :businessId AND GSI1SK = :prov',
                    ExpressionAttributeValues= {
                        ':businessId': {'S': 'BUS#' + businessId },
                        ':prov': {'S': 'PRO#' + lastItem }
                    }
                )
                locationId = ''
                for val in json_dynamodb.loads(dataLoc['Items']):
                    locationId = val['PKID']
            
                lastItem = {'PKID': {'S': locationId }, 'SKID': {'S': 'PRO#' + lastItem }, 'GSI1PK': {'S': 'BUS#' + businessId },'GSI1SK': {'S': 'PRO#' + lastItem }}

        locs=[]
        locsName = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :businessId AND begins_with(SKID , :locs)',
            ExpressionAttributeValues={
                ':businessId': {'S': 'BUS#'+businessId},
                ':locs': {'S': 'LOC#'}
            }
        )
        for item in json_dynamodb.loads(locsName['Items']):
            data = {
                'LocationId': item['SKID'].replace('LOC#',''),
                'Name': item['NAME']
            }
            locs.append(data)
        
        if salir == 0:
            if lastItem == '':
                response = dynamodb.query(
                    TableName="TuCita247",
                    IndexName="TuCita247_Index",
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='GSI1PK = :businessId AND begins_with ( GSI1SK, :services )',
                    ExpressionAttributeNames=e,
                    ExpressionAttributeValues=a,
                    FilterExpression=f,
                    Limit=items
                )
            else:
                logger.info("ingreso bien")
                response = dynamodb.query(
                    TableName="TuCita247",
                    IndexName="TuCita247_Index",
                    ReturnConsumedCapacity='TOTAL',
                    ExclusiveStartKey= lastItem,
                    KeyConditionExpression='GSI1PK = :businessId AND begins_with ( GSI1SK, :services )',
                    ExpressionAttributeNames=e,
                    ExpressionAttributeValues=a,
                    FilterExpression=f,
                    Limit=items
                )
            
            recordset ={}
            lastItem = ''
            for row in json_dynamodb.loads(response['Items']):
                recordset = {
                    'ProviderId': row['SKID'].replace('PRO#',''),
                    'Name': row['NAME'],
                    'LocationId': row['PKID'].replace('BUS#' + businessId + '#LOC#',''),
                    'Location': findLoc(locs, row['PKID'].replace('BUS#' + businessId + '#LOC#','')),
                    'Status': row['STATUS']
                }
                records.append(recordset)

            if 'LastEvaluatedKey' in response:
                lastItem = json_dynamodb.loads(response['LastEvaluatedKey'])
                lastItem = lastItem['SKID'].replace('PRO#','')

            resultSet = { 
                'lastItem': lastItem,
                'providers': records
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