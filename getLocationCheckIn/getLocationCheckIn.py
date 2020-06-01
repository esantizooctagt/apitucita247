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
        businessId = event['pathParameters']['businessId']
        locationId = event['pathParameters']['locationId']
        dateAppoIni = event['pathParameters']['dateAppo']
        lastItem = event['pathParameters']['lastItem']
        appoId = event['pathParameters']['appoId']

        status = 3
        if lastItem == '_':
            lastItem = ''
            response = dynamodb.query(
                TableName="TuCita247",
                IndexName="TuCita247_Index",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='GSI1PK = :gsi1pk AND begins_with ( GSI1SK , :gsi1sk_ini )',
                ExpressionAttributeNames=n,
                FilterExpression=f,
                ExpressionAttributeValues={
                    ':gsi1pk': {'S': 'BUS#' + businessId + '#LOC#' + locationId},
                    ':gsi1sk_ini': {'S': str(status) +'#DT#' + dateAppoIni}
                },
                Limit = 2
            )
        else:
            lastItem = {'GSI1PK': {'S': 'BUS#' + businessId + '#LOC#' + locationId },'GSI1SK': {'S': str(status) + '#DT#' + lastItem }, 'SKID': {'S': 'APPO#' + appoId}, 'PKID': {'S': 'APPO#' + appoId}}
            response = dynamodb.query(
                TableName="TuCita247",
                IndexName="TuCita247_Index",
                ReturnConsumedCapacity='TOTAL',
                ExclusiveStartKey= lastItem,
                KeyConditionExpression='GSI1PK = :gsi1pk AND begins_with ( GSI1SK , :gsi1sk_ini )',
                FilterExpression=f,
                ExpressionAttributeNames=n,
                ExpressionAttributeValues={
                    ':gsi1pk': {'S': 'BUS#' + businessId + '#LOC#' + locationId},
                    ':gsi1sk_ini': {'S': str(status) +'#DT#' + dateAppoIni}
                },
                Limit = 2
            )

        record = []
        recordset = {}
        logger.info(response)
        locations = json_dynamodb.loads(response['Items'])
        for row in locations:
            recordset = {
                'AppointmentId': row['PKID'].replace('APPO#',''),
                'Name': row['NAME'],
                'Phone': row['PHONE'],
                'Door': row['DOOR'] if 'DOOR' in row else '',
                'Status': row['STATUS']
            }
            record.append(recordset)
        
        lastItem = '_'
        appoId = '_'
        if 'LastEvaluatedKey' in response:
            lastItem = json_dynamodb.loads(response['LastEvaluatedKey'])
            appoId = lastItem['PKID'].replace('APPO#','')
            lastItem = lastItem['GSI1SK']

        resultSet = { 
            'Code': 200,
            'lastItem': lastItem,
            'AppId': appoId,
            'Appos': record
        }
        statusCode = 200
        body = json.dumps(resultSet)
    
        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Code': 500, 'Message': 'Error on load appointments'})
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