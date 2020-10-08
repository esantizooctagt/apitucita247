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
        dateAppo = event['pathParameters']['dateAppo']

        status = 3
        response = dynamodb.query(
            TableName="TuCita247",
            IndexName="TuCita247_TypeAppos",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='GSI4PK = :gsi4pk AND begins_with ( GSI4SK , :gsi4sk )',
            ExpressionAttributeValues={
                ':gsi4pk': {'S': 'BUS#' + businessId + '#LOC#' + locationId},
                ':gsi4sk': {'S': str(status) +'#DT#' + dateAppo}
            }
        )

        record = []
        recordset = {}
        logger.info(response)
        for row in json_dynamodb.loads(response['Items']):
            recordset = {
                'AppointmentId': row['PKID'].replace('APPO#',''),
                'Name': row['NAME'],
                'Phone': row['PHONE'],
                'Door': row['DOOR'] if 'DOOR' in row else '',
                'Status': row['STATUS'],
                'DateAppo': row['DATE_APPO'],
                'NoPeople': row['PEOPLE_QTY'],
                'CheckOut': False
            }
            record.append(recordset)

        appoId = ''
        lastItem = ''
        while 'LastEvaluatedKey' in response:
            lastItem = json_dynamodb.loads(response['LastEvaluatedKey'])
            response = dynamodb.query(
                TableName="TuCita247",
                IndexName="TuCita247_TypeAppos",
                ReturnConsumedCapacity='TOTAL',
                ExclusiveStartKey= lastItem,
                KeyConditionExpression='GSI4PK = :gsi4pk AND begins_with ( GSI4SK , :gsi4sk )',
                ExpressionAttributeValues={
                    ':gsi4pk': {'S': 'BUS#' + businessId + '#LOC#' + locationId},
                    ':gsi4sk': {'S': str(status) +'#DT#' + dateAppo}
                },
            )

            recordset = {}
            for row in json_dynamodb.loads(response['Items']):
                recordset = {
                'AppointmentId': row['PKID'].replace('APPO#',''),
                'Name': row['NAME'],
                'Phone': row['PHONE'],
                'Door': row['DOOR'] if 'DOOR' in row else '',
                'Status': row['STATUS'],
                'DateAppo': row['DATE_APPO'],
                'NoPeople': row['PEOPLE_QTY'],
                'CheckOut': False
            }
            record.append(recordset)

        resultSet = { 
            'Code': 200,
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