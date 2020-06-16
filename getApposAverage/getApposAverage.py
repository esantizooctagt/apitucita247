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
        initDate = event['pathParameters']['initDate']
        businessId = event['pathParameters']['businessId']
        
        response = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :businessId AND begins_with( SKID , :locations )',
            ExpressionAttributeValues={
                ':businessId': {'S': 'BUS#' + businessId},
                ':locations': {'S': 'LOC#'}
            }
        )
        
        locs = []
        for row in json_dynamodb.loads(response['Items']):
            recordset = {
                'LocationId': row['SKID'].replace('LOC#',''),
                'Name': row['NAME']
            }
            locs.append(recordset)
        
        locsData = []
        for item in locs:
            response = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :locationId AND begins_with( SKID , :date )',
                ExpressionAttributeValues={
                    ':locationId': {'S': 'LOC#' + item['LocationId'] + '#DT#' + initDate[0:7]},
                    ':date': {'S': 'DT#' + initDate[0:7]}
                }
            )
            record = []
            for det in json_dynamodb.loads(response['Items']):
                recordset = {
                    'Qty': det['QTY_APPOS'],
                    'Average': det['TIME_APPO'],
                    'DateAppo': det['SKID'].replace('DT#','')
                }
                record.append(recordset)
            locations = {
                'LocationId': item['LocationId'],
                'Name': item['Name'],
                'Data': record
            }
            locsData.append(locations)

        statusCode = 200
        body = json.dumps(locsData)
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