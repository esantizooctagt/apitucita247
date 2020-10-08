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

def lambda_handler(event, context):
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']

    records =[]
    try:
        businessId = event['pathParameters']['businessId']
        providerId = event['pathParameters']['providerId']

        response = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :businessId AND begins_with ( SKID, :services )',
            ExpressionAttributeNames={'#s': 'STATUS'},
            ExpressionAttributeValues={':businessId': {'S': 'BUS#' + businessId}, ':stat': {'N': '2'}, ':services': {'S':'SER#'}},
            FilterExpression='#s < :stat'
        )

        recordset ={}
        for row in json_dynamodb.loads(response['Items']):
            selected = 0
            providers = dynamodb.query(
                TableName="TuCita247",
                IndexName="TuCita247_Index",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='GSI1PK = :businessId AND GSI1SK = :serviceId',
                ExpressionAttributeValues={':businessId': {'S': 'BUS#' + businessId + '#PRO#' + providerId}, ':serviceId': {'S': row['SKID']}}
            )
            for item in json_dynamodb.loads(providers['Items']):
                if item['GSI1SK'] == row['SKID']:
                    selected = 1

            recordset = {
                'ServiceId': row['SKID'].replace('SER#',''),
                'Name': row['NAME'],
                'TimeService': row['TIME_SERVICE'],
                'CustomerPerBooking': row['CUSTOMER_PER_BOOKING'],
                'Selected': selected
            }
            records.append(recordset)


            resultSet = { 
                'services': records
            }
        
        statusCode = 200
        body = json.dumps(resultSet)

        if statusCode == '':
            statusCode = 500
            body = json.dumps({"Message": "Error on request try again", "Code": 500})
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