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

dynamodb = boto3.client('dynamodb', region_name='us-east-1')
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
            response = dynamodb.query(
                TableName="TuCita247",
                IndexName="TuCita247_Index",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :businessId AND begins_with ( SKID, :provider )',
                ExpressionAttributeValues={':businessId': {'S': 'BUS#' + businessId}, ':provider': {'S':'PRO#'}}
            )
            recordset = {
                'ServiceId': row['SKID'].replace('SER#',''),
                'Name': row['NAME'],
                'TimeService': row['TIME_SERVICE'],
                'Status': row['STATUS']
            }
            records.append(recordset)

            if 'LastEvaluatedKey' in response:
                lastItem = json_dynamodb.loads(response['LastEvaluatedKey'])
                lastItem = lastItem['SKID'].replace('SER#','')

            resultSet = { 
                'lastItem': lastItem,
                'services': records
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