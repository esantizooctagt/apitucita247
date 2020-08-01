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

        response = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :businessId AND begins_with( SKID , :locations )',
            ExpressionAttributeValues={
                ':businessId': {'S': 'BUS#' + businessId},
                ':locations': {'S': 'LOC#'}
            }
        )
        record = []
        locations = json_dynamodb.loads(response['Items'])
        for row in locations:
            serv = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :businessId AND begins_with( SKID , :providerId )',
                ExpressionAttributeValues={
                    ':businessId': {'S': 'BUS#' + businessId + '#' + row['SKID'].replace('LOC#','')},
                    ':providerId': {'S': 'PRO#'}
                }
            )
            services = []
            for item in json_dynamodb.loads(serv['Items']):
                serviceData = {
                    'ProviderId': row['SKID'].replace('LOC#','')+'#'+item['SKID'].replace('PRO#',''),
                    'Name': item['NAME']
                }
                services.append(serviceData)

            recordset = {
                'BusinessId': businessId,
                'LocationId': row['SKID'].replace('LOC#',''),
                'Name': row['NAME'] if 'NAME' in row else '',
                'Doors': row['DOORS'] if 'DOORS' in row else '',
                'Status': row['STATUS'] if 'STATUS' in row else 0,
                'Services': services
            }
            record.append(recordset)

        statusCode = 200
        body = json.dumps({'Code': 200, 'Locs': record})
    
        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Code': 500, 'Message': 'Error on update user'})
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