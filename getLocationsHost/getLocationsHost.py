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

        response = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :businessId AND begins_with( SKID , :locations )',
            FilterExpression='#s = :stat',
            ExpressionAttributeNames={'#s': 'STATUS'},
            ExpressionAttributeValues={
                ':businessId': {'S': 'BUS#' + businessId},
                ':locations': {'S': 'LOC#'},
                ':stat' : {'N': '1'}
            }
        )
        record = []
        locations = json_dynamodb.loads(response['Items'])
        for row in locations:
            serv = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :businessId AND begins_with( SKID , :providerId )',
                FilterExpression='#s = :stat',
                ExpressionAttributeNames={'#s': 'STATUS'},
                ExpressionAttributeValues={
                    ':businessId': {'S': 'BUS#' + businessId + '#' + row['SKID']},
                    ':providerId': {'S': 'PRO#'},
                    ':stat' : {'N': '1'}
                }
            )
            providers = []
            for item in json_dynamodb.loads(serv['Items']):
                serviceData = {
                    'ProviderId': row['SKID'].replace('LOC#','')+'#'+item['SKID'].replace('PRO#',''),
                    'Name': item['NAME']
                }
                providers.append(serviceData)

            recordset = {
                'BusinessId': businessId,
                'LocationId': row['SKID'].replace('LOC#',''),
                'Name': row['NAME'] if 'NAME' in row else '',
                'Doors': row['DOORS'] if 'DOORS' in row else '',
                'Status': row['STATUS'] if 'STATUS' in row else 0,
                'TimeZone': row['TIME_ZONE'] if 'TIME_ZONE' in row else 'America/Puerto_Rico',
                'Providers': providers
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