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

def getKey(obj):
    return obj['Name']
    
def lambda_handler(event, context):
    try:
        businessId = event['pathParameters']['businessId']
        locations = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :businessId AND begins_with( SKID , :locations )',
            FilterExpression='#s = :status',
            ExpressionAttributeNames={'#s':'STATUS'},
            ExpressionAttributeValues={
                ':businessId': {'S': 'BUS#' + businessId},
                ':locations': {'S': 'LOC#'},
                ':status': {'N': str(1)}
            }
        )
        recordset = {}
        records = []
        for row in json_dynamodb.loads(locations['Items']):
            cityData = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :country AND SKID = :city',
                ExpressionAttributeValues={
                    ':country': {'S': 'COUNTRY#PRI'},
                    ':city': {'S': 'CITY#'+ row['CITY']}
                }
            )
            cityESP = ''
            cityENG = ''
            for item in json_dynamodb.loads(cityData['Items']):
                cityESP = item['NAME_ESP']
                cityENG = item['NAME_ENG']

            recordset = {
                'BusinessId': row['PKID'].replace('BUS#',''),
                'LocationId': row['SKID'].replace('LOC#',''),
                'Name': row['NAME'] if 'NAME' in row else '',
                'Address': row['ADDRESS'] if 'ADDRESS' in row else '',
                'Geolocation': row['GEOLOCATION'] if 'GEOLOCATION' in row else '',
                'Status': row['STATUS'] if 'STATUS' in row else 0,
                'Door': row['DOORS'] if 'DOORS' in row else '',
                'TimeZone': row['TIME_ZONE'] if 'TIME_ZONE' in row else 'America/Puerto_Rico',
                'City_ENG': cityENG,
                'City_ESP': cityESP
            }
            records.append(recordset)
        records.sort(key=getKey)
        business = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :businessId AND SKID = :metadata',
            ExpressionAttributeValues={
                ':businessId': {'S': 'BUS#' + businessId},
                ':metadata': {'S': 'METADATA'}
            },
            Limit =1
        )
        data = {}
        for bus in json_dynamodb.loads(business['Items']):
            data = {
                'BusinessId': bus['PKID'].replace('BUS#',''),
                'Name': bus['NAME'],
                'LongDescription': bus['LONGDESCRIPTION'] if 'LONGDESCRIPTION' in bus else '',
                'ShortDescription': bus['SHORTDESCRIPTION'] if 'SHORTDESCRIPTION' in bus else '',
                'Imagen': bus['IMGBUSINESS'] if 'IMGBUSINESS' in bus else '',
                'Locations': records
            }
            
        statusCode = 200
        body = json.dumps({'Code': 200, 'Business': data})
    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message':'Error on request try again'+ str(e)})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : "*"
        },
        'body' : body
    }
    return response