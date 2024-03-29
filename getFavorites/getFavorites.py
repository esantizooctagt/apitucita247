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
    records =[]
    try:
        customerId = event['pathParameters']['customerId']
        response = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :customerId AND begins_with ( SKID , :favs )',
            ExpressionAttributeValues={
                ':customerId': {'S': 'CUS#' + customerId},
                ':favs': {'S': 'FAVS#'}
            },
        )
        for row in json_dynamodb.loads(response['Items']):
            locAddress = ''
            location = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :businessId AND SKID = :locationId',
                ExpressionAttributeValues={
                    ':businessId': {'S': 'BUS#' + row['BUSID']},
                    ':locationId': {'S': 'LOC#' + row['LOCID']}
                },
                Limit = 1
            )
            for loc in json_dynamodb.loads(location['Items']):
                cityData = dynamodb.query(
                    TableName="TuCita247",
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='PKID = :country AND SKID = :city',
                    ExpressionAttributeValues={
                        ':country': {'S': 'COUNTRY#PRI'},
                        ':city': {'S': 'CITY#'+ loc['CITY']}
                    }
                )
                cityESP = ''
                cityENG = ''
                for item in json_dynamodb.loads(cityData['Items']):
                    cityESP = item['NAME_ESP']
                    cityENG = item['NAME_ENG']
                    
                locAddress = loc['ADDRESS'] if 'ADDRESS' in loc else ''

                NameLoc = loc['NAME'] if 'NAME' in loc else ''
                Geolocation = loc['GEOLOCATION'] if 'GEOLOCATION' in loc else ''
                Door = loc['DOORS'] if 'DOORS' in loc else ''
                Status = loc['STATUS'] if 'STATUS' in loc else 0

                locationId = loc['SKID'].replace('LOC#','')
                businessId = loc['PKID'].replace('BUS#','')

            business = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :businessId AND SKID = :metadata',
                ExpressionAttributeValues={
                    ':businessId': {'S': 'BUS#' + row['BUSID']},
                    ':metadata': {'S': 'METADATA'}
                },
                Limit = 1
            )
            recordsetDet = {}
            recordsetDet = {
                'LocationId': locationId,
                'Name': NameLoc,
                'Address': locAddress,
                'Geolocation': Geolocation,
                'Status': Status,
                'Door': Door,
                'BusinessId': businessId,
                'City_ENG': cityENG,
                'City_ESP': cityESP
            }
            recordset = {}
            for business in json_dynamodb.loads(business['Items']):
                recordset = {
                    'Name': business['NAME'],
                    'Imagen': business['IMGBUSINESS'] if 'IMGBUSINESS' in business else '',
                    'LongDescription': business['LONGDESCRIPTION'] if 'LONGDESCRIPTION' in business else '',
                    'ShortDescription': business['SHORTDESCRIPTION'],
                    'Location': locAddress,
                    'LocationId': locationId,
                    'BusinessId': businessId,
                    'Locations': recordsetDet
                }
                records.append(recordset)

        statusCode = 200
        body = json.dumps(records)
    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message': 'Error on request try again ' + str(e)})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : "*"
        },
        'body' : body
    }
    return response