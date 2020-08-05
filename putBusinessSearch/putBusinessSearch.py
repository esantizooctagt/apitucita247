import sys
import logging
import json

import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr
from dynamodb_json import json_util as json_dynamodb

import datetime
import dateutil.tz
from datetime import timezone

from decimal import *

import os

REGION = 'us-east-1'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.client('dynamodb', region_name='us-east-1')
dynamodbData = boto3.resource('dynamodb', region_name='us-east-1')
search = boto3.client('cloudsearchdomain', endpoint_url="https://search-tucita247-djl3mvkaapbmo5zjxat7pcnepu.us-east-1.cloudsearch.amazonaws.com")

logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):
    stage = event['headers']

    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']
    
    try:

        country_date = dateutil.tz.gettz('America/Puerto_Rico')
        today = datetime.datetime.now(tz=country_date)
        dayName = today.strftime("%A")[0:3].upper()

        business = dynamodb.query(
            TableName="TuCita247",
            IndexName="TuCita247_TypeAppos",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='GSI4PK = :search',
            ExpressionAttributeValues={
                ':search': {'S': 'SEARCH'}
            }
        )

        record = []
        recordset = {}
        for row in json_dynamodb.loads(business['Items']):
            locs = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :businessId AND begins_with(SKID , :locationId)',
                ExpressionAttributeValues={
                    ':businessId': {'S': row['PKID']},
                    ':locationId': {'S': 'LOC#'}
                }
            )
            cities = []
            sectors = []
            for item in json_dynamodb.loads(locs['Items']):
                cities.append(item['CITY'])
                sector = item['SECTOR'] if 'SECTOR' in item else ''
                if sector != '':
                    sectors.append(sector)

            recordset = {
                "type" : "add",
                "id" : row['PKID'],
                "fields" : {
                    "pkid" : row['PKID'],
                    "name_eng" : row['NAME'],
                    "skid" : row['PKID'],
                    "name_esp" : row['NAME'],
                    "city": cities,
                    "sector": sectors,
                    "tags": row['TAGS'].split(","),
                    # "location_field": item['GEOLOCATION'].replace('"LAT": ','').replace('"LNG": ',''),
                    "tipo": "2"
                    }
                }
            record.append(recordset)

            table = dynamodbData.Table('TuCita247')
            updateBusiness = table.update_item(
                Key={
                    'PKID': row['PKID'],
                    'SKID': 'METADATA'
                },
                UpdateExpression="REMOVE GSI4PK, GSI4SK",
                ReturnValues="NONE"
            )

        response = search.upload_documents(
            documents=json.dumps(record),
            contentType='application/json'
        )
        resultSet = { 
            'Code': 200,
            'Business': record
        }
        statusCode = 200
        body = json.dumps(resultSet)
    
        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Code': 500, 'Message': 'Error on load operation hours'})
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