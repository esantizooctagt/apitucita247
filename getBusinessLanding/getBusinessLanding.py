import sys
import logging
import json

import datetime
import dateutil.tz
from datetime import timezone

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
    stage = ''
    businessId = ''
    body = ''
    cors = ''
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']

    try:
        statusCode = ''
        country_date = dateutil.tz.gettz('America/Puerto_Rico')
        today = datetime.datetime.now(tz=country_date)
        dateOpe = today.strftime("%Y-%m")

        link = event['pathParameters']['link']
        response = dynamodb.query(
            TableName="TuCita247",
            IndexName="TuCita247_Appos",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :link',
            ExpressionAttributeValues={
                ':link': {'S': 'LINK#' + dateOpe}
            },
            Limit =1
        )
        for row in json_dynamodb.loads(response['Items']):            
            businessId = row['PKID'].replace('BUS#','')
            locs = []
            if businessId != '':
                locations = dynamodb.query(
                    TableName="TuCita247",
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='PKID = :businessId',
                    ExpressionAttributeValeus={
                        ':businessId': {'S': 'BUS#' + businessId}
                    }
                )
                for det in json_dynamodb.loads(locations['Items']):
                    record = {
                        'LocationId': det['SKID'].replace('LOC#',''),
                        'Name': det['NAME'],
                        'Address': det['ADDRESS']
                    }
                    locs.append(record)
            
            recordset = {
                'BusinessId': row['PKID'].replace('BUS#',''),
                'Name': row['NAME'],
                'Imagen': row['IMGLINK'] if 'IMGLINK' else '',
                'LongDescrip': row['LONGDESCRIPTION'] if 'LONGDESCRIPTION' else '',
                'Phone': row['PHONE'] if 'PHONE' else '',
                'ShortDescript': row['SHORTDESCRIPTION'] if 'SHORTDESCRIPTION' else '',
                'WebSite': row['WEBSITE'] if 'WEBSITE' else '',
                'Locs': locs
            }
            
            statusCode = 200
            body = json.dumps(recordset)
        
        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Message':'Something goes wrong, try again', 'Code': 500})
    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message':'Error on request try again'+ str(e)})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : cors
        },
        'body' : body
    }
    return response