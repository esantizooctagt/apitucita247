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

dynamodb = boto3.client('dynamodb', region_name=REGION)
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
        dateFin = (today + datetime.timedelta(days=90)).strftime("%Y-%m-%d")
        dateIni = today.strftime("%Y-%m-%d")
        
        businessId = event['pathParameters']['businessId']
        response = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :businessId AND SKID = :plan',
            ExpressionAttributeValues={
                ':businessId': {'S': 'BUS#' + businessId},
                ':plan': {'S': 'PLAN'}
            }
        )
        records = []
        packs = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :businessId AND begins_with(SKID, :pack)', #AND SKID between :packsIni AND :packsFin
            ExpressionAttributeValues={
                ':businessId': {'S': 'BUS#' + businessId},
                ':pack': {'S': 'PACK#'}
                # ':packsIni': {'S': 'PACK#' + dateIni},
                # ':packsFin': {'S': 'PACK#' + dateFin}
            }
        )
        recordset ={}
        for pack in json_dynamodb.loads(packs['Items']):
            recordset = {
                'DueDate': '', #pack['SKID'].replace('PACK#',''),
                'Available': pack['AVAILABLE'],
                'Used': pack['APPOINTMENTS']-pack['AVAILABLE'],
                'Appointments': pack['APPOINTMENTS']
            }
            records.append(recordset)

        for row in json_dynamodb.loads(response['Items']):
            recordset = {
                'Total': row['APPOINTMENTS'],
                'Available': row['AVAILABLE'],
                'Used': row['APPOINTMENTS']-row['AVAILABLE'],
                'DueDate': row['DUE_DATE'],
                'Name': row['NAME'],
                'Packs': records
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