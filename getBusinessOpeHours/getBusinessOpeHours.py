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

dynamodb = boto3.client('dynamodb', region_name=REGION)
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def findTimeZone(businessId, locationId):
    timeZone='America/Puerto_Rico'
    locZone = dynamodb.query(
        TableName="TuCita247",
        ReturnConsumedCapacity='TOTAL',
        KeyConditionExpression='PKID = :key AND SKID = :skey',
        ExpressionAttributeValues={
            ':key': {'S': 'BUS#'+businessId},
            ':skey': {'S': 'LOC#'+locationId}
        }
    )
    for timeLoc in json_dynamodb.loads(locZone['Items']):
        timeZone = timeLoc['TIME_ZONE'] if 'TIME_ZONE' in timeLoc else 'America/Puerto_Rico'
    return timeZone

def lambda_handler(event, context):
    stage = event['headers']

    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']
    
    try:
        businessId = event['pathParameters']['businessId']
        locationId = event['pathParameters']['locationId']
        dayOffValid = True

        country_date = dateutil.tz.gettz(findTimeZone(businessId, locationId))
        today = datetime.datetime.now(tz=country_date)
        dayName = today.strftime("%A")[0:3].upper()

        response = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :businessId AND SKID = :locationId',
            ExpressionAttributeValues={
                ':businessId': {'S': 'BUS#' + businessId},
                ':locationId': {'S': 'LOC#' + locationId}
            }
        )

        daysOff = []
        opeHours = ''
        for row in json_dynamodb.loads(response['Items']):
            daysOff = row['DAYS_OFF'] if 'DAYS_OFF' in row else []
            opeHours = json.loads(row['OPERATIONHOURS']) if 'OPERATIONHOURS' in row else ''
        
        if daysOff != []:
            dayOffValid = today.strftime("%Y-%m-%d") not in daysOff
        
        record = []
        recordset = {}
        currHour = today.strftime("%H")
        if dayOffValid:
            dateAppo = opeHours[dayName] if dayName in opeHours else []
            for item in dateAppo:
                ini = item['I']
                fin = item['F']
                recordset = {
                    'HoraIni': ini,
                    'HoraFin': fin
                }
                record.append(recordset)

        resultSet = { 
            'Code': 200,
            'CurrHour': currHour,
            'Hours': record
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