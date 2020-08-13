import sys
import logging
import json

import boto3
import botocore.exceptions
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
from dynamodb_json import json_util as json_dynamodb

import datetime
import dateutil.tz
from datetime import timezone

import os

REGION = 'us-east-1'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb', region_name=REGION)
dynamodbQuery = boto3.client('dynamodb', region_name=REGION)
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):
    try:
        statusCode = ''
        dataId = ''
        appoData = ''
        businessId = ''
        locationId = ''
        providerId = ''

        appointmentId = event['pathParameters']['appointmentId']
        dateAppo = event['pathParameters']['dateAppo']

        status = 5
        country_date = dateutil.tz.gettz('America/Puerto_Rico')
        today = datetime.datetime.now(tz=country_date)
        dateOpe = today.strftime("%Y-%m-%d-%H-%M-%S")

        response = dynamodbQuery.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :appointmentId AND SKID = :appointmentId',
            ExpressionAttributeValues={
                ':appointmentId': {'S': 'APPO#' + appointmentId}
            }
        )
        for row in json_dynamodb.loads(response['Items']):
            dataId = row['GSI1PK']
            appoData = str(row['DATE_APPO'])[0:10]+'#APPO#'+appointmentId
            if dataId != '':
                businessId = 'BUS#'+data.split('#')[1]+'#5'
                locationId = 'BUS#'+data.split('#')[1]+'#LOC#'+data.split('#')[3]+'#5'
                providerId = 'BUS#'+data.split('#')[1]+'#LOC#'+data.split('#')[3]+'#PRO#'+data.split('#')[5]+'#5'

        table = dynamodb.Table('TuCita247')
        e = {'#s': 'STATUS'}
        response = table.update_item(
            Key={
                'PKID': 'APPO#' + appointmentId,
                'SKID': 'APPO#' + appointmentId
            },
            UpdateExpression="SET #s = :status, GSI1SK = :key01, GSI2SK = :key02, TIMECANCEL = :dateope, GSI5PK = :pkey05, GSI5SK = :skey05, GSI6PK = :pkey06, GSI6SK = :skey06, GSI7PK = :pkey07, GSI7SK = :skey07",
            ExpressionAttributeNames=e,
            ExpressionAttributeValues={':status': status, ':key01': str(status) + '#DT#' + str(dateAppo), ':key02': '#5',  ':dateope': dateOpe, ':pkey05': businessId, ':skey05': appoData, ':pkey06': locationId, ':skey06': appoData, ':pkey07': providerId, ':skey07': appoData},
            ReturnValues="NONE"
        )

        statusCode = 200
        body = json.dumps({'Message': 'Appointment updated successfully', 'Code': 200})

        logger.info(response)
        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Message': 'Error on update appointment', 'Code': 500})

    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message': 'Error on request try again ' + str(e), 'Code': 500})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : "*"
        },
        'body' : body
    }
    return response