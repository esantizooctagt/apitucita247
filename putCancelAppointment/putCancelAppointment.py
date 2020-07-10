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

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):
    try:
        statusCode = ''
        appointmentId = event['pathParameters']['appointmentId']
        dateAppo = event['pathParameters']['dateAppo']

        status = 5
        country_date = dateutil.tz.gettz('America/Puerto_Rico')
        today = datetime.datetime.now(tz=country_date)
        dateOpe = today.strftime("%Y-%m-%d-%H-%M-%S")

        table = dynamodb.Table('TuCita247')
        e = {'#s': 'STATUS'}
        response = table.update_item(
            Key={
                'PKID': 'APPO#' + appointmentId,
                'SKID': 'APPO#' + appointmentId
            },
            UpdateExpression="SET #s = :status, GSI1SK = :key01, GSI2SK = :key02, TIMECANCEL = :dateope",
            ExpressionAttributeNames=e,
            ExpressionAttributeValues={':status': status, ':key01': str(status) + '#DT#' + str(dateAppo), ':key02': '#5' if str(status) == '5' else str(dateAppo)[0:10],  ':dateope': dateOpe},
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