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

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']
        
    try:
        statusCode = ''
        data = json.loads(event['body'])
        appointmentId = event['pathParameters']['id']
        status = data['Status']
        dateAppo = data['DateAppo']
        reasonId = data['Reason']

        table = dynamodb.Table('TuCita247')
        e = {'#s': 'STATUS'}
        response = table.update_item(
            Key={
                'PKID': 'APPO#' + appointmentId,
                'SKID': 'APPO#' + appointmentId
            },
            UpdateExpression="set #s = :status, GSI1SK = :key01, GSI2SK = :key02, ReasonId = :reason",
            ExpressionAttributeNames=e,
            ExpressionAttributeValues={
                ':status': str(status),
                ':key01': str(status) + '#DT#' + str(dateAppo),
                ':key02': str(status) + '#DT#' + str(dateAppo),
                ':reason': reasonId
            }
            # ReturnValues="UPDATED_NEW"
        )
        #PASA A PRE-CHECK IN Y ENVIA NOTIFICACION POR TWILIO A SMS y CORREO (TWILIO), ONESIGNAL (PUSH NOTIFICATION)
        if status == 2:
            # GET USER PREFERENCE NOTIFICATION
            status = 1
        
        statusCode = 200
        body = json.dumps({'Message': 'Appointment updated successfully', 'Code': 200})

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
            "access-control-allow-origin" : cors
        },
        'body' : body
    }
    return response