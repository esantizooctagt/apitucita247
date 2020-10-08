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

import os

REGION = 'us-east-1'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.client('dynamodb', region_name=REGION)
dynamoUpd = boto3.resource('dynamodb', region_name=REGION)
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']
        
    try:
        statusCode = ''
        timeChat = ''
        data = json.loads(event['body'])
        appointmentId = event['pathParameters']['id']
        userType = event['pathParameters']['type']
        message = data['Message']

        country_date = dateutil.tz.gettz('America/Puerto_Rico')
        today = datetime.datetime.now(tz=country_date)

        timeChat = today.strftime("%d %B, %I:%M %p")

        response = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :appointmentId AND SKID = :appointmentId',
            ExpressionAttributeValues={
                ':appointmentId': {'S': 'APPO#'+appointmentId}
            }
        )
        getMessage = ''
        for row in json_dynamodb.loads(response['Items']):
            getMessage = row['MESSAGES'] if 'MESSAGES' in row else []

        conversation = []
        if userType == "1":
            conver = {
                "H":  message,
                "T": timeChat
            }
        else:
            conver = {
                "U":  message,
                "T": timeChat
            }
            
        if getMessage != '':
            conversation = getMessage
            conversation.append(conver)
        else:
            conversation.append(conver)

        table = dynamoUpd.Table('TuCita247')
        response = table.update_item(
            Key={
                'PKID': 'APPO#' + appointmentId,
                'SKID': 'APPO#' + appointmentId
            },
            UpdateExpression="set MESSAGES = :chat, UNREAD = :unread",
            ExpressionAttributeValues={
                ':chat': conversation,
                ':unread': "U" if userType == "1" else "H" 
            }
            # ReturnValues="UPDATED_NEW"
        )

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