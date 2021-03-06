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
lambdaInv = boto3.client('lambda')
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
    try:
        statusCode = ''
        timeChat = ''

        data = json.loads(event['body'])
        appointmentId = event['pathParameters']['appointmentId']
        userType = 'U'
        message = data['Message']

        response = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :appointmentId AND SKID = :appointmentId',
            ExpressionAttributeValues={
                ':appointmentId': {'S': 'APPO#'+appointmentId}
            }
        )
        getMessage = ''
        businessId = ''
        locationId = ''
        dateAppointment = ''
        for row in json_dynamodb.loads(response['Items']):
            getMessage = row['MESSAGES'] if 'MESSAGES' in row else []
            dateAppointment = row['DATE_APPO']
            keys = row['GSI1PK'].split('#')
            businessId = keys[1]
            locationId = keys[3]

        country_date = dateutil.tz.gettz(findTimeZone(businessId, locationId))
        today = datetime.datetime.now(tz=country_date)
        dateOpe = today.strftime("%Y-%m-%d")
        timeChat = today.strftime("%d %B, %I:%M %p")

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
            UpdateExpression="SET MESSAGES = :chat, UNREAD = :unread",
            ExpressionAttributeValues={
                ':chat': conversation,
                ':unread': "U" if userType == "1" else "H" 
            }
            # ReturnValues="UPDATED_NEW"
        )

        data = {
            'BusinessId': businessId,
            'LocationId': locationId,
            'AppId': appointmentId,
            'User': 'H',
            'Message': conver,
            'Tipo': 'MESS'
        }
        if dateOpe[0:10] == dateAppointment[0:10]:
            lambdaInv.invoke(
                FunctionName='PostMessages',
                InvocationType='Event',
                Payload=json.dumps(data)
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
            "access-control-allow-origin" : "*"
        },
        'body' : body
    }
    return response