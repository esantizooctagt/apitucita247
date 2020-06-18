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

dynamodb = boto3.client('dynamodb', region_name='us-east-1')
dynamoUpd = boto3.resource('dynamodb', region_name='us-east-1')
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):
    try:
        statusCode = ''
        appointmentId = event['pathParameters']['appointmentId']
        whoRead = 'U'
        unRead = ''

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
            unRead = row['UNREAD'] if 'UNREAD' in row else ''

        if unRead == whoRead:
            table = dynamoUpd.Table('TuCita247')
            response = table.update_item(
                Key={
                    'PKID': 'APPO#' + appointmentId,
                    'SKID': 'APPO#' + appointmentId
                },
                UpdateExpression="set UNREAD = :unread",
                ExpressionAttributeValues={
                    ':unread': "0" 
                }
                # ReturnValues="UPDATED_NEW"
            )

        statusCode = 200
        body = json.dumps({'Message': 'Appointment updated successfully', 'Code': 200, 'Messages': getMessage})

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