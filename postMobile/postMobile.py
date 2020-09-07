import sys
import json
import logging
import random

import os

import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr
from dynamodb_json import json_util as json_dynamodb

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.client('dynamodb', region_name='us-east-1')
sms = boto3.client('sns')
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):
    response = ''
    verifCode = 0
    verifCode = random.randint(100000, 999999)
    
    data = json.loads(event['body'])
    to_number = event['pathParameters']['number']
    playerId = data['PlayerId']
    bodyStr = 'Your TuCita247 verification code is: ' + str(verifCode)
    
    try:
        sms.publish(
            PhoneNumber="+"+to_number,
            Message=bodyStr
        )
        details = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :mobile AND begins_with ( SKID , :customer )',
            ExpressionAttributeValues={
                ':mobile': {'S': 'MOB#' + to_number},
                ':customer': {'S': 'CUS#'}
            }
        )
        recordset = {}
        for item in json_dynamodb.loads(details['Items']):
            recordset = {
                'CustomerId': item['SKID'].replace('CUS#',''),
                'Status': item['STATUS'],
                'Name': item['NAME'],
                'Gender': item['GENDER'] if 'GENDER' in item else '',
                'Email': item['EMAIL'] if 'EMAIL' in item else '',
                'Preferences': item['PREFERENCES'] if 'PREFERENCES' in item else '',
                'Disability': item['DISABILITY'] if 'DISABILITY' in item else '',
                'DOB': item['DOB'] if 'DOB' in item else '',
                'Mobile': to_number
            }
            if item['SKID'] != '' and playerId != '':
                dynamodb.update_item(
                    TableName='TuCita247',
                    Key={
                        'PKID':{'S': 'MOB#'+ to_number},
                        'SKID':{'S': item['SKID']}
                    },
                    UpdateExpression='SET PLAYERID = :playerId',
                    ExpressionAttributeValues={':playerId': {'S': playerId}}
                )

        statusCode = 200
        body = json.dumps({'VerifcationCode': str(verifCode), 'Customer': recordset, 'Code': 200})
    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message': str(e), 'Code': 500})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : "*"
        },
        'body' : body
    }
    return response