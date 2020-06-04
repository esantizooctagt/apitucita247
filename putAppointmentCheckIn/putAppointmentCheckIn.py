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

dynamodb = boto3.client('dynamodb', region_name='us-east-1')
dynamodbTable = boto3.resource('dynamodb', region_name='us-east-1')
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
        qrCode = data['qrCode'] if 'qrCode' in data else ''
        businessId = data['BusinessId'] if 'BusinessId' in data else ''
        locationId = data['LocationId'] if 'LocationId' in data else ''

        country_date = dateutil.tz.gettz('America/Puerto_Rico')
        today = datetime.datetime.now(tz=country_date)
        dateOpe = today.strftime("%Y-%m-%d-%H-%M-%S")
        
        items = []
        recordset = {
            "Update": {
                "TableName": "TuCita247",
                "Key": {
                    "PKID": {"S": 'APPO#' + appointmentId}, 
                    "SKID": {"S": 'APPO#' + appointmentId}
                },
                "UpdateExpression": "SET #s = :status, GSI1SK = :key, GSI2SK = :key, TIMECHECKIN = :dateOpe", 
                "ExpressionAttributeValues": {
                    ":status": {"N": "3"}, 
                    ":key": {"S": str(status) + '#DT#' + str(dateAppo)}, 
                    ":dateOpe": {"S": str(dateAppo)},
                    ":qrCode": {"S": qrCode}
                },
                "ExpressionAttributeNames": {'#s': 'STATUS'},
                "ConditionExpression": "attribute_exists(PKID) AND attribute_exists(SKID) AND QRCODE = :qrCode",
                "ReturnValuesOnConditionCheckFailure": "ALL_OLD" 
            }
        }
        items.append(recordset)
        
        # recordset = {
        #     "Update": {
        #         "TableName": "TuCita247",
        #         "Key": {
        #             "PKID": {"S": 'APPO#' + appointmentId}, 
        #             "SKID": {"S": 'APPO#' + appointmentId}
        #         },
        #         "UpdateExpression": "REMOVE GSI3PK, GSI3SK", 
        #         "ReturnValuesOnConditionCheckFailure": "NONE" 
        #     }
        # }
        # items.append(recordset)

        recordset = {
            "Update": {
                "TableName": "TuCita247",
                "Key": {
                    "PKID": {"S": 'BUS#' + businessId}, 
                    "SKID": {"S": 'LOC#' + locationId}, 
                },
                "UpdateExpression": "SET PEOPLE_CHECK_IN = PEOPLE_CHECK_IN + :increment",
                "ExpressionAttributeValues": { 
                    ":increment": {"N": "1"}
                },
                "ConditionExpression": "attribute_exists(PKID) AND attribute_exists(SKID)",
                "ReturnValuesOnConditionCheckFailure": "ALL_OLD" 
            }
        }
        items.append(recordset)

        tranAppo = dynamodb.transact_write_items(
            TransactItems = items
        )

        table = dynamodbTable.Table('TuCita247')
        response = table.update_item(
            Key={
                'PKID': 'APPO#' + appointmentId,
                'SKID': 'APPO#' + appointmentId
            },
            UpdateExpression="REMOVE GSI3PK, GSI3SK",
            ReturnValues="NONE"
        )
        
        logger.info(tranAppo)
        statusCode = 200
        body = json.dumps({'Message': 'Appointment updated successfully', 'Code': 200})

        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Message': 'Error on update appointment', 'Code': 500})
    except dynamodb.exceptions.TransactionCanceledException as e:
        statusCode = 404
        body = json.dumps({'Message': 'QR Code invalid ' + str(e), 'Code': 404})
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