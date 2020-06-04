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
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']
        
    try:
        statusCode = ''
        appointmentId = ''
        dateAppo = ''
        data = json.loads(event['body'])
        status = data['Status']
        qrCode = data['qrCode']
        businessId = data['BusinessId']
        locationId = data['LocationId']

        country_date = dateutil.tz.gettz('America/Puerto_Rico')
        today = datetime.datetime.now(tz=country_date)
        dateOpe = today.strftime("%Y-%m-%d")
        
        response = dynamodb.query(
            TableName="TuCita247",
            IndexName="TuCita247_Appos",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='GSI3PK = :key01 AND GSI3SK = :key02',
            ExpressionAttributeNames=e,
            ExpressionAttributeValues={
                ':key01': {'S': 'BUS#'+businessId+'#LOC#'+locationId+'#'+dateOpe},
                ':key02': {'S': 'QR#'+qrCode}
            }
        )
        for row in json_dynamodb.loads(response['Items']):
            appointmentId = row['PKID']
            dateAppo = row['DATE_APPO']

        if appointmentId != '':
            items = []
            recordset = {
                "Update": {
                    "TableName": "TuCita247",
                    "Key": {
                        "PKID": {"S": appointmentId}, 
                        "SKID": {"S": appointmentId}
                    },
                    "UpdateExpression": "SET #s = :status, GSI1SK = :key, GSI2SK = :key, TIMECHECKOUT = :dateOpe", 
                    "ExpressionAttributeValues": {
                        ":status": {"N": str(status)}, 
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

            recordset = {
                "Update": {
                    "TableName": "TuCita247",
                    "Key": {
                        "PKID": {"S": 'BUS#' + businessId}, 
                        "SKID": {"S": 'LOC#' + locationId}, 
                    },
                    "UpdateExpression": "SET PEOPLE_CHECK_IN = PEOPLE_CHECK_IN - :increment",
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
            
            logger.info(tranAppo)
            statusCode = 200
            body = json.dumps({'Message': 'Appointment updated successfully', 'Code': 200})
        else:
            statusCode = 404
            body = json.dumps({'Message': 'QR Code invalid ' + str(e), 'Code': 404})

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