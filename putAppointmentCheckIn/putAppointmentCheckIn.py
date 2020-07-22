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
# dynamodbTable = boto3.resource('dynamodb', region_name='us-east-1')
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def cleanNullTerms(d):
   clean = {}
   for k, v in d.items():
      if isinstance(v, dict):
         nested = cleanNullTerms(v)
         if len(nested.keys()) > 0:
            clean[k] = nested
      elif v is not None:
         clean[k] = v
   return clean
   
def lambda_handler(event, context):
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']
        
    try:
        statusCode = ''
        typeAppo = ''
        data = json.loads(event['body'])
        appointmentId = event['pathParameters']['id']
        status = data['Status']
        dateAppo = data['DateAppo']
        qty = data['Guests']
        qrCode = data['qrCode'] if 'qrCode' in data else ''
        businessId = data['BusinessId'] if 'BusinessId' in data else ''
        locationId = data['LocationId'] if 'LocationId' in data else ''
        serviceId = data['ServiceId'] if 'ServiceId' in data else ''

        country_date = dateutil.tz.gettz('America/Puerto_Rico')
        today = datetime.datetime.now(tz=country_date)
        dateOpe = today.strftime("%Y-%m-%d-%H-%M-%S")

        response = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :pkid AND SKID = :skid',
            ExpressionAttributeValues={
                ':pkid': {'S': 'APPO#' + appointmentId},
                ':skid': {'S': 'APPO#' + appointmentId}
            }
        )
        for row in json_dynamodb.loads(response['Items']):
            typeAppo = row['TYPE']

        items = []
        recordset = {
            "Update": {
                "TableName": "TuCita247",
                "Key": {
                    "PKID": {"S": 'APPO#' + appointmentId}, 
                    "SKID": {"S": 'APPO#' + appointmentId}
                },
                "UpdateExpression": "SET #s = :status, GSI1SK = :key, GSI2SK = :key2, TIMECHECKIN = :dateOpe, PEOPLE_QTY = :qty" + ("" if typeAppo != 2 else ", GSI4PK = :key4, GSI4SK = :skey4"), 
                "ExpressionAttributeValues": {
                    ":status": {"N": "3"}, 
                    ":key": {"S": str(status) + '#DT#' + str(dateAppo)}, 
                    ":key2": {"S": '#5' if str(status) == '5' else str(status) + '#DT#' + str(dateAppo)}, 
                    ":dateOpe": {"S": str(dateOpe)},
                    ":qrCode": {"S": qrCode},
                    ":key4": {"S": None if typeAppo != 2 else "BUS#" + businessId + "#LOC#" + locationId},
                    ":qty": {"N": str(qty)},
                    ":skey4": {"S": None if typeAppo != 2 else str(status) + "#DT#" + str(dateAppo) + "#" + appointmentId}
                },
                "ExpressionAttributeNames": {'#s': 'STATUS'},
                "ConditionExpression": "attribute_exists(PKID) AND attribute_exists(SKID) AND QRCODE = :qrCode",
                "ReturnValuesOnConditionCheckFailure": "ALL_OLD" 
            }
        }
        items.append(cleanNullTerms(recordset))
        
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
                    ":increment": {"N": str(qty)}
                },
                "ConditionExpression": "attribute_exists(PKID) AND attribute_exists(SKID)",
                "ReturnValuesOnConditionCheckFailure": "ALL_OLD" 
            }
        }
        items.append(recordset)

        recordset = {
            "Update": {
                "TableName": "TuCita247",
                "Key": {
                    "PKID": {"S": 'BUS#' + businessId + '#' + locationId}, 
                    "SKID": {"S": 'SER#' + serviceId}, 
                },
                "UpdateExpression": "SET PEOPLE_CHECK_IN = PEOPLE_CHECK_IN + :increment",
                "ExpressionAttributeValues": { 
                    ":increment": {"N": str(qty)}
                },
                "ConditionExpression": "attribute_exists(PKID) AND attribute_exists(SKID)",
                "ReturnValuesOnConditionCheckFailure": "ALL_OLD" 
            }
        }
        items.append(recordset)

        tranAppo = dynamodb.transact_write_items(
            TransactItems = items
        )

        # table = dynamodbTable.Table('TuCita247')
        # response = table.update_item(
        #     Key={
        #         'PKID': 'APPO#' + appointmentId,
        #         'SKID': 'APPO#' + appointmentId
        #     },
        #     UpdateExpression="REMOVE GSI3PK, GSI3SK",
        #     ReturnValues="NONE"
        # )
        
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