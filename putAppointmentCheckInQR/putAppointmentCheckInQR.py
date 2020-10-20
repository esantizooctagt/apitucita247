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
lambdaInv = boto3.client('lambda')
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
        dateAppo = ''
        appointmentId = ''
        
        data = json.loads(event['body'])
        status = data['Status']
        qty = data['Guests']
        qrCode = data['qrCode'].upper() if 'qrCode' in data else ''
        businessId = data['BusinessId'] if 'BusinessId' in data else ''
        locationId = data['LocationId'] if 'LocationId' in data else ''
        # providerId = data['ProviderId'] if 'ProviderId' in data else ''

        country_date = dateutil.tz.gettz('America/Puerto_Rico')
        today = datetime.datetime.now(tz=country_date)
        dateOpe = today.strftime("%Y-%m-%d")
        service = ''

        e = {'#s': 'STATUS'}
        f = '#s <= :stat'
        response = dynamodb.query(
            TableName="TuCita247",
            IndexName="TuCita247_Appos",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='GSI3PK = :key01 AND GSI3SK = :key02',
            ExpressionAttributeNames=e,
            FilterExpression=f,
            ExpressionAttributeValues={
                ':key01': {'S': 'BUS#'+businessId+'#LOC#'+locationId+'#'+dateOpe},
                ':key02': {'S': 'QR#'+qrCode},
                ':stat' : {'N': '2'}
            }
        )
        for row in json_dynamodb.loads(response['Items']):
            appointmentId = row['PKID'].replace('APPO#','')
            dateAppo = row['DATE_APPO']
            customerId = row['GSI2PK'].replace('CUS#','')
            providerId = row['GSI1PK'].replace('BUS#'+businessId+'#LOC#'+locationId+'#PRO#','')

        items = []
        dateOpe = today.strftime("%Y-%m-%d-%H-%M-%S")
        if appointmentId != '':
            recordset = {
                "Update": {
                    "TableName": "TuCita247",
                    "Key": {
                        "PKID": {"S": 'APPO#' + appointmentId}, 
                        "SKID": {"S": 'APPO#' + appointmentId}
                    },
                    "UpdateExpression": "SET #s = :status, GSI1SK = :key, GSI2SK = :key2, TIMECHECKIN = :dateOpe, PEOPLE_QTY = :qty, GSI5PK = :key05, GSI5SK = :skey05, GSI6PK = :key06, GSI6SK = :skey06, GSI7PK = :key07, GSI7SK = :skey07, GSI9SK = :key", 
                    "ExpressionAttributeValues": {
                        ":status": {"N": "3"}, 
                        ":key": {"S": str(status) + '#DT#' + str(dateAppo)}, 
                        ":key2": {"S": '#5' if str(status) == '5' else  str(status) + '#DT#' + str(dateAppo)[0:10]}, 
                        ":qty": {"N": str(qty)},
                        ":dateOpe": {"S": str(dateOpe)},
                        ":key05": {"S" : 'BUS#' + businessId},
                        ":skey05": {"S" : str(dateAppo)[0:10]+'#APPO#' + appointmentId},
                        ":key06": {"S" : 'BUS#' + businessId + '#LOC#' + locationId},
                        ":skey06": {"S" : str(dateAppo)[0:10]+'#APPO#' + appointmentId},
                        ":key07": {"S" : 'BUS#' + businessId + '#LOC#' + locationId + '#PRO#' + providerId},
                        ":skey07": {"S" : str(dateAppo)[0:10]+'#APPO#' + appointmentId}
                    },
                    "ExpressionAttributeNames": {'#s': 'STATUS'},
                    "ConditionExpression": "attribute_exists(PKID) AND attribute_exists(SKID)",
                    "ReturnValuesOnConditionCheckFailure": "ALL_OLD" 
                }
            }
            items.append(cleanNullTerms(recordset))

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

            # recordset = {
            #     "Update": {
            #         "TableName": "TuCita247",
            #         "Key": {
            #             "PKID": {"S": 'BUS#' + businessId + '#LOC#' + locationId}, 
            #             "SKID": {"S": 'PRO#' + providerId}, 
            #         },
            #         "UpdateExpression": "SET PEOPLE_CHECK_IN = PEOPLE_CHECK_IN + :increment",
            #         "ExpressionAttributeValues": { 
            #             ":increment": {"N": str(qty)}
            #         },
            #         "ConditionExpression": "attribute_exists(PKID) AND attribute_exists(SKID)",
            #         "ReturnValuesOnConditionCheckFailure": "ALL_OLD" 
            #     }
            # }
            # items.append(recordset)

            tranAppo = dynamodb.transact_write_items(
                TransactItems = items
            )
            logger.info("entro dateOpe " + dateOpe[0:10])
            logger.info("entro daeAppo " +  dateAppo[0:10])
            if dateOpe[0:10] == dateAppo[0:10]:
                data = {
                    'BusinessId': businessId,
                    'LocationId': locationId,
                    'AppId': appointmentId,
                    'Guests': qty,
                    'Tipo': 'MOVE',
                    'To': 'CHECKIN'
                }
                logger.info("entro")
                logger.info(data)
                lambdaInv.invoke(
                    FunctionName='PostMessages',
                    InvocationType='Event',
                    Payload=json.dumps(data)
                )

            logger.info(tranAppo)
            statusCode = 200
            body = json.dumps({'Message': 'Appointment updated successfully', 'Code': 200})
        else:
            statusCode = 404
            body = json.dumps({'Message': 'Invalid appointment, please verify', 'Code': 404})

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