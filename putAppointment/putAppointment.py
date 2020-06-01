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
dynamodbTran = boto3.client('dynamodb', region_name='us-east-1')
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
        reasonId = data['Reason'] if 'Reason' in data else ''
        qrCode = data['qrCode'] if 'qrCode' in data else ''
        businessId = data['BusinessId'] if 'BusinessId' in data else ''
        locationId = data['LocationId'] if 'LocationId' in data else ''

        country_date = dateutil.tz.gettz('America/Puerto_Rico')
        today = datetime.datetime.now(tz=country_date)
        dateOpe = today.strftime("%Y-%m-%d-%H-%M-%S")

        table = dynamodb.Table('TuCita247')
        e = {'#s': 'STATUS'}
        if reasonId != '':
            v = {':status': str(status), ':key01': str(status) + '#DT#' + str(dateAppo), ':reason': reasonId, ':dateope': dateOpe}
        else:
            v = {':status': str(status), ':key01': str(status) + '#DT#' + str(dateAppo), ':dateope': dateOpe}
        
        c = ''
        if str(status) == "3":
            items = []
            if qrCode != 'VALID':
                # c = 'QRCODE = :qrCode'
                # v = {':status': str(status), ':key01': str(status) + '#DT#' + str(dateAppo), ':dateope': dateOpe, ':qrCode': qrCode}
                # response = table.update_item(
                #     Key={
                #         'PKID': 'APPO#' + appointmentId,
                #         'SKID': 'APPO#' + appointmentId
                #     },
                #     UpdateExpression="set #s = :status, GSI1SK = :key01, GSI2SK = :key01, TIMECHECKIN = :dateope",
                #     ExpressionAttributeNames=e,
                #     ConditionExpression=c,
                #     ExpressionAttributeValues=v,
                #     ReturnValues="UPDATED_NEW"
                # )
                recordset = {
                    "Put": {
                        "TableName": "TuCita247",
                        "Item": {
                            "PKID": {"S": 'APPO#' + appointmentId}, 
                            "SKID": {"S": 'APPO#' + appointmentId}, 
                            "STATUS": {"N": "3"}, 
                            "GSI1SK": {"S": str(status) + '#DT#' + str(dateAppo)}, 
                            "GSI2SK": {"S": str(status) + '#DT#' + str(dateAppo)},
                            "TIMECHECKIN": {"S": str(dateAppo) }
                        },
                        "ConditionExpression": "attribute_exists(PKID) AND attribute_exists(SKID) AND QRCODE = " + qrCode,
                        "ReturnValuesOnConditionCheckFailure": "UPDATED_NEW" 
                    }
                }
                items.append(recordset)
            else:
                # response = table.update_item(
                #     Key={
                #         'PKID': 'APPO#' + appointmentId,
                #         'SKID': 'APPO#' + appointmentId
                #     },
                #     UpdateExpression="set #s = :status, GSI1SK = :key01, GSI2SK = :key01, TIMECHECKIN = :dateope",
                #     ExpressionAttributeNames=e,
                #     ExpressionAttributeValues=v,
                #     ReturnValues="UPDATED_NEW"
                # )
                recordset = {
                    "Put": {
                        "TableName": "TuCita247",
                        "Item": {
                            "PKID": {"S": 'APPO#' + appointmentId}, 
                            "SKID": {"S": 'APPO#' + appointmentId}, 
                            "STATUS": {"N": "3"}, 
                            "GSI1SK": {"S": str(status) + '#DT#' + str(dateAppo)}, 
                            "GSI2SK": {"S": str(status) + '#DT#' + str(dateAppo)},
                            "TIMECHECKIN": {"S": str(dateAppo) }
                        },
                        "ConditionExpression": "attribute_exists(PKID) AND attribute_exists(SKID)",
                        "ReturnValuesOnConditionCheckFailure": "UPDATED_NEW" 
                    }
                }
                items.append(recordset)

            recordset = {
                "Update ": {
                    "TableName": "TuCita247",
                    "Key": {
                        "PKID": {"S": 'BUS#' + businessId}, 
                        "SKID": {"S": 'LOC#' + locationId}, 
                    },
                    "UpdateExpression" :  "PEOPLE_CHECK_IN = PEOPLE_CHECK_IN + 1",
                    "ConditionExpression": "attribute_exists(PKID) AND attribute_exists(SKID)",
                    "ReturnValuesOnConditionCheckFailure": "UPDATED_NEW" 
                }
            }
            items.append(recordset)
            
            logger.info(items)
            response = dynamodb.transact_write_items(
                TransactItems = items
            )

            statusCode = 200
            body = json.dumps({'Message': 'Appointment updated successfully', 'Code': 200, 'Appo': json_dynamodb.loads(response['Items'])})
        else:
            response = table.update_item(
                Key={
                    'PKID': 'APPO#' + appointmentId,
                    'SKID': 'APPO#' + appointmentId
                },
                UpdateExpression="set #s = :status, GSI1SK = :key01, GSI2SK = :key01" + (", TIMECHEK = :dateope" if str(status) == "2" else "") + (", TIMECANCEL = :dateope" if str(status) == "5" else "") + (", REASONID = :reason" if reasonId != "" else ""),
                ExpressionAttributeNames=e,
                ExpressionAttributeValues=v,
                ReturnValues="UPDATED_NEW"
            )

            statusCode = 200
            body = json.dumps({'Message': 'Appointment updated successfully', 'Code': 200, 'Appo': json_dynamodb.loads(response['Items'])})

        logger.info(response)
        #PASA A PRE-CHECK IN Y ENVIA NOTIFICACION POR TWILIO A SMS y CORREO (TWILIO), ONESIGNAL (PUSH NOTIFICATION)
        # if status == 2:
        #     # GET USER PREFERENCE NOTIFICATION
        #     status = 1
        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Message': 'Error on update appointment', 'Code': 500})
    except ClientError as e:  
        if e.response['Error']['Code']=='ConditionalCheckFailedException':  
            statusCode = 404
            body = json.dumps({'Message': 'Invalida qr code', 'Code': 400})
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