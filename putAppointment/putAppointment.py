import sys
import logging
import requests
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

dynamodb = boto3.resource('dynamodb', region_name=REGION)
dynamodbQuery = boto3.client('dynamodb', region_name=REGION)
sms = boto3.client('sns')
ses = boto3.client('ses',region_name=REGION)
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']
        
    try:
        statusCode = ''
        dataId = ''
        appoData = ''
        businessId = ''
        locationId = ''
        providerId = ''

        data = json.loads(event['body'])
        appointmentId = event['pathParameters']['id']
        
        status = data['Status']
        dateAppo = data['DateAppo']
        reasonId = data['Reason'] if 'Reason' in data else ''
        customerId = data['CustomerId'] if 'CustomerId' in data else ''

        country_date = dateutil.tz.gettz('America/Puerto_Rico')
        today = datetime.datetime.now(tz=country_date)
        dateOpe = today.strftime("%Y-%m-%d-%H-%M-%S")

        response = dynamodbQuery.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :appointmentId AND SKID = :appointmentId',
            ExpressionAttributeValues={
                ':appointmentId': {'S': 'APPO#' + appointmentId}
            }
        )
        for row in json_dynamodb.loads(response['Items']):
            dataId = row['GSI1PK']
            appoData = str(row['DATE_APPO'])[0:10]+'#APPO#'+appointmentId
            if dataId != '':
                businessId = 'BUS#'+dataId.split('#')[1]+'#5'
                locationId = 'BUS#'+dataId.split('#')[1]+'#LOC#'+dataId.split('#')[3]+'#5'
                providerId = 'BUS#'+dataId.split('#')[1]+'#LOC#'+dataId.split('#')[3]+'#PRO#'+dataId.split('#')[5]+'#5'

        table = dynamodb.Table('TuCita247')
        e = {'#s': 'STATUS'}
        if reasonId != '':
            v = {':status': status, ':key01': str(status) + '#DT#' + str(dateAppo), ':key02': '#5', ':reason': reasonId, ':dateope': dateOpe, ':pkey05': businessId, ':skey05': appoData, ':pkey06': locationId, ':skey06': appoData, ':pkey07': providerId, ':skey07': appoData}
        else:
            v = {':status': status, ':key01': str(status) + '#DT#' + str(dateAppo), ':key02': str(status) + '#DT#' + str(dateAppo), ':dateope': dateOpe}
        
        cancelStr = ''
        if str(status) == "5":
            cancelStr = ', GSI5PK = :pkey05, GSI5SK = :skey05, GSI6PK = :pkey06, GSI6SK = :skey06, GSI7PK = :pkey07, GSI7SK = :skey07, TIMECANCEL = :dateope'

        response = table.update_item(
            Key={
                'PKID': 'APPO#' + appointmentId,
                'SKID': 'APPO#' + appointmentId
            },
            UpdateExpression="set #s = :status, GSI1SK = :key01, GSI2SK = :key02" + (", TIMECHEK = :dateope" if str(status) == "2" else "") + (", REASONID = :reason" if reasonId != "" else "") + cancelStr,
            ExpressionAttributeNames=e,
            ExpressionAttributeValues=v,
            ReturnValues="UPDATED_NEW"
        )

        statusCode = 200
        body = json.dumps({'Message': 'Appointment updated successfully', 'Code': 200, 'Appo': json_dynamodb.loads(response['Attributes'])})

        logger.info(response)
        #PASA A PRE-CHECK IN Y ENVIA NOTIFICACION POR SNS A SMS y CORREO, ONESIGNAL (PUSH NOTIFICATION)
        if status == 2:
            # GET USER PREFERENCE NOTIFICATION
            response = dynamodbQuery.query(
                TableName="TuCita247",
                IndexName="TuCita247_Index",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='GSI1PK = :key AND GSI1SK = :key',
                ExpressionAttributeValues={
                    ':key': {'S': 'CUS#' + customerId}
                }
            )
            preference = ''
            playerId = ''
            for row in json_dynamodb.loads(response['Items']):
                preference = row['PREFERENCES'] if 'PREFERENCES' in row else ''
                mobile = row['PKID'].replace('MOB#','')
                email = row['EMAIL'] if 'EMAIL' in row else ''
                playerId = row['PLAYERID'] if 'PLAYERID' in row else ''
            
            #CODIGO UNICO DEL TELEFONO PARA PUSH NOTIFICATION ONESIGNAL
            if playerId != '':
                header = {"Content-Type": "application/json; charset=utf-8"}
                payload = {"app_id": "476a02bb-38ed-43e2-bc7b-1ded4d42597f",
                        "include_player_ids": [playerId],
                        "contents": {"en": "You can go to the nearest entrance to check in"}}
                req = requests.post("https://onesignal.com/api/v1/notifications", headers=header, data=json.dumps(payload))

            if preference == 1 and mobile != '':
                #SMS
                to_number = mobile
                bodyStr = 'You can go to the nearest entrance to check in'
                sms.publish(
                    PhoneNumber="+"+to_number,
                    Message=bodyStr
                )
                
            if preference == 2 and email != '':
                #EMAIL
                SENDER = "Tu Cita 24/7 <no-reply@tucita247.com>"
                RECIPIENT = email
                SUBJECT = "Tu Cita 24/7 Check-In"
                BODY_TEXT = ("You can yo to the nearest entrance to check in")
                            
                # The HTML body of the email.
                BODY_HTML = """<html>
                <head></head>
                <body>
                <h1>Tu Cita 24/7</h1>
                <p>You can yo to the nearest entrance to check in</p>
                </body>
                </html>"""

                CHARSET = "UTF-8"

                response = ses.send_email(
                    Destination={
                        'ToAddresses': [
                            RECIPIENT,
                        ],
                    },
                    Message={
                        'Body': {
                            'Html': {
                                'Charset': CHARSET,
                                'Data': BODY_HTML,
                            },
                            'Text': {
                                'Charset': CHARSET,
                                'Data': BODY_TEXT,
                            },
                        },
                        'Subject': {
                            'Charset': CHARSET,
                            'Data': SUBJECT,
                        },
                    },
                    Source=SENDER
                )

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