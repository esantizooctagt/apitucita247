import sys
import logging
import requests
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
sms = boto3.client('sns')
ses = boto3.client('ses', region_name=REGION)
lambdaInv = boto3.client('lambda')
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
        dateOpe = today.strftime("%Y-%m-%d")
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
        businessId = ''
        locationId = ''
        dateAppointment = ''
        for row in json_dynamodb.loads(response['Items']):
            getMessage = row['MESSAGES'] if 'MESSAGES' in row else []
            dateAppointment = row['DATE_APPO']
            keys = row['GSI1PK'].split('#')
            businessId = keys[1]
            locationId = keys[3]

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
        )

        data = {
            'BusinessId': businessId,
            'LocationId': locationId,
            'AppId': appointmentId,
            'User': 'U',
            'Tipo': 'MESS'
        }
        if dateOpe[0:10] == dateAppointment[0:10] and userType != "1":
            lambdaInv.invoke(
                FunctionName='PostMessages',
                InvocationType='Event',
                Payload=json.dumps(data)
            )

        phone = ''
        customerId = ''
        response = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :appointmentId AND SKID = :appointmentId',
            ExpressionAttributeValues={
                ':appointmentId': {'S': 'APPO#'+appointmentId}
            }
        )
        for row in json_dynamodb.loads(response['Items']):
            customerId = row['GSI2PK'].replace('CUS#','')
            phone = row['PHONE']

        if phone != '00000000000':
            # GET USER PREFERENCE NOTIFICATION
            response = dynamodb.query(
                TableName="TuCita247",
                IndexName="TuCita247_Index",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='GSI1PK = :key AND GSI1SK = :key',
                ExpressionAttributeValues={
                    ':key': {'S': 'CUS#' + customerId}
                }
            )
            preference = 0
            playerId = ''
            language = ''
            for row in json_dynamodb.loads(response['Items']):
                preference = int(row['PREFERENCES']) if 'PREFERENCES' in row else 0
                email = row['EMAIL'] if 'EMAIL' in row else ''
                playerId = row['PLAYERID'] if 'PLAYERID' in row else ''
                language = str(row['LANGUAGE']).lower() if 'LANGUAGE' in row else 'en'

            logger.info('Preference user ' + customerId + ' -- ' + str(preference))
            if language == 'en':
                sendMsg = 'Tu Cita 24/7. You received a new message: ' + message
            else:
                sendMsg = 'Tu Cita 24/7. Ha recibido un mensaje nuevo: ' + message
            #CODIGO UNICO DEL TELEFONO PARA PUSH NOTIFICATION ONESIGNAL
            if playerId != '':
                header = {"Content-Type": "application/json; charset=utf-8"}
                payload = {"app_id": "476a02bb-38ed-43e2-bc7b-1ded4d42597f",
                        "include_player_ids": [playerId],
                        "contents": {"en": sendMsg}}
                req = requests.post("https://onesignal.com/api/v1/notifications", headers=header, data=json.dumps(payload))

            if int(preference) == 1:
                #SMS
                to_number = phone
                bodyStr = sendMsg
                sms.publish(
                    PhoneNumber="+"+to_number,
                    Message=bodyStr,
                    MessageAttributes={
                            'AWS.SNS.SMS.SMSType': {
                            'DataType': 'String',
                            'StringValue': 'Transactional'
                        }
                    }
                )
                
            if int(preference) == 2 and email != '':
                #EMAIL
                SENDER = "Tu Cita 24/7 <no-reply@tucita247.com>"
                RECIPIENT = email
                SUBJECT = "Tu Cita 24/7"
                BODY_TEXT = (sendMsg)
                            
                # The HTML body of the email.
                BODY_HTML = """<html>
                <head></head>
                <body>
                <h1>Tu Cita 24/7</h1>
                <p>""" + sendMsg + """</p>
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