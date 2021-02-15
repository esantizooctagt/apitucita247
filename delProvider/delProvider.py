import sys
import requests
import logging
import json

import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr
from dynamodb_json import json_util as json_dynamodb

import datetime
import dateutil.tz
from datetime import timezone

import base64

import uuid
import os

REGION = 'us-east-1'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb', region_name=REGION)
dynamodbQry = boto3.client('dynamodb', region_name=REGION)
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
        busLanguage = 'es'
        providerId = event['pathParameters']['providerId']
        businessId = event['pathParameters']['businessId']
        locationId = event['pathParameters']['locationId']

        lanData = dynamodbQry.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :pkid AND SKID = :skid',
            ExpressionAttributeValues={
                ':pkid': {'S': 'BUS#' + businessId},
                ':skid': {'S': 'METADATA'}
            }
        )
        for lang in json_dynamodb.loads(lanData['Items']):
            busLanguage = lang['LANGUAGE'] if 'LANGUAGE' in lang else 'es'

        busName = dynamodbQry.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :pkid AND SKID = :skid',
            ExpressionAttributeValues={
                ':pkid': {'S': 'BUS#' + businessId},
                ':skid': {'S': 'LOC#' + locationId}
            }
        )
        businessName = ''
        Address = ''
        TimeZone = 'America/Puerto_Rico'
        for bus in json_dynamodb.loads(busName['Items']):
            businessName = bus['NAME']
            Address = bus['ADDRESS']
            TimeZone = bus['TIME_ZONE'] if 'TIME_ZONE' in bus else 'America/Puerto_Rico'

        country_date = dateutil.tz.gettz(TimeZone)
        today = datetime.datetime.now(tz=country_date)
        currDate = today.strftime("%Y-%m-%d-%H-00")
        nextDate = (today + datetime.timedelta(days=365)).strftime("%Y-%m-%d-23-59")
        dateOpe = today.strftime("%Y-%m-%d-%H-%M-%S")

        table = dynamodb.Table('TuCita247')
        response = table.update_item(
            Key={
                'PKID': 'BUS#' + businessId + '#LOC#' + locationId,
                'SKID': 'PRO#' + providerId
            },
            UpdateExpression="SET #s = :status",
            ExpressionAttributeNames={'#s': 'STATUS'},
            ExpressionAttributeValues={
                ':status': 2
            },
            ConditionExpression="attribute_exists(PKID) AND attribute_exists(SKID)",
            ReturnValues="NONE"
        )

        getAppos = dynamodbQry.query(
            TableName="TuCita247",
            IndexName="TuCita247_Index",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='GSI1PK = :key01 and GSI1SK between :skey01 AND :skey02',
            ExpressionAttributeValues={
                ':key01': {'S': 'BUS#'+businessId+'#LOC#'+locationId+'#PRO#'+providerId},
                ':skey01': {'S': '1#DT#'+currDate},
                ':skey02': {'S': '1#DT#'+nextDate}
            }
        )
        table = dynamodb.Table('TuCita247')
        for appo in json_dynamodb.loads(getAppos['Items']):
            updAppo = table.update_item(
                Key={
                    'PKID': appo['PKID'],
                    'SKID': appo['PKID']
                },
                UpdateExpression="SET GSI1SK = :dtkey, GSI2SK = :dtkey, GSI5PK = :buskey, GSI5SK = :skey5, GSI6PK = :pkey6, GSI6SK = :skey5, GSI7PK = :pkey7, GSI7SK = :skey5, GSI9SK = :dtkey, #s = :stat, TIMECANCEL = :timeNow, STATUS_CANCEL = :statCancel",
                ExpressionAttributeNames={'#s': 'STATUS'},
                ExpressionAttributeValues={
                    ':dtkey': str('5#DT#'+appo['DATE_APPO']),
                    ':buskey': 'BUS#'+businessId+'#5',
                    ':skey5': appo['DATE_APPO'][0:10]+'#'+appo['PKID'],
                    ':pkey6': 'BUS#'+businessId+'#LOC#'+locationId+'#5',
                    ':pkey7': 'BUS#'+businessId+'#LOC#'+locationId+'#'+providerId+'#5',
                    ':stat': 5,
                    ':statCancel': 5,
                    ':timeNow': dateOpe
                }
            )
            logger.info(updAppo)

            if appo['DATE_APPO'][0:10] == dateOpe[0:10]:
                data = {
                    'BusinessId': businessId,
                    'LocationId': locationId,
                    'AppId': appo['PKID'].replace('APPO#',''),
                    'CustomerId': appo['GSI2PK'].replace('CUS#',''),
                    'Tipo': 'CANCEL'
                }
                lambdaInv.invoke(
                    FunctionName='PostMessages',
                    InvocationType='Event',
                    Payload=json.dumps(data)
                )

                data = {
                    'BusinessId': businessId,
                    'CustomerId': appo['GSI2PK'].replace('CUS#',''),
                    'LocationId': locationId,
                    'AppId': appo['PKID'].replace('APPO#',''),
                    'Address': Address,
                    'NameBusiness': businessName,
                    'Guests': int(appo['PEOPLE_QTY']),
                    'QrCode': appo['QRCODE'],
                    'UnRead': 0,
                    'Ready': 0,
                    'DateFull': appo['DATE_APPO'],
                    'Disability': appo['DISABILITY'] if 'DISABILITY' in appo else '',
                    'Door': appo['DOOR'],
                    'Name': appo['NAME'],
                    'OnBehalf': appo['ON_BEHALF'],
                    'Phone': appo['PHONE'],
                    'Status': 5,
                    'Tipo': 'MOVE',
                    'TimeZone': TimeZone,
                    'To': 'EXPIRED'
                }
                lambdaInv.invoke(
                    FunctionName='PostMessages',
                    InvocationType='Event',
                    Payload=json.dumps(data)
                )

            # GET USER PREFERENCE NOTIFICATION
            customer = dynamodbQry.query(
                TableName="TuCita247",
                IndexName="TuCita247_Index",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='GSI1PK = :key AND GSI1SK = :key',
                ExpressionAttributeValues={
                    ':key': {'S': 'CUS#' + appo['GSI2PK'].replace('CUS#','')}
                }
            )
            preference = 0
            playerId = ''
            msg = ''
            language = busLanguage
            for row in json_dynamodb.loads(customer['Items']):
                preference = int(row['PREFERENCES']) if 'PREFERENCES' in row else 0
                mobile = row['PKID'].replace('MOB#','')
                # email = row['EMAIL'] if 'EMAIL' in row else ''
                email = row['EMAIL_COMM'] if 'EMAIL_COMM' in row else row['EMAIL'] if 'EMAIL' in row else ''
                playerId = row['PLAYERID'] if 'PLAYERID' in row else ''
                if playerId != '':
                    language = str(row['LANGUAGE']).lower() if 'LANGUAGE' in row else busLanguage
            
            hrAppo = datetime.datetime.strptime(appo['DATE_APPO'], '%Y-%m-%d-%H-%M').strftime('%I:%M %p')
            dayAppo = datetime.datetime.strptime(appo['DATE_APPO'][0:10], '%Y-%m-%d').strftime('%b %d %Y')
            if language == "en":
                msg = businessName + ' has canceled your booking for ' + dayAppo  + ', ' + hrAppo + '. Reason: LOCATION CLOSED'
            else:
                msg = businessName + ' ha cancelado su cita para ' + dayAppo + ', ' + hrAppo + '. Razón: LOCALIDAD CERRADA'
                
            #CODIGO UNICO DEL TELEFONO PARA PUSH NOTIFICATION ONESIGNAL
            if playerId != '':
                header = {"Content-Type": "application/json; charset=utf-8"}
                payload = {"app_id": "476a02bb-38ed-43e2-bc7b-1ded4d42597f",
                        "include_player_ids": [playerId],
                        "contents": {"en": msg}}
                req = requests.post("https://onesignal.com/api/v1/notifications", headers=header, data=json.dumps(payload))

            if preference == 1 and mobile != '00000000000':
                #SMS
                to_number = mobile
                bodyStr = msg
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
                
            if preference == 2 and email != '':
                #EMAIL
                SENDER = "Tu Cita 24/7 <no-reply@tucita247.com>"
                RECIPIENT = email
                SUBJECT = "Tu Cita 24/7"
                BODY_TEXT = (msg)
                            
                # The HTML body of the email.
                BODY_HTML = """<html>
                <head></head>
                <body>
                <h1>Tu Cita 24/7</h1>
                <p>""" + msg + """</p>
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
        body = json.dumps({'Message': 'Service provider deleted successfully', 'Code': 200})

        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Message': 'Error on deleted service provider'})
    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message': 'Error on request try again ' + str(e)})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : cors
        },
        'body' : body
    }
    return response