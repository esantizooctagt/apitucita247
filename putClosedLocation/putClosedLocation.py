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
dynamoQr = boto3.client('dynamodb', region_name=REGION)
sms = boto3.client('sns')
ses = boto3.client('ses', region_name=REGION)
lambdaInv = boto3.client('lambda')
logger.info("SUCCESS: Connection to DynamoDB succeeded")
#falta cerrar las horas del dia a partir de la fecha/hora
def lambda_handler(event, context):
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']
        
    try:
        statusCode = ''
        locationId = event['pathParameters']['id']
        businessId = event['pathParameters']['businessId']
        closed = int(event['pathParameters']['closed'])
        busLanguage = event['pathParameters']['busLanguage']
        
        country_date = dateutil.tz.gettz('America/Puerto_Rico')
        today = datetime.datetime.now(tz=country_date)
        dateOpe = today.strftime("%Y-%m-%d-%H-%M-%S")
        dateNow = today.strftime("%Y-%m-%d")
        dateFin = today.strftime("%Y-%m-%d")
        hourNow = today.strftime("%H")

        busName = dynamoQr.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :pkid AND SKID = :skid',
            ExpressionAttributeValues={
                ':pkid': {'S': 'BUS#' + businessId},
                ':skid': {'S': 'LOC#' + locationId}
            }
        )
        businessName = ''
        for bus in json_dynamodb.loads(busName['Items']):
            businessName = bus['NAME']

        table = dynamodb.Table('TuCita247')
        if closed == 0:
            response = table.update_item(
                Key={
                    'PKID': 'BUS#' + businessId,
                    'SKID': 'LOC#' + locationId
                },
                UpdateExpression="SET PEOPLE_CHECK_IN = :qty", 
                ExpressionAttributeValues= {':qty': 0},
                ReturnValues="UPDATED_NEW"
            )

            data = {
                'BusinessId': businessId,
                'LocationId': locationId,
                'Tipo': 'RESET'
            }
            lambdaInv.invoke(
                FunctionName='PostMessages',
                InvocationType='Event',
                Payload=json.dumps(data)
            )

        if closed == 1:
            response = table.update_item(
                Key={
                    'PKID': 'BUS#' + businessId,
                    'SKID': 'LOC#' + locationId
                },
                UpdateExpression="SET PEOPLE_CHECK_IN = :qty, OPEN_DATE = :closed, #o = :open", 
                ExpressionAttributeValues= {':qty': 0, ':closed': '', ':initVal': 1, ':open': 0},
                ExpressionAttributeNames={'#o': 'OPEN'},
                ConditionExpression='#o = :initVal',
                ReturnValues="UPDATED_NEW"
            )

            data = {
                'BusinessId': businessId,
                'LocationId': locationId,
                'Tipo': 'CLOSED'
            }
            lambdaInv.invoke(
                FunctionName='PostMessages',
                InvocationType='Event',
                Payload=json.dumps(data)
            )

            providers = dynamoQr.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :pkid AND begins_with(SKID, :skid)',
                ExpressionAttributeValues={
                    ':pkid': {'S': 'BUS#' + businessId + '#LOC#' + locationId},
                    ':skid': {'S': 'PRO#'}
                }
            )
            for provider in json_dynamodb.loads(providers['Items']):
                for i in range(1, 3):
                    appos = dynamoQr.query(
                        TableName="TuCita247",
                        IndexName="TuCita247_Index",
                        ReturnConsumedCapacity='TOTAL',
                        KeyConditionExpression='GSI1PK = :gsi1pk AND GSI1SK BETWEEN :gsi1sk_ini AND :gsi1sk_fin',
                        ExpressionAttributeValues={
                            ':gsi1pk': {'S': 'BUS#' + businessId + '#LOC#' + locationId + '#PRO#' + provider['SKID'].replace('PRO#','')},
                            ':gsi1sk_ini': {'S': str(i)+'#DT#' + dateNow+'-00-00'},
                            ':gsi1sk_fin': {'S': str(i)+'#DT#' + dateFin+'-23-59'}
                        }
                    )
                    #DISABLED HOURS FROM NOW TO 23
                    for hr in range(int(hourNow), 24):
                        logger.info('LOC#'+locationId+'#'+provider['SKID']+'#DT#'+dateNow)
                        logger.info('HR#'+str(hr).zfill(2)+'-00')
                        updHr = table.update_item(
                            Key={
                                'PKID': 'LOC#'+locationId+'#'+provider['SKID']+'#DT#'+dateNow,
                                'SKID': 'HR#'+str(hr).zfill(2)+'-00'
                            },
                            UpdateExpression="SET AVAILABLE = :available, CANCEL = :cancel", #, TIME_SERVICE = :service",
                            ExpressionAttributeValues={
                                ':available': 0,
                                ':cancel': 1
                                # ':service': 1
                            }
                        )
                        
                    table = dynamodb.Table('TuCita247')
                    for appo in json_dynamodb.loads(appos['Items']):
                        
                        updAppo = table.update_item(
                            Key={
                                'PKID': appo['PKID'],
                                'SKID': appo['PKID']
                            },
                            UpdateExpression="SET GSI1SK = :dtkey, GSI2SK = :keyCancel, GSI5PK = :buskey, GSI5SK = :skey5, GSI6PK = :pkey6, GSI6SK = :skey5, GSI7PK = :pkey7, GSI7SK = :skey5, GSI9SK = :dtkey, #s = :stat, TIMECANCEL = :timeNow",
                            ExpressionAttributeNames={'#s': 'STATUS'},
                            ExpressionAttributeValues={
                                ':dtkey': str('5#DT#'+appo['DATE_APPO']),
                                ':keyCancel': str('#5'),
                                ':buskey': 'BUS#'+businessId+'#5',
                                ':skey5': appo['DATE_APPO'][0:10]+'#'+appo['PKID'],
                                ':pkey6': 'BUS#'+businessId+'#LOC#'+locationId+'#5',
                                ':pkey7': 'BUS#'+businessId+'#LOC#'+locationId+'#'+provider['SKID']+'#5',
                                ':stat': 5,
                                ':timeNow': dateOpe
                            }
                            # ReturnValues="UPDATED_NEW"
                        )
                        logger.info(updAppo)
                        
                        # GET USER PREFERENCE NOTIFICATION
                        customer = dynamoQr.query(
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
                        for row in json_dynamodb.loads(customer['Items']):
                            preference = int(row['PREFERENCES']) if 'PREFERENCES' in row else 0
                            mobile = row['PKID'].replace('MOB#','')
                            email = row['EMAIL'] if 'EMAIL' in row else ''
                            playerId = row['PLAYERID'] if 'PLAYERID' in row else ''
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
                            SUBJECT = "Tu Cita 24/7 Check-In"
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
        body = json.dumps({'Message': 'Service closed successfully', 'Code': 200, 'Business': json_dynamodb.loads(response['Attributes'])})

        logger.info(response)
        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Message': 'Error on closed location', 'Code': 500})
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