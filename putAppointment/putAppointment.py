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
ses = boto3.client('ses', region_name=REGION)
lambdaInv = boto3.client('lambda')
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def findTimeZone(businessId, locationId):
    timeZone='America/Puerto_Rico'
    locZone = dynamodbQuery.query(
        TableName="TuCita247",
        ReturnConsumedCapacity='TOTAL',
        KeyConditionExpression='PKID = :key AND SKID = :skey',
        ExpressionAttributeValues={
            ':key': {'S': 'BUS#'+businessId},
            ':skey': {'S': 'LOC#'+locationId}
        }
    )
    for timeLoc in json_dynamodb.loads(locZone['Items']):
        timeZone = timeLoc['TIME_ZONE'] if 'TIME_ZONE' in timeLoc else 'America/Puerto_Rico'
    return timeZone

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
        busId = ''
        locId = ''
        busLanguage = ''

        data = json.loads(event['body'])
        appointmentId = event['pathParameters']['id']
        status = data['Status']
        dateAppo = data['DateAppo']
        guests = data['Guests'] if 'Guests' in data else ''
        reasonId = data['Reason'] if 'Reason' in data else ''
        customerId = data['CustomerId'] if 'CustomerId' in data else ''
        businessName = data['BusinessName']
        busLanguage = data['Language']

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
            customerId = row['GSI2PK'].replace('CUS#','')
            appoData = str(row['DATE_APPO'])[0:10]+'#APPO#'+appointmentId
            if dataId != '':
                busId = dataId.split('#')[1]
                locId = dataId.split('#')[3]
                businessId = 'BUS#'+dataId.split('#')[1]+'#5'
                locationId = 'BUS#'+dataId.split('#')[1]+'#LOC#'+dataId.split('#')[3]+'#5'
                providerId = 'BUS#'+dataId.split('#')[1]+'#LOC#'+dataId.split('#')[3]+'#PRO#'+dataId.split('#')[5]+'#5'
                keyUpd = 'LOC#'+dataId.split('#')[3]+'#PRO#'+dataId.split('#')[5]+'#DT#'+dateAppo[0:10]

        country_date = dateutil.tz.gettz(findTimeZone(busId, locId))
        today = datetime.datetime.now(tz=country_date)
        dateOpe = today.strftime("%Y-%m-%d-%H-%M-%S")
        table = dynamodb.Table('TuCita247')
        e = {'#s': 'STATUS'}
        logger.info("prev todo ok")
        if str(status) != "5":
            if str(status) != "2":
                v = {':status': status, ':key01': str(status) + '#DT#' + str(dateAppo), ':key02': str(status) + '#DT#' + str(dateAppo), ':dateope': dateOpe}
                response = table.update_item(
                    Key={
                        'PKID': 'APPO#' + appointmentId,
                        'SKID': 'APPO#' + appointmentId
                    },
                    UpdateExpression="SET #s = :status, GSI1SK = :key01, GSI2SK = :key02, GSI9SK = :key01",
                    ExpressionAttributeNames=e,
                    ExpressionAttributeValues=v,
                    ReturnValues="UPDATED_NEW"
                )
                appo = json_dynamodb.loads(response['Attributes'])
            else:
                logger.info("status 2 ok")
                v = {':status': status, ':key01': str(status) + '#DT#' + str(dateAppo), ':key02': str(status) + '#DT#' + str(dateAppo), ':dateope': dateOpe, ':precheckin': 'PRECHECKIN'}
                response = table.update_item(
                    Key={
                        'PKID': 'APPO#' + appointmentId,
                        'SKID': 'APPO#' + appointmentId
                    },
                    UpdateExpression="SET #s = :status, GSI1SK = :key01, GSI2SK = :key02, GSI9SK = :key01, TIMECHEK = :dateope, GSI8PK = :precheckin, GSI8SK = :dateope",
                    ExpressionAttributeNames=e,
                    ExpressionAttributeValues=v,
                    ReturnValues="UPDATED_NEW"
                )
                appo = json_dynamodb.loads(response['Attributes'])

            if dateOpe[0:10] == dateAppo[0:10]:
                data = {
                    'BusinessId': busId,
                    'LocationId': locId,
                    'CustomerId': customerId,
                    'AppId': appointmentId,
                    'Time': dateOpe,
                    'Tipo': 'MOVE',
                    'To': 'PRECHECK'
                }
                lambdaInv.invoke(
                    FunctionName='PostMessages',
                    InvocationType='Event',
                    Payload=json.dumps(data)
                )
        else:
            items = []
            recordset = {
                "Update": {
                    "TableName": "TuCita247",
                    "Key": {
                        "PKID": {"S": 'APPO#' + appointmentId}, 
                        "SKID": {"S": 'APPO#' + appointmentId}, 
                    },
                    "UpdateExpression": "SET #s = :status, GSI1SK = :key01, GSI2SK = :key01, REASONID = :reason, GSI5PK = :pkey05, GSI5SK = :skey05, GSI6PK = :pkey06, GSI6SK = :skey06, GSI7PK = :pkey07, GSI7SK = :skey07, GSI9SK = :key01, TIMECANCEL = :dateope, STATUS_CANCEL = :statCancel REMOVE GSI8PK, GSI8SK",
                    "ExpressionAttributeValues": { 
                        ":status": {"N": str(status)}, 
                        ":key01": {"S": str(status) + '#DT#' + str(dateAppo)}, 
                        # ":key02": {"S": '#5'}, 
                        ":reason": {"S": reasonId},  
                        ":pkey05": {"S": businessId}, 
                        ":skey05": {"S": appoData}, 
                        ":pkey06": {"S": locationId}, 
                        ":skey06": {"S": appoData}, 
                        ":pkey07": {"S": providerId}, 
                        ":skey07": {"S": appoData},
                        ":dateope": {"S": dateOpe},
                        ":statCancel": {"N": str(3)}
                    },
                    "ExpressionAttributeNames": {'#s': 'STATUS'},
                    "ConditionExpression": "attribute_exists(PKID) AND attribute_exists(SKID)",
                    "ReturnValuesOnConditionCheckFailure": "ALL_OLD" 
                }
            }
            items.append(recordset)

            logger.info(items)
            response = dynamodbQuery.transact_write_items(
                TransactItems = items
            )
            appo = ''
            
            # if dateOpe[0:10] == dateAppo[0:10]:
            data = {
                'BusinessId': busId,
                'LocationId': locId,
                'AppId': appointmentId,
                'CustomerId': customerId,
                'Tipo': 'CANCEL'
            }
            lambdaInv.invoke(
                FunctionName='PostMessages',
                InvocationType='Event',
                Payload=json.dumps(data)
            )

        statusCode = 200
        body = json.dumps({'Message': 'Appointment updated successfully', 'Code': 200, 'Appo': appo})

        logger.info(response)
        #PASA A PRE-CHECK O CANCELACION IN Y ENVIA NOTIFICACION POR SNS A SMS y CORREO, ONESIGNAL (PUSH NOTIFICATION)
        textMess = ''
        if status == 2 or status == 5:
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
            preference = 0
            playerId = ''
            language = ''

            #OBTIENE EL LENGUAJE DEL NEGOCIO
            lanData = dynamodbQuery.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :pkid AND SKID = :skid',
                ExpressionAttributeValues={
                    ':pkid': {'S': 'BUS#' + busId},
                    ':skid': {'S': 'METADATA'}
                }
            )
            for lang in json_dynamodb.loads(lanData['Items']):
                language = lang['LANGUAGE'] if 'LANGUAGE' in lang else 'es'

            for row in json_dynamodb.loads(response['Items']):
                preference = int(row['PREFERENCES']) if 'PREFERENCES' in row else 0
                mobile = row['PKID'].replace('MOB#','')
                # email = row['EMAIL'] if 'EMAIL' in row else ''
                email = row['EMAIL_COMM'] if 'EMAIL_COMM' in row else row['EMAIL'] if 'EMAIL' in row else ''
                playerId = row['PLAYERID'] if 'PLAYERID' in row else ''
                if playerId != '':
                    language = str(row['LANGUAGE']).lower() if 'LANGUAGE' in row else language

            if status == 2:
                if language == "en":
                    textMess = 'We are ready to serve you. Please proceed to the entrance.'
                else:
                    textMess = 'Estamos listos para servirte. Por favor dirígete a la entrada.'
            else:
                hrAppo = datetime.datetime.strptime(dateAppo, '%Y-%m-%d-%H-%M').strftime('%I:%M %p')
                dayAppo = datetime.datetime.strptime(dateAppo[0:10], '%Y-%m-%d').strftime('%b %d %Y')
                if language == "en":
                    textMess = businessName + ' has canceled your booking for ' + dayAppo  + ', ' + hrAppo + '. Reason: ' + reasonId
                else:
                    textMess = businessName + ' ha cancelado su cita para ' + dayAppo + ', ' + hrAppo + '. Razón: ' + reasonId

            logger.info('Preference user ' + customerId + ' -- ' + str(preference))
            #CODIGO UNICO DEL TELEFONO PARA PUSH NOTIFICATION ONESIGNAL
            if playerId != '':
                header = {"Content-Type": "application/json; charset=utf-8"}
                payload = {"app_id": "476a02bb-38ed-43e2-bc7b-1ded4d42597f",
                        "include_player_ids": [playerId],
                        "contents": {"en": textMess }}
                req = requests.post("https://onesignal.com/api/v1/notifications", headers=header, data=json.dumps(payload))

            if int(preference) == 1 and mobile != '00000000000':
                #SMS
                to_number = mobile
                bodyStr = textMess
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
                BODY_TEXT = (textMess)
                            
                # The HTML body of the email.
                BODY_HTML = """<html>
                <head></head>
                <body>
                <h1>Tu Cita 24/7</h1>
                <p>""" + textMess + """</p>
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
        else:
            statusCode = 404
            body = json.dumps({'Message': str(e), 'Code': 400})
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