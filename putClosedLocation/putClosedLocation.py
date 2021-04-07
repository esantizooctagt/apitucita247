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

def findTimeZone(businessId, locationId):
    timeZone='America/Puerto_Rico'
    locZone = dynamoQr.query(
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
        locationId = event['pathParameters']['id']
        businessId = event['pathParameters']['businessId']
        closed = int(event['pathParameters']['closed'])
        busLanguage = event['pathParameters']['busLanguage']
        
        country_date = dateutil.tz.gettz(findTimeZone(businessId, locationId))
        today = datetime.datetime.now(tz=country_date)
        dateOpe = today.strftime("%Y-%m-%d-%H-%M-%S")

        dateAppoIni = (today + datetime.timedelta(hours=-4)).strftime("%Y-%m-%d-%H-00")
        dateAppoFin = (today + datetime.timedelta(hours=-1)).strftime("%Y-%m-%d-%H-59")

        dateNow = today.strftime("%Y-%m-%d-%H")
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
        Address = ''
        TimeZone = ''
        for bus in json_dynamodb.loads(busName['Items']):
            businessName = bus['NAME']
            Address = bus['ADDRESS']
            TimeZone = bus['TIME_ZONE']

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
                appos = dynamoQr.query(
                    TableName="TuCita247",
                    IndexName="TuCita247_Index",
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='GSI1PK = :gsi1pk AND GSI1SK BETWEEN :gsi1sk_ini AND :gsi1sk_fin',
                    ExpressionAttributeValues={
                        ':gsi1pk': {'S': 'BUS#' + businessId + '#LOC#' + locationId + '#PRO#' + provider['SKID'].replace('PRO#','')},
                        ':gsi1sk_ini': {'S': '1#DT#' + dateAppoIni},
                        ':gsi1sk_fin': {'S': '1#DT#' + dateAppoFin}
                    }
                )
                table = dynamodb.Table('TuCita247')
                for appo in json_dynamodb.loads(appos['Items']):
                    updAppo = table.update_item(
                        Key={
                            'PKID': appo['PKID'],
                            'SKID': appo['PKID']
                        },
                        UpdateExpression="SET GSI1SK = :dtkey, GSI2SK = :dtkey, GSI9SK = :dtkey, #s = :stat",
                        ExpressionAttributeNames={'#s': 'STATUS'},
                        ExpressionAttributeValues={
                            ':dtkey': str('6#DT#'+appo['DATE_APPO']),
                            ':stat': 6
                        }
                    )
                    logger.info(updAppo)

                    putLog = table.put_item(
                        Item={
                            'PKID': 'LOG#'+str(dateOpe)[0:10],
                            'SKID': appo['PKID'] + '#' + str(dateOpe),
                            'DATE_APPO': str(dateOpe),
                            'STATUS': int(6)
                        },
                        ReturnValues='NONE'
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
                        'TimeZone': TimeZone,
                        'Tipo': 'MOVE',
                        'To': 'EXPIRED'
                    }
                    lambdaInv.invoke(
                        FunctionName='PostMessages',
                        InvocationType='Event',
                        Payload=json.dumps(data)
                    )
                    
                for i in range(1, 3):
                    appos = dynamoQr.query(
                        TableName="TuCita247",
                        IndexName="TuCita247_Index",
                        ReturnConsumedCapacity='TOTAL',
                        KeyConditionExpression='GSI1PK = :gsi1pk AND GSI1SK BETWEEN :gsi1sk_ini AND :gsi1sk_fin',
                        ExpressionAttributeValues={
                            ':gsi1pk': {'S': 'BUS#' + businessId + '#LOC#' + locationId + '#PRO#' + provider['SKID'].replace('PRO#','')},
                            ':gsi1sk_ini': {'S': str(i)+'#DT#' + dateNow+'-00'},
                            ':gsi1sk_fin': {'S': str(i)+'#DT#' + dateFin+'-23-59'}
                        }
                    )
                    #DISABLED HOURS FROM NOW TO 23
                    for hr in range(int(hourNow), 24):
                        res = dynamoQr.query(
                            TableName="TuCita247",
                            ReturnConsumedCapacity='TOTAL',
                            KeyConditionExpression='PKID = :key AND SKID = :skey',
                            ExpressionAttributeValues={
                                ':key': {'S': 'LOC#' + locationId + '#PRO#' + provider['SKID'].replace('PRO#','') + '#DT#' + dateNow[0:10]},
                                ':skey': {'S': 'HR#'+str(hr).zfill(2)+'-00'}
                            }
                        )
                        ingresa = 0
                        for appoRes in json_dynamodb.loads(res['Items']):
                            ingresa = 1 
                            updHr = table.update_item(
                                Key={
                                    'PKID': 'LOC#'+locationId+'#'+provider['SKID']+'#DT#'+dateNow[0:10],
                                    'SKID': 'HR#'+str(hr).zfill(2)+'-00'
                                },
                                UpdateExpression="SET AVAILABLE = :available, CANCEL = :cancel",
                                ExpressionAttributeValues={
                                    ':available': 0,
                                    ':cancel': 1
                                }
                            )
                        if ingresa == 0:
                            updHr = table.put_item(
                                Item={
                                    'PKID': 'LOC#'+locationId+'#'+provider['SKID']+'#DT#'+dateNow[0:10],
                                    'SKID': 'HR#'+str(hr).zfill(2)+'-00',
                                    'AVAILABLE': 0,
                                    'CANCEL': 1
                                },
                                ReturnValues='NONE'
                            )

                    table = dynamodb.Table('TuCita247')
                    for appo in json_dynamodb.loads(appos['Items']):
                        updAppo = table.update_item(
                            Key={
                                'PKID': appo['PKID'],
                                'SKID': appo['PKID']
                            },
                            UpdateExpression="SET GSI1SK = :dtkey, GSI2SK = :dtkey, GSI5PK = :buskey, GSI5SK = :skey5, GSI6PK = :pkey6, GSI6SK = :skey5, GSI7PK = :pkey7, GSI7SK = :skey5, GSI9SK = :dtkey, #s = :stat, TIMECANCEL = :timeNow, STATUS_CANCEL = :statCancel",
                            ExpressionAttributeNames={'#s': 'STATUS'},
                            ExpressionAttributeValues={
                                ':dtkey': str('5#DT#'+appo['DATE_APPO']),
                                # ':keyCancel': str('#5'),
                                ':buskey': 'BUS#'+businessId+'#5',
                                ':skey5': appo['DATE_APPO'][0:10]+'#'+appo['PKID'],
                                ':pkey6': 'BUS#'+businessId+'#LOC#'+locationId+'#5',
                                ':pkey7': 'BUS#'+businessId+'#LOC#'+locationId+'#'+provider['SKID']+'#5',
                                ':stat': 5,
                                ':statCancel': 5,
                                ':timeNow': dateOpe
                            }
                            # ReturnValues="UPDATED_NEW"
                        )
                        logger.info(updAppo)

                        putLog = table.put_item(
                            Item={
                                'PKID': 'LOG#'+str(dateOpe)[0:10],
                                'SKID': appo['PKID'] + '#' + str(dateOpe),
                                'DATE_APPO': str(dateOpe),
                                'STATUS': int(5)
                            },
                            ReturnValues='NONE'
                        )
                        
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
                        #OBTIENE EL LENGUAJE DEL NEGOCIO
                        lanData = dynamoQr.query(
                            TableName="TuCita247",
                            ReturnConsumedCapacity='TOTAL',
                            KeyConditionExpression='PKID = :pkid AND SKID = :skid',
                            ExpressionAttributeValues={
                                ':pkid': {'S': 'BUS#' + businessId},
                                ':skid': {'S': 'METADATA'}
                            }
                        )
                        for lang in json_dynamodb.loads(lanData['Items']):
                            language = lang['LANGUAGE'] if 'LANGUAGE' in lang else 'es'

                        for row in json_dynamodb.loads(customer['Items']):
                            preference = int(row['PREFERENCES']) if 'PREFERENCES' in row else 0
                            mobile = row['PKID'].replace('MOB#','')
                            # email = row['EMAIL'] if 'EMAIL' in row else ''
                            email = row['EMAIL_COMM'] if 'EMAIL_COMM' in row else row['EMAIL'] if 'EMAIL' in row else ''
                            playerId = row['PLAYERID'] if 'PLAYERID' in row else ''
                            if playerId != '':
                                language = str(row['LANGUAGE']).lower() if 'LANGUAGE' in row else language
                        
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