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
lambdaInv = boto3.client('lambda')

logger.info("SUCCESS: Connection to DynamoDB succeeded")

def findService(serviceId, services):
    for item in services:
        if item['ServiceId'] == serviceId:
            return int(item['TimeService'])
    item = 0
    return item

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

def workHours():
    return ['0000','0015','0030','0045','0100','0115','0130','0145','0200','0215','0230','0245','0300','0315','0330','0345','0400','0415','0430','0445','0500','0515','0530','0545','0600','0615','0630','0645','0700','0715','0730','0745','0800','0815','0830','0845','0900','0915','0930','0945','1000','1015','1030','1045','1100','1115','1130','1145','1200','1215','1230','1245','1300','1315','1330','1345','1400','1415','1430','1445','1500','1515','1530','1545','1600','1615','1630','1645','1700','1715','1730','1745','1800','1815','1830','1845','1900','1915','1930','1945','2000','2015','2030','2045','2100','2115','2130','2145','2200','2215','2230','2245','2300','2315','2330','2345']

def timeSerHours():
    return [0,15,30,45,100,115,130,145,200,215,230,245,300,315,330,345,400,415,430,445,500,515,530,545,600]

def timeSerHours15():
    return [0,15,30,85,100,115,130,185,200,215,230,285,300,315,330,385,400,415,430,485,500,515,530,585,600]

def timeSerHours30():
    return [0,15,70,85,100,115,170,185,200,215,270,285,300,315,370,385,400,415,470,485,500,515,570,585,600]

def timeSerHours45():
    return [0,55,70,85,100,155,170,185,200,255,270,285,300,355,370,385,400,455,470,485,500,555,570,585,600]

def lambda_handler(event, context):
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']

    statusCode = ''
        
    try:
        businessId = event['pathParameters']['businessId']
        locationId = event['pathParameters']['locationId']
        providerId = event['pathParameters']['providerId']
        dateAppo = event['pathParameters']['dateAppo']
        busLanguage = event['pathParameters']['busLanguage']

        country_date = dateutil.tz.gettz(findTimeZone(businessId, locationId))
        today = datetime.datetime.now(tz=country_date)
        dateOpe = today.strftime("%Y-%m-%d-%H-%M-%S")

        #GET BUSINESS NAME
        business = dynamodbQuery.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :key AND SKID = :skey',
            ExpressionAttributeValues={
                ':key': {'S': 'BUS#' + businessId},
                ':skey': {'S': 'METADATA'}
            }
        )
        businessName = ''
        for bus in json_dynamodb.loads(business['Items']):
            businessName = bus['NAME']

        #GET SERVICES
        services = dynamodbQuery.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :key01 AND begins_with(SKID , :key02)',
            ExpressionAttributeValues={
                ':key01': {'S': 'BUS#' + businessId},
                ':key02': {'S': 'SER#'},
                ':status': {'N': str(1)}
            },
            FilterExpression='#s = :status',
            ExpressionAttributeNames={'#s': 'STATUS'}
        )
        serv = []
        for item in json_dynamodb.loads(services['Items']):
            recordset = {
                'ServiceId': item['SKID'].replace('SER#',''),
                'TimeService': item['TIME_SERVICE']
            }
            serv.append(recordset)

        response = dynamodbQuery.query(
            TableName="TuCita247",
            IndexName="TuCita247_Index",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='GSI1PK = :gsi1pk AND GSI1SK between :gsi1sk_ini and :gsi1sk_fin',
            ExpressionAttributeValues={
                ':gsi1pk': {'S': 'BUS#' + businessId + '#LOC#' + locationId + '#PRO#' + providerId},
                ':gsi1sk_ini': {'S': '1#DT#' + dateAppo[0:10] + '-00-00'},
                ':gsi1sk_fin': {'S': '1#DT#' + dateAppo}
            }
        )
        businessIdData = 'BUS#'+businessId+'#5'
        locationIdData = 'BUS#'+businessId+'#LOC#'+locationId+'#5'
        providerIdData = 'BUS#'+businessId+'#LOC#'+locationId+'#PRO#'+providerId+'#5'
        cancel = int(dateAppo[-5:].replace('-',''))  #15-30, original dateAppo[-5:]
        # cancel = int(cancel[0:2])  #15
        items = []
        customerId=''
        qty = 0
        for row in json_dynamodb.loads(response['Items']):
            timeService = findService(row['SERVICEID'], serv)
            customerId = row['GSI2PK'].replace('CUS#','')
            qty = row['PEOPLE_QTY']
            if timeService != 0:
                newTime = 0
                min = int(str(row['GSI1SK'])[-2:])
                if min == 0:
                    times = timeSerHours()
                if min == 15:
                    times = timeSerHours15()
                if min == 30:
                    times = timeSerHours30()
                if min == 45:
                    times = timeSerHours45()
                newTime = times[timeSerHours().index(timeService)]
                citainiTemp = str(row['GSI1SK'])[-5:].replace('-','')  #15-30, reemplazar - x '', original str(row['GSI1SK'])[-5:]
                citaini = int(citainiTemp)  # 15, original int(citainiTemp[0:2])
                citafin = int(citaini)+int(newTime) #15 + tiempo servicio, deberia ser 15-30(reemplazar - x '') + tiempo servicio ej 1600
                if cancel >= citaini and cancel <= citafin:
                    appoData = str(row['DATE_APPO'])[0:10]+'#'+row['PKID']
                    appoDateMess = row['DATE_APPO']
                    appoId = row['PKID']
                    recordset = {
                        "Update": {
                            "TableName": "TuCita247",
                            "Key": {
                                "PKID": {"S": row['PKID']}, 
                                "SKID": {"S": row['PKID']}, 
                            },
                            "UpdateExpression": "SET #s = :status, MODIFIED_DATE = :mod_date, GSI1SK = :key01, GSI2SK = :key01, GSI5PK = :pkey05, GSI5SK = :skey05, GSI6PK = :pkey06, GSI6SK = :skey06, GSI7PK = :pkey07, GSI7SK = :skey07, GSI9SK = :key01, TIMECANCEL = :dateope, STATUS_CANCEL = :statCancel REMOVE GSI8PK, GSI8SK",
                            "ExpressionAttributeValues": { 
                                ":status": {"N": str(5)}, 
                                ":key01": {"S": str(5) + '#DT#' + row['DATE_APPO']}, 
                                # ":key02": {"S": '#5'}, 
                                ":pkey05": {"S": businessIdData}, 
                                ":skey05": {"S": appoData}, 
                                ":pkey06": {"S": locationIdData}, 
                                ":skey06": {"S": appoData}, 
                                ":pkey07": {"S": providerIdData}, 
                                ":skey07": {"S": appoData},
                                ":dateope": {"S": dateOpe},
                                ":statCancel": {"N": str(4)},
                                ":mod_date": {"S": str(dateOpe)}
                            },
                            "ExpressionAttributeNames": {'#s': 'STATUS'},
                            "ConditionExpression": "attribute_exists(PKID) AND attribute_exists(SKID)",
                            "ReturnValuesOnConditionCheckFailure": "ALL_OLD" 
                        }
                    }
                    items.append(recordset)

                    recordset = {
                        "Put": {
                            "TableName": "TuCita247",
                            "Item": {
                                "PKID": {"S": 'LOG#' + str(dateOpe)[0:10]},
                                "SKID": {"S": row['PKID'] + '#' + str(dateOpe)},
                                "DATE_APPO": {"S": str(dateOpe)},
                                "STATUS": {"N": str(5)}
                            },
                            "ConditionExpression": "attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                            "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
                            }
                        }
                    items.append(recordset)

                    #REMOVE FROM QEUE
                    data = {
                        'BusinessId': businessId,
                        'LocationId': locationId,
                        'AppId': appoId.replace('APPO#',''),
                        'CustomerId': customerId,
                        'ProviderId': providerId,
                        'ServiceId': row['SERVICEID'],
                        'DateAppo': str(row['DATE_APPO'])[0:10],
                        'Hour': str(row['DATE_APPO'])[-5:],
                        'Qty': qty,
                        'Tipo': 'CANCEL'
                    }
                    lambdaInv.invoke(
                        FunctionName='PostMessages',
                        InvocationType='Event',
                        Payload=json.dumps(data)
                    )

                    # GET USER PREFERENCE NOTIFICATION
                    response = dynamodbQuery.query(
                        TableName="TuCita247",
                        IndexName="TuCita247_Index",
                        ReturnConsumedCapacity='TOTAL',
                        KeyConditionExpression='GSI1PK = :key AND GSI1SK = :key',
                        ExpressionAttributeValues={
                            ':key': {'S': row['GSI2PK']}
                        }
                    )
                    preference = ''
                    playerId = ''
                    language = ''
                    msg = ''
                    #OBTIENE EL LENGUAJE DEL NEGOCIO
                    lanData = dynamodbQuery.query(
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

                    for row in json_dynamodb.loads(response['Items']):
                        preference = int(row['PREFERENCES']) if 'PREFERENCES' in row else 0
                        mobile = row['PKID'].replace('MOB#','')
                        # email = row['EMAIL'] if 'EMAIL' in row else ''
                        email = row['EMAIL_COMM'] if 'EMAIL_COMM' in row else row['EMAIL'] if 'EMAIL' in row else ''
                        playerId = row['PLAYERID'] if 'PLAYERID' in row else ''
                        if playerId != '':
                            language = str(row['LANGUAGE']).lower() if 'LANGUAGE' in row else language
                    
                    hrAppo = datetime.datetime.strptime(appoDateMess, '%Y-%m-%d-%H-%M').strftime('%I:%M %p')
                    dayAppo = datetime.datetime.strptime(appoDateMess[0:10], '%Y-%m-%d').strftime('%b %d %Y')
                    if language == "en":
                        msg = businessName + ' has canceled your booking for ' + dayAppo  + ', ' + hrAppo + '. Reason: SERVICE UNAVAILABLE'
                    else:
                        msg = businessName + ' ha cancelado su cita para ' + dayAppo + ', ' + hrAppo + '. Razón: SERVICIO NO DISPONIBLE'

                    #CODIGO UNICO DEL TELEFONO PARA PUSH NOTIFICATION ONESIGNAL
                    if playerId != '':
                        header = {"Content-Type": "application/json; charset=utf-8"}
                        payload = {"app_id": "476a02bb-38ed-43e2-bc7b-1ded4d42597f",
                                "include_player_ids": [playerId],
                                "contents": {"en": msg}}
                        req = requests.post("https://onesignal.com/api/v1/notifications", headers=header, data=json.dumps(payload))

                    if preference == 1 and mobile != '':
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
        
        response = dynamodbQuery.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :key01 AND SKID = :key02',
            ExpressionAttributeValues={
                ':key01': {'S': 'LOC#' + locationId + '#PRO#' + providerId + '#DT#' + dateAppo[0:10]},
                ':key02': {'S': 'HR#' + dateAppo[-5:]}
            }
        )
        entro = 0
        for row in json_dynamodb.loads(response['Items']):
            entro  = 1
            recordset = {
                "Update": {
                    "TableName": "TuCita247",
                    "Key": {
                        "PKID": {"S": 'LOC#'+locationId+'#PRO#'+providerId+'#DT#'+dateAppo[0:10]},
                        "SKID": {"S": 'HR#'+dateAppo[-5:]},
                    },
                    "UpdateExpression": "SET CANCEL = :cancel, AVAILABLE = :available",
                    "ExpressionAttributeValues": { 
                        ":cancel": {"N": str("1")},
                        ":available": {"N": str("0")}
                    },
                    "ConditionExpression": "attribute_exists(PKID) AND attribute_exists(SKID)", 
                    "ReturnValuesOnConditionCheckFailure": "ALL_OLD" 
                }
            }
        if entro == 0:
            recordset = {
                "Put": {
                    "TableName": "TuCita247",
                    "Item": {
                        "PKID": {"S": 'LOC#'+locationId+'#PRO#'+providerId+'#DT#'+dateAppo[0:10]},
                        "SKID": {"S": 'HR#'+dateAppo[-5:]},
                        "CANCEL": {"N": str(1)},
                        "AVAILABLE": {"N": str(0)}
                    },
                    "ConditionExpression": "attribute_not_exists(PKID) AND attribute_not_exists(SKID)", 
                    "ReturnValuesOnConditionCheckFailure": "ALL_OLD" 
                }
            }

        items.append(recordset)
        logger.info(items)
        response = dynamodbQuery.transact_write_items(
            TransactItems = items
        )

        data = {
            'BusinessId': businessId,
            'LocationId': locationId,
            'ProviderId': providerId,
            'AppId': '',
            'CustomerId': '',
            'DateAppo': dateAppo[0:10],
            'Hour': dateAppo[-5:],
            'Qty': 0,
            'Available': 0,
            'Tipo': 'AVAILABLE'
        }
        lambdaInv.invoke(
            FunctionName='PostMessages',
            InvocationType='Event',
            Payload=json.dumps(data)
        )

        logger.info(response)
        statusCode = 200
        body = json.dumps({'Message': 'Citas deleted successfully', 'Code': 200})

        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Message': 'Error on update appointment', 'Code': 500})
    except ClientError as e:
        logger.info(str(e))
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