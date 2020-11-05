import sys
import logging
import requests
import json

import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr
from dynamodb_json import json_util as json_dynamodb

import uuid
import string
import math
import random

from decimal import *

import datetime
import dateutil.tz
from datetime import timezone

import os

REGION = 'us-east-1'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.client('dynamodb', region_name=REGION)
sms = boto3.client('sns')
ses = boto3.client('ses', region_name=REGION)
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

def findHours(time, hours):
    for item in hours:
        if item['Hour'] == time:
            return item, item['Start'], item['Available'], item['ServiceId']
    item = ''
    return item, 0, 0, ''

def findService(serviceId, servs):
    for item in servs:
        if item['ServiceId'] == serviceId:
            return int(item['CustomerPerTime'])
    item = 0
    return item

def findUsedHours(time, hours, serviceId, interval):
    count = 0
    for item in hours:
        if item['ServiceId'] == serviceId:
            if item['Hour'] < time and item['Hour']+interval >= time:
                count = count + int(item['People'])
            if item['Hour'] == time:
                count = count + int(item['People'])
    return count

def searchHours(time, hours):
    for item in hours:
        if item['Hour'] == time:
            return item
    item = ''
    return item

def findBookings(timeIni, timeFin, hours, service, intervalo):
    qty = 0
    for item in hours:
        if item['ServiceId'] == service:
            if item['Hour'] >= timeIni and item['Hour'] <= timeFin:
               qty = qty + item['People']
            else:
                if item['Hour']+intervalo >= timeIni and item['Hour']+intervalo <= timeFin:
                   qty = qty + item['People'] 
    return qty
    
def findHoursAppo(time, hours, service):
    for item in hours:
        if item['Hour'] == time and item['ServiceId'] == service:
            return item
    item = ''
    return item

def availableHour(hour, time, dayArr, loc, prov, serv, dtAppo):
    value = False
    getAvailability = dynamodb.query(
        TableName="TuCita247",
        ReturnConsumedCapacity='TOTAL',
        KeyConditionExpression='PKID = :usedData AND SKID = :time',
        ExpressionAttributeValues={
            ':usedData': {'S': 'LOC#'+loc+'#PRO#'+prov+'#DT#'+dtAppo.strftime("%Y-%m-%d")},
            ':time': {'S': 'HR#'+time}
        },
        ScanIndexForward=True
    )
    for res in json_dynamodb.loads(getAvailability['Items']):
        if int(res['CANCEL']) == 1:
            return False
        if int(res['AVAILABLE']) == 1 and (res['SERVICEID'] == '' or res['SERVICEID'] == serv):
            return True
        if int(res['AVAILABLE']) == 1 and res['SERVICEID'] != '' and res['SERVICEID'] != serv:
            return False
            
    if len(dayArr) >= 1:
        if hour >= int(dayArr[0]['I']) and hour <= int(dayArr[0]['F'])-1:
            return True
    if len(dayArr) == 2:
        if hour >= int(dayArr[1]['I']) and hour <= int(dayArr[1]['F'])-1:
            return True
    return value

def lambda_handler(event, context):
    try:
        statusCode = ''
        letters = string.ascii_uppercase + string.digits

        data = json.loads(event['body'])
        customerId = data['CustomerId']

        country_date = dateutil.tz.gettz('America/Puerto_Rico')
        today = datetime.datetime.now(tz=country_date)
        dateOpe = today.strftime("%Y-%m-%d-%H-%M-%S")
        
        dateIni = today.strftime("%Y-%m-%d")
        dateFin = today + datetime.timedelta(days=90)
        dateFin = dateFin.strftime("%Y-%m-%d")

        custBooking = dynamodb.query(
            TableName = "TuCita247",
            ReturnConsumedCapacity = 'TOTAL',
            KeyConditionExpression = 'PKID = :customerId',
            ExpressionAttributeValues = {
                ':customerId': {'S': 'RES#CUS#' + customerId}
            }
        )
        for custBooks in json_dynamodb.loads(custBooking['Items']): 
            statusPlan = 0
            bucket = 0
            numCustomer = 0
            numberAppos = 0

            res = str(custBooks['GSI1PK']).split('#')
            businessId = res[2]
            locationId = res[4]
            serviceId = custBooks['SERVICEID']
            providerId = res[6]
            appoResId = custBooks['SKID'].replace('APPO#','')
            name = custBooks['NAME']
            phone = custBooks['PHONE']
            door = custBooks['DOOR'] if 'DOOR' in custBooks else ''
            guest = custBooks['PEOPLE_QTY']
            disability = custBooks['DISABILITY'] if 'DISABILITY' in custBooks else ''
            onbehalf = int(custBooks['ON_BEHALF'])
            appoDate = datetime.datetime.strptime(custBooks['DATE_APPO'][0:10], '%Y-%m-%d')
            hourDate = custBooks['DATE_APPO'][-5:].replace('-',':')
            dateAppointment = appoDate.strftime("%Y-%m-%d") + '-' + hourDate.replace(':','-')

            #STATUS DEL PAQUETE ADQUIRIDO 1 ACTIVO Y TRAE TOTAL DE NUMERO DE CITAS
            statPlan = dynamodb.query(
                TableName = "TuCita247",
                ReturnConsumedCapacity = 'TOTAL',
                KeyConditionExpression = 'PKID = :businessId AND SKID = :key',
                ExpressionAttributeValues = {
                    ':businessId': {'S': 'BUS#' + businessId},
                    ':key': {'S': 'PLAN'}
                },
                Limit = 1
            )
            for stat in json_dynamodb.loads(statPlan['Items']):
                dueDate = datetime.datetime.strptime(stat['DUE_DATE'], '%Y-%m-%d').strftime("%Y-%m-%d-23-59")
                if dueDate > today.strftime("%Y-%m-%d-%H-%M") and stat['STATUS'] == 1:
                    statusPlan = stat['STATUS']
                    numberAppos = stat['AVAILABLE']

            if statusPlan == 0:
                statusCode = 404
                body = json.dumps({'Message': 'Disabled plan', 'Data': {}, 'Code': 400})
            else:
                typeAppo = 0
                code = ''
                if numberAppos > 0:
                    typeAppo = 1
                else:
                    #CITAS DISPONIBLES DE PAQUETES ADQUIRIDOS QUE VENCEN A FUTURO
                    avaiAppoPack = dynamodb.query(
                        TableName = "TuCita247",
                        ReturnConsumedCapacity = 'TOTAL',
                        KeyConditionExpression = 'PKID = :businessId AND SKID BETWEEN :key and :fin',
                        ExpressionAttributeValues = {
                            ':businessId': {'S': 'BUS#' + businessId},
                            ':key': {'S': 'PACK#' + dateIni},
                            ':fin': {'S': 'PACK#' + dateFin}
                        }
                    )
                    for availablePlan in json_dynamodb.loads(avaiAppoPack['Items']):
                        if availablePlan['AVAILABLE'] > 0:
                            numberAppos = availablePlan['AVAILABLE']
                            typeAppo = 2
                            code = availablePlan['SKID']
                            break

            #OBTIENE LOS SERVICIOS DEL NEGOCIO
            getServices = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :businessId AND SKID = :serviceId',
                ExpressionAttributeValues={
                    ':businessId': {'S': 'BUS#'+businessId},
                    ':serviceId': {'S': 'SER#'+serviceId}
                }
            )
            for serv in json_dynamodb.loads(getServices['Items']):
                bucket = int(serv['TIME_SERVICE'])
                numCustomer = int(serv['CUSTOMER_PER_TIME'])

            #PROCEDE A GUARDAR LA CITA
            recordset = {}
            items = []
            appoId = str(uuid.uuid4()).replace("-","")
            qrCode = ''.join(random.choice(letters) for i in range(6))
            recordset = {
                "Put": {
                    "TableName": "TuCita247",
                    "Item": {
                        "PKID": {"S": 'APPO#'+appoId}, 
                        "SKID": {"S": 'APPO#'+appoId}, 
                        "STATUS": {"N": "1"}, 
                        "NAME": {"S": name},
                        "DATE_APPO": {"S": dateAppointment},
                        "PHONE": {"S": phone},
                        "DOOR": {"S": door},
                        "ON_BEHALF": {"N": str(onbehalf)},
                        "PEOPLE_QTY": {"N": str(guest)},
                        "DISABILITY": {"N": str(disability) if disability != '' else None},
                        "SERVICEID": {"S": serviceId},
                        "QRCODE": {"S": qrCode},
                        "TYPE": {"N": "1"},
                        "GSI1PK": {"S": 'BUS#' + businessId + '#LOC#' + locationId + '#PRO#' + providerId}, 
                        "GSI1SK": {"S": '1#DT#' + dateAppointment}, 
                        "GSI2PK": {"S": 'CUS#' + customerId},
                        "GSI2SK": {"S": '1#DT#' + dateAppointment},
                        "GSI3PK": {"S": 'BUS#' + businessId + '#LOC#' + locationId + '#' + dateAppointment[0:10]}, 
                        "GSI3SK": {"S": 'QR#' + qrCode},
                        "GSI9PK": {"S": 'BUS#' + businessId + '#LOC#' + locationId}, 
                        "GSI9SK": {"S": '1#DT#' + dateAppointment}, 
                    },
                    "ConditionExpression": "attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                    "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
                    }
                }
            logger.info(cleanNullTerms(recordset))
            items.append(cleanNullTerms(recordset))

            recordset = {
                "Put": {
                    "TableName": "TuCita247",
                    "Item": {
                        "PKID": {"S": 'LOC#' + locationId + '#' + dateAppointment[0:10] + '#' + qrCode}, 
                        "SKID": {"S": 'LOC#' + locationId + '#' + dateAppointment[0:10] + '#' + qrCode}
                    },
                    "ConditionExpression": "attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                    "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
                    }
                }
            logger.info(recordset)
            items.append(recordset)

            if typeAppo == 1:
                recordset = {
                    "Update":{
                        "TableName": "TuCita247",
                        "Key": {
                            "PKID": {"S": 'BUS#' + businessId}, 
                            "SKID": {"S": 'PLAN'}, 
                        },
                        "UpdateExpression": "SET AVAILABLE = AVAILABLE - :increment",
                        "ExpressionAttributeValues": { 
                            ":increment": {"N": str(1)},
                            ":nocero": {"N": str(0)}
                        },
                        "ConditionExpression": "attribute_exists(PKID) AND attribute_exists(SKID) AND AVAILABLE > :nocero",
                        "ReturnValuesOnConditionCheckFailure": "ALL_OLD" 
                        }
                    }
            else:
                recordset = {
                    "Update":{
                        "TableName": "TuCita247",
                        "Key": {
                            "PKID": {"S": 'BUS#' + businessId}, 
                            "SKID": {"S": code}, 
                        },
                        "UpdateExpression": "SET AVAILABLE = AVAILABLE - :increment",
                        "ExpressionAttributeValues": { 
                            ":increment": {"N": str(1)},
                            ":nocero": {"N": str(0)}
                        },
                        "ConditionExpression": "attribute_exists(PKID) AND attribute_exists(SKID) AND AVAILABLE > :nocero",
                        "ReturnValuesOnConditionCheckFailure": "ALL_OLD" 
                        }
                    }
            
            logger.info(recordset)
            items.append(recordset)

            #VALIDA SI SE CREA REGISTRO O SE ACTUALIZA
            getSummarize = dynamodb.query(
                TableName = "TuCita247",
                ReturnConsumedCapacity = 'TOTAL',
                KeyConditionExpression = 'PKID = :key01 AND SKID = :key02',
                ExpressionAttributeValues = {
                    ':key01': {"S": 'LOC#' + locationId + '#PRO#' + providerId + '#DT#' + dateAppointment[0:10]},
                    ':key02': {"S": 'HR#'+hourDate.replace(':','-')}
                }
            )
            updateSum = 0
            putSum = 0
            for summ in json_dynamodb.loads(getSummarize['Items']):
                putSum = 1
                if summ['SERVICEID'] == '':
                    updateSum = 1

            if updateSum == 0 and putSum == 0:
                recordset = {
                    "Put": {
                        "TableName": "TuCita247",
                        "Item": {
                            "PKID": {"S": 'LOC#' + locationId + '#PRO#' + providerId + '#DT#' + dateAppointment[0:10]}, 
                            "SKID": {"S": 'HR#'+hourDate.replace(':','-')},
                            "TIME_SERVICE": {"N": str(bucket)},
                            "CUSTOMER_PER_TIME": {"N": str(int(numCustomer))},
                            "SERVICEID": {"S": str(serviceId)},
                            "AVAILABLE": {"N": str(int(numCustomer)-int(guest))},
                            "CANCEL": {"N": str(0)}
                        },
                        "ConditionExpression": "attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                        "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
                        }
                    }
            if updateSum == 0 and putSum == 1:
                #update
                recordset = {
                    "Update":{
                        "TableName": "TuCita247",
                        "Key": {
                            "PKID": {"S": 'LOC#' + locationId + '#PRO#' + providerId + '#DT#' + dateAppointment[0:10]}, 
                            "SKID": {"S": 'HR#'+hourDate.replace(':','-')}
                        },
                        "UpdateExpression": "SET AVAILABLE = AVAILABLE - :increment",
                        "ExpressionAttributeValues": { 
                            ":increment": {"N": str(guest)}, #str(1)},
                            ":nocero": {"N": str(0)},
                            ":serviceId": {"S": serviceId}
                        },
                        "ConditionExpression": "attribute_exists(PKID) AND attribute_exists(SKID) AND AVAILABLE >= :nocero AND SERVICEID = :serviceId",
                        "ReturnValuesOnConditionCheckFailure": "ALL_OLD" 
                    }
                }
            if updateSum == 1:
                #update
                recordset = {
                    "Update":{
                        "TableName": "TuCita247",
                        "Key": {
                            "PKID": {"S": 'LOC#' + locationId + '#PRO#' + providerId + '#DT#' + dateAppointment[0:10]}, 
                            "SKID": {"S": 'HR#'+hourDate.replace(':','-')}
                        },
                        "UpdateExpression": "SET AVAILABLE = :available, TIME_SERVICE = :timeSer, CUSTOMER_PER_TIME = :custPerTime, SERVICEID = :serviceId",
                        "ExpressionAttributeValues": { 
                            ":available": {"N": str(int(numCustomer)-int(guest))},
                            ":timeSer": {"N": str(bucket)},
                            ":custPerTime": {"N": str(numCustomer)},
                            ":serviceId": {"S": str(serviceId)}
                        },
                        "ConditionExpression": "attribute_exists(PKID) AND attribute_exists(SKID)",
                        "ReturnValuesOnConditionCheckFailure": "ALL_OLD" 
                    }
                }
            logger.info(recordset)
            items.append(recordset)

            recordset = {
                "Delete":{
                    "TableName": "TuCita247",
                    "Key": {
                        "PKID": {"S": "RES#CUS#"+customerId},
                        "SKID": {"S": "APPO#"+appoResId}
                    },
                    "ConditionExpression": "attribute_exists(PKID) AND attribute_exists(SKID)",
                    "ReturnValuesOnConditionCheckFailure": "NONE"
                }
            }
            logger.info(recordset)
            items.append(recordset)

            logger.info(items)
            response = dynamodb.transact_write_items(
                TransactItems = items
            )
            sTime = ''
            hTime = int(str(hourDate)[0:2])
            if hTime >= 12:
                if hTime == 12:
                    sTime = str(hTime) + ':00 PM'
                else:
                    hTime = hTime-12
                    sTime = str(hTime).rjust(2,'0') + ':00 PM'
            else:
                sTime = str(hTime).rjust(2,'0') + ':00 AM'

            appoInfo = {
                'Tipo': 'APPO',
                'BusinessId': businessId,
                'LocationId': locationId,
                'AppId': appoId,
                'ClientId': customerId,
                'ProviderId': providerId,
                'Name': name,
                'Phone': phone,
                'OnBehalf': str(onbehalf),
                'Guests': 0 if str(guest) == '' else int(guest),
                'Door': door,
                'Disability': 0 if disability == '' else int(disability),
                'DateFull': dateAppointment,
                'Type': '1',
                'DateAppo': sTime
            }
            if dateOpe[0:10] == dateAppointment[0:10]:
                lambdaInv.invoke(
                    FunctionName='PostMessages',
                    InvocationType='Event',
                    Payload=json.dumps(appoInfo)
                )

            if phone != '00000000000':
                # GET USER PREFERENCE NOTIFICATION
                preference = 0
                playerId = ''
                language = ''
                businessName = ''
                msgPush = ''
                msg = ''
                lat = ''
                lng = ''
                response = dynamodb.query(
                    TableName="TuCita247",
                    IndexName="TuCita247_Index",
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='GSI1PK = :key AND GSI1SK = :key',
                    ExpressionAttributeValues={
                        ':key': {'S': 'CUS#' + customerId}
                    }
                )
                for row in json_dynamodb.loads(response['Items']):
                    preference = int(row['PREFERENCES']) if 'PREFERENCES' in row else 0
                    email = row['EMAIL'] if 'EMAIL' in row else ''
                    playerId = row['PLAYERID'] if 'PLAYERID' in row else ''
                    language = str(row['LANGUAGE']).lower() if 'LANGUAGE' in row else 'en'
                
                buss = dynamodb.query(
                    TableName="TuCita247",
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='PKID = :key AND SKID = :meta',
                    ExpressionAttributeValues={
                        ':key': {'S': 'BUS#' + businessId},
                        ':meta': {'S': 'METADATA'}
                    }
                )
                for busName in json_dynamodb.loads(buss['Items']):
                    businessName = str(busName['NAME'])[0:27]+'...' if len(busName['NAME']) > 27 else busName['NAME']

                locs = dynamodb.query(
                    TableName="TuCita247",
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='PKID = :key AND SKID = :loc',
                    ExpressionAttributeValues={
                        ':key': {'S': 'BUS#' + businessId},
                        ':loc': {'S': 'LOC#' + locationId}
                    }
                )
                for locNum in json_dynamodb.loads(locs['Items']):
                    coordenates = json.loads(locNum['GEOLOCATION'])
                    lat = str(coordenates['LAT'])
                    lng = str(coordenates['LNG'])
                
                hrAppo = datetime.datetime.strptime(dateAppointment, '%Y-%m-%d-%H-%M').strftime('%I:%M %p')
                dayAppo = datetime.datetime.strptime(dateAppointment[0:10], '%Y-%m-%d').strftime('%b %d %Y')
                if language == 'en':
                    msg = 'Your booking at ' + businessName + ' was confirmed for ' + dayAppo + ', ' + hrAppo + ', located at https://www.google.com/maps/search/?api=1&query='+lat+','+lng+'. Thanks, Tu Cita 24/7.'
                    msgPush = 'Your booking at ' + businessName + ' was confirmed for ' + dayAppo + ', ' + hrAppo + '. Thanks, Tu Cita 24/7.'
                else:
                    msg = 'Su cita en ' + businessName + ' fue confirmada para ' + dayAppo + ', ' + hrAppo + ', ubicado en https://www.google.com/maps/search/?api=1&query='+lat+','+lng+'. Gracias, Tu Cita 24/7.'
                    msgPush = 'Su cita en ' + businessName + ' fue confirmada para ' + dayAppo + ', ' + hrAppo + '. Gracias, Tu Cita 24/7.'

                logger.info(msg)
                logger.info(msgPush)

                #CODIGO UNICO DEL TELEFONO PARA PUSH NOTIFICATION ONESIGNAL
                if playerId != '':
                    header = {"Content-Type": "application/json; charset=utf-8"}
                    payload = {"app_id": "476a02bb-38ed-43e2-bc7b-1ded4d42597f",
                            "include_player_ids": [playerId],
                            "contents": {"en": msgPush}}
                    req = requests.post("https://onesignal.com/api/v1/notifications", headers=header, data=json.dumps(payload))
                
                if int(preference) == 1:
                    #SMS
                    to_number = phone
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
                    
                if int(preference) == 2 and email != '':
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
            body = json.dumps({'Message': 'Appointment saved successfully', 'Code': 200})
        
        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Message': 'Error !!!', 'Code': 400})
    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message': 'Error on request try again ' + str(e)})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : "*"
        },
        'body' : body
    }
    return response