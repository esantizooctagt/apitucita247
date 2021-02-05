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

def findServiceTime(serviceId, servs):
    for item in servs:
        if item['ServiceId'] == serviceId:
            return int(item['TimeService'])
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

def findTimeZone(businessId, locationId):
    timeZone='America/Puerto_Rico'
    locZone = dynamodb.query(
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
    try:
        statusCode = ''
        language = ''
        letters = string.ascii_uppercase + string.digits

        data = json.loads(event['body'])
        businessId = data['BusinessId']
        locationId = data['LocationId']
        serviceId = data['ServiceId']
        providerId = data['ProviderId']
        name = data['Name']
        phone = data['Phone']
        door = data['Door']
        guest = data['Guests']
        disability = data['Disability']
        customerId = data['CustomerId']
        onbehalf = data['OnBehalf']
        appoDate = datetime.datetime.strptime(data['AppoDate'], '%m-%d-%Y')
        hourDate = data['AppoHour']
        businessName = data['BusinessName']
        language = data['Language']
        dateAppointment = appoDate.strftime("%Y-%m-%d") + '-' + data['AppoHour'].replace(':','-')

        country_date = dateutil.tz.gettz(findTimeZone(businessId, locationId))
        today = datetime.datetime.now(tz=country_date)
        dateOpe = today.strftime("%Y-%m-%d-%H-%M-%S")

        todayOpe = datetime.datetime.now()
        dateOpeRes = todayOpe.strftime("%Y-%m-%d-%H-%M-%S")

        dayName = appoDate.strftime("%A")[0:3].upper()
        dateIni= today.strftime("%Y-%m-%d")
        dateFin = today + datetime.timedelta(days=90)
        dateFin = dateFin.strftime("%Y-%m-%d")

        statusPlan = 0
        numberAppos = 0
        result = {}
        hoursData = []
        hours = []
        services = []
        currHour = ''
        statusCode = ''
        
        if appoDate.strftime("%Y-%m-%d") == today.strftime("%Y-%m-%d"):
            currHour = today.strftime("%H:%M")
            if int(currHour.replace(':','')[0:2]) > int(hourDate.replace(':','')[0:2]):
                statusCode = 404
                body = json.dumps({'Message': 'Hour not available', 'Data': result, 'Code': 400})
                response = {
                    'statusCode' : statusCode,
                    'headers' : {
                        "content-type" : "application/json",
                        "access-control-allow-origin" : "*"
                    },
                    'body' : body
                }
                return response

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
            body = json.dumps({'Message': 'Disabled plan', 'Data': result, 'Code': 400})
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
                    KeyConditionExpression = 'PKID = :businessId',
                    FilterExpression = 'AVAILABLE > :cero',
                    ExpressionAttributeValues = {
                        ':businessId': {'S': 'BUS#' + businessId},
                        ':cero': {'N': str(0)}
                    }
                )
                for availablePlan in json_dynamodb.loads(avaiAppoPack['Items']):
                    numberAppos = availablePlan['AVAILABLE']
                    typeAppo = 2
                    code = availablePlan['SKID']
                    break

            #SIN DISPONIBILIDAD DE CITAS
            if numberAppos == 0:
                statusCode = 404
                body = json.dumps({'Message': 'No appointments available', 'Data': result, 'Code': 400})
            else:
                #OBTIENE EL LENGUAJE DEL NEGOCIO
                lanData = dynamodb.query(
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

                #ENTRA SI HAY CITAS DISPONIBLES YA SEA DE PLAN O PAQUETE VIGENTE
                #OBTIENE LOS SERVICIOS DEL NEGOCIO
                getServices = dynamodb.query(
                    TableName="TuCita247",
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='PKID = :businessId AND begins_with(SKID , :services)',
                    ExpressionAttributeValues={
                        ':businessId': {'S': 'BUS#'+businessId},
                        ':services': {'S': 'SER#'}
                    }
                )
                recordset = {}
                for serv in json_dynamodb.loads(getServices['Items']):
                    recordset = {
                        'ServiceId': serv['SKID'].replace('SER#',''),
                        'CustomerPerTime': int(serv['CUSTOMER_PER_TIME']),
                        'TimeService': int(serv['TIME_SERVICE'])
                    }
                    services.append(recordset)
                
                #GET CURRENT SERVICE
                service = dynamodb.query(
                    TableName="TuCita247",
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='PKID = :businessId AND SKID = :serviceId',
                    ExpressionAttributeValues={
                        ':businessId': {'S': 'BUS#'+businessId},
                        ':serviceId': {'S': 'SER#'+serviceId}
                    }
                )
                bufferTime = 0
                for serv in json_dynamodb.loads(service['Items']):
                    bucket = serv['TIME_SERVICE']
                    numCustomer = serv['CUSTOMER_PER_TIME']
                    bufferTime = serv['BUFFER_TIME']

                #GET OPERATION HOURS FROM SPECIFIC LOCATION
                getCurrDate = dynamodb.query(
                    TableName = "TuCita247",
                    ReturnConsumedCapacity = 'TOTAL',
                    KeyConditionExpression = 'PKID = :businessId and SKID = :key',
                    ExpressionAttributeValues = {
                        ':businessId': {'S': 'BUS#'+businessId+'#LOC#'+locationId},
                        ':key': {'S': 'PRO#'+providerId}
                    },
                    Limit = 1
                )
                for currDate in json_dynamodb.loads(getCurrDate['Items']):
                    periods = []
                    dayOffValid = True

                    opeHours = json.loads(currDate['OPERATIONHOURS'])
                    daysOff = currDate['DAYS_OFF'] if 'DAYS_OFF' in currDate else []
                    dateAppo = opeHours[dayName] if dayName in opeHours else ''
                    if daysOff != []:
                        dayOffValid = appoDate.strftime("%Y-%m-%d") not in daysOff
                        if dayOffValid == False:
                            statusCode = 500
                            body = json.dumps({'Message': 'Day Off', 'Data': [], 'Code': 400})
                            break
                    
                    #BOOKINGS
                    hoursBooks = []
                    hoursData = []
                    getAppos = dynamodb.query(
                        TableName="TuCita247",
                        IndexName="TuCita247_Index",
                        ReturnConsumedCapacity='TOTAL',
                        KeyConditionExpression='GSI1PK = :key01 and begins_with(GSI1SK, :key02)',
                        ExpressionAttributeValues={
                            ':key01': {'S': 'BUS#'+businessId+'#LOC#'+locationId+'#PRO#'+providerId},
                            ':key02': {'S': '1#DT#'+appoDate.strftime("%Y-%m-%d")}
                        }
                    )
                    for hours in json_dynamodb.loads(getAppos['Items']):
                        timeBooking = int(hours['GSI1SK'].replace('1#DT#'+appoDate.strftime("%Y-%m-%d")+'-','')[0:2])
                        cxTime = findServiceTime(hours['SERVICEID'], services)
                        recordset = {
                            'Hour': timeBooking,
                            'ServiceId': hours['SERVICEID'],
                            'People': hours['PEOPLE_QTY'],
                            'TimeService': cxTime,
                            'Cancel': 0
                        }
                        resAppo = findHoursAppo(timeBooking, hoursBooks, hours['SERVICEID'])
                        if resAppo == '':
                            hoursBooks.append(recordset)
                        else:
                            hoursBooks.remove(resAppo)
                            recordset['People'] = int(hours['PEOPLE_QTY'])+int(resAppo['People']) 
                            hoursBooks.append(recordset)

                    #OBTIENE LAS CITAS DEL DIA EN PROCESO
                    if dateOpe[0:10] == appoDate.strftime("%Y-%m-%d"):
                        getAppos02 = dynamodb.query(
                            TableName="TuCita247",
                            IndexName="TuCita247_Index",
                            ReturnConsumedCapacity='TOTAL',
                            KeyConditionExpression='GSI1PK = :key01 and begins_with(GSI1SK, :key02)',
                            ExpressionAttributeValues={
                                ':key01': {'S': 'BUS#'+businessId+'#LOC#'+locationId+'#PRO#'+providerId},
                                ':key02': {'S': '2#DT#'+appoDate.strftime("%Y-%m-%d")}
                            }
                        )
                        for hoursCita in json_dynamodb.loads(getAppos02['Items']):
                            timeBooking = int(hoursCita['GSI1SK'].replace('2#DT#'+appoDate.strftime("%Y-%m-%d")+'-','')[0:2])
                            cxTime = findServiceTime(hoursCita['SERVICEID'], services)
                            recordset = {
                                'Hour': timeBooking,
                                'ServiceId': hoursCita['SERVICEID'],
                                'People': hoursCita['PEOPLE_QTY'],
                                'TimeService': cxTime,
                                'Cancel': 0
                            }
                            resAppo = findHoursAppo(timeBooking, hoursBooks, hoursCita['SERVICEID'])
                            if resAppo == '':
                                hoursBooks.append(recordset)
                            else:
                                hoursBooks.remove(resAppo)
                                recordset['People'] = int(hoursCita['PEOPLE_QTY'])+int(resAppo['People']) 
                                hoursBooks.append(recordset)

                    #OBTIENE LAS CITAS EN RESERVA DE UN DIA
                    getReservas = dynamodb.query(
                        TableName="TuCita247",
                        IndexName="TuCita247_Index",
                        ReturnConsumedCapacity='TOTAL',
                        KeyConditionExpression='GSI1PK = :key01 and begins_with(GSI1SK, :key02)',
                        ExpressionAttributeValues={
                            ':key01': {'S': 'RES#BUS#'+businessId+'#LOC#'+locationId+'#PRO#'+providerId},
                            ':key02': {'S': '1#DT#'+appoDate.strftime("%Y-%m-%d")}
                        }
                    )
                    for res in json_dynamodb.loads(getReservas['Items']):
                        timeBooking = int(str(res['DATE_APPO'][-5:])[0:2])
                        cxTime = findServiceTime(res['SERVICEID'], services)
                        recordset = {
                            'Hour': timeBooking,
                            'ServiceId': res['SERVICEID'],
                            'People': res['PEOPLE_QTY'],
                            'TimeService': cxTime,
                            'Cancel': 0
                        }
                        resAppo = findHoursAppo(timeBooking, hoursBooks, res['SERVICEID'])
                        if resAppo == '':
                            hoursBooks.append(recordset)
                        else:
                            hoursBooks.remove(resAppo)
                            recordset['People'] = int(res['PEOPLE_QTY'])+int(resAppo['People']) 
                            hoursBooks.append(recordset)

                    #GET CLOSED OR OPEN HOURS
                    getCurrHours = dynamodb.query(
                        TableName="TuCita247",
                        ReturnConsumedCapacity='TOTAL',
                        KeyConditionExpression='PKID = :key',
                        ExpressionAttributeValues = {
                            ':key': {'S': 'LOC#'+locationId+'#PRO#'+providerId+'#DT#'+appoDate.strftime("%Y-%m-%d")}
                        },
                        ScanIndexForward=True
                    )
                    for cancel in json_dynamodb.loads(getCurrHours['Items']):
                        if int(cancel['CANCEL']) == 1:
                            recordset = {
                                'Hour': int(cancel['SKID'].replace('HR#','')[0:2]),
                                'ServiceId': '',
                                'People': 0,
                                'TimeService': 0,
                                'Cancel': 1
                            }
                            timeExists = searchHours(int(cancel['SKID'].replace('HR#','')[0:2]), hoursBooks)
                            if timeExists == '':
                                hoursBooks.append(recordset)
                            else:
                                hoursBooks.remove(timeExists)
                                hoursBooks.append(recordset)
                        if int(cancel['AVAILABLE']) == 1:
                            recordset = {
                                'Hour': int(cancel['SKID'].replace('HR#','')[0:2]),
                                'ServiceId': '',
                                'People': 0,
                                'TimeService': 0,
                                'Cancel': 0
                            }
                            timeExists = searchHours(int(cancel['SKID'].replace('HR#','')[0:2]), hoursBooks)
                            if timeExists == '':
                                hoursBooks.append(recordset)

                    for item in hoursBooks:
                        if item['Cancel'] == 1:
                            timeExists = searchHours(str(item['Hour']).rjust(2,'0')+':00', hoursData)
                            if timeExists != '':
                                hoursData.remove(timeExists)
                            recordset = {
                                'Hour': str(item['Hour']).rjust(2,'0')+':00',
                                'TimeService': 1,
                                'ServiceId': '',
                                'Available': 0,
                                'Cancel': 1,
                                'Start': 1
                            }
                            hoursData.append(recordset)
                        else:
                            custPerTime = 0
                            if 'ServiceId' in item:
                                custPerTime = findService(item['ServiceId'], services)
                            
                            recordset = {
                                'Hour': str(item['Hour']).rjust(2,'0')+':00',
                                'TimeService': item['TimeService'],
                                'Available': custPerTime-item['People'],
                                'ServiceId': item['ServiceId'],
                                'Cancel': 0,
                                'Start': 1
                            }
                            hoursData.append(recordset)

                    validAppo = 0
                    y = range(0, bucket)
                    for z in y:
                        locTime = str(int(hourDate[0:2])+z).zfill(2)+':'+str(hourDate[3:5])
                        hrArr, start, available, ser = findHours(locTime, hoursData)
                        if hrArr != '':
                            if (ser == serviceId and int(available)-int(guest) >= 0 and hrArr['Cancel'] == 0) or (ser == '' and hrArr['Cancel'] == 0):
                                validAppo = 1
                            else:
                                validAppo = -1
                                break
                        else:
                            for item in dateAppo:
                                ini = Decimal(item['I'])
                                fin = Decimal(item['F'])
                                if int(locTime[0:2]) >= ini and int(locTime[0:2]) < fin:
                                    if numCustomer > 0:
                                        validAppo = 1
                                        break
                                    else:
                                        validAppo = -1
                                        break
                #PROCEDE A GUARDAR LA CITA
                if validAppo == 1:
                    servs = dynamodb.query(
                        TableName="TuCita247",
                        ReturnConsumedCapacity='TOTAL',
                        KeyConditionExpression='PKID = :businessId AND begins_with(SKID , :serv)',
                        ExpressionAttributeValues={
                            ':businessId': {'S': 'BUS#'+businessId},
                            ':serv': {'S': 'SER#' }
                        }
                    )
                    count = 0
                    servName = ''
                    for serv in json_dynamodb.loads(servs['Items']):
                        count = count + 1
                        if serv['SKID'].replace('SER#','') == serviceId:
                            servName = serv['NAME']
                    if count == 1:
                        servName = ''
                    
                    provs =  dynamodb.query(
                        TableName="TuCita247",
                        ReturnConsumedCapacity='TOTAL',
                        KeyConditionExpression='PKID = :key AND begins_with(SKID , :provs)',
                        ExpressionAttributeValues={
                            ':key': {'S': 'BUS#'+businessId+'#LOC#'+locationId},
                            ':provs': {'S': 'PRO#' }
                        }
                    )
                    countp = 0
                    provName = ''
                    for prov in json_dynamodb.loads(provs['Items']):
                        countp = countp + 1
                        if prov['SKID'].replace('PRO#','') == providerId:
                            provName = prov['NAME']
                    if countp == 1:
                        provName = ''

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
                                "DATE_TRANS": {"S": str(dateOpe)},
                                "GSI1PK": {"S": 'BUS#' + businessId + '#LOC#' + locationId + '#PRO#' + providerId}, 
                                "GSI1SK": {"S": '1#DT#' + dateAppointment}, 
                                "GSI2PK": {"S": 'CUS#' + customerId},
                                "GSI2SK": {"S": '1#DT#' + dateAppointment},
                                "GSI3PK": {"S": 'BUS#' + businessId + '#LOC#' + locationId + '#' + dateAppointment[0:10]}, 
                                "GSI3SK": {"S": 'QR#' + qrCode},
                                "GSI9PK": {"S": 'BUS#' + businessId + '#LOC#' + locationId}, 
                                "GSI9SK": {"S": '1#DT#' + dateAppointment},
                                "GSI10PK": {"S": 'CUS#' + customerId},
                                "GSI10SK": {"S": dateAppointment}
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
                        'BufferTime': bufferTime,
                        'Name': name,
                        'Provider': provName,
                        'Service': servName,
                        'Phone': phone,
                        'OnBehalf': str(onbehalf),
                        'Guests': 0 if str(guest) == '' else int(guest),
                        'Door': door,
                        'Disability': 0 if disability == '' else int(disability),
                        'DateFull': dateAppointment,
                        'Type': '1',
                        'DateAppo': sTime,
                        'QrCode': qrCode,
                        'OpenMess': 0,
                        'OpenCanc': 0,
                        'OpenItem': 0,
                        'DateTrans': str(dateOpe),
                        'Status': 1,
                        'UnRead': ''
                    }

                    validAppo = (today + datetime.timedelta(hours=6)).strftime("%Y-%m-%d-%H-%M")
                    if dateAppointment <= validAppo: 
                        lambdaInv.invoke(
                            FunctionName='PostMessages',
                            InvocationType='Event',
                            Payload=json.dumps(appoInfo)
                        )

                    if phone != '00000000000':
                        # GET USER PREFERENCE NOTIFICATION
                        preference = 0
                        playerId = ''
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
                            # email = row['EMAIL'] if 'EMAIL' in row else ''
                            email = row['EMAIL_COMM'] if 'EMAIL_COMM' in row else row['EMAIL'] if 'EMAIL' in row else ''
                            playerId = row['PLAYERID'] if 'PLAYERID' in row else ''
                            if playerId != '':
                                language = str(row['LANGUAGE']).lower() if 'LANGUAGE' in row else language
                        
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
                        strQrCode = ''
                        if language == 'en':
                            strQrCode = 'Code: '+qrCode
                            msg = 'Your booking at ' + businessName + ' was confirmed for ' + dayAppo + ', ' + hrAppo + ', located at https://www.google.com/maps/search/?api=1&query='+lat+','+lng+'. '+strQrCode+'. Tu Cita 24/7.'
                            msgPush = 'Your booking at ' + businessName + ' was confirmed for ' + dayAppo + ', ' + hrAppo + '. '+strQrCode+'. Tu Cita 24/7.'
                        else:
                            strQrCode = 'Código: '+qrCode
                            msg = 'Su cita en ' + businessName + ' fue confirmada para ' + dayAppo + ', ' + hrAppo + ', ubicado en https://www.google.com/maps/search/?api=1&query='+lat+','+lng+'. '+strQrCode+'. Tu Cita 24/7.'
                            msgPush = 'Su cita en ' + businessName + ' fue confirmada para ' + dayAppo + ', ' + hrAppo + '. '+strQrCode+'. Tu Cita 24/7.'

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
                    body = json.dumps({'Message': 'Appointment saved successfully', 'Code': 200, 'Appointment': appoInfo})
                else:
                    statusCode = 500
                    body = json.dumps({'Message': 'Unavailable date and time', 'Code': 400})
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