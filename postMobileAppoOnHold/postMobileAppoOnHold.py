import sys
import logging
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

def repValues(data):
    for item in data:
        if str(float(item['I'])-int(float(item['I'])))[1:] == ".5":
            item['I'] = item['I'].replace('.5','.3')
        if str(float(item['F'])-int(float(item['F'])))[1:] == ".5":
            item['F'] = item['F'].replace('.5','.3')
    return data

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
    try:
        statusCode = ''
        letters = string.ascii_uppercase + string.digits

        data = json.loads(event['body'])
        businessId = data['BusinessId']
        locationId = data['LocationId']
        serviceId = data['ServiceId']
        providerId = data['ProviderId']
        name = data['Name']
        businessName = data['BusinessName']
        businessAddr = data['BusinessAddr']
        phone = data['Phone']
        door = data['Door']
        guest = data['Guests']
        disability = data['Disability']
        customerId = data['CustomerId']
        onbehalf = data['OnBehalf']
        comments = data['Comments'] if 'Comments' in data else ''
        appoDate = datetime.datetime.strptime(data['AppoDate'], '%m-%d-%Y')
        hourDate = data['AppoHour']
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
        servName = ''
        
        if appoDate.strftime("%Y-%m-%d") == today.strftime("%Y-%m-%d"):
            currHour = today.strftime("%H:%M")
            if int(currHour.replace(':','')) > int(hourDate.replace(':','')):
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
                bucket = 0
                for serv in json_dynamodb.loads(getServices['Items']):
                    recordset = {
                        'ServiceId': serv['SKID'].replace('SER#',''),
                        'CustomerPerTime': int(serv['CUSTOMER_PER_TIME']),
                        'TimeService': int(serv['TIME_SERVICE'])
                    }
                    services.append(recordset)
                    if serv['SKID'].replace('SER#','') == serviceId:
                        bucket = serv['TIME_SERVICE']
                        numCustomer = serv['CUSTOMER_PER_TIME']
                        servName = serv['NAME']
                
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
                    dateAppo = repValues(opeHours[dayName]) if dayName in opeHours else ''
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
                        timeBooking = int(hours['GSI1SK'].replace('1#DT#'+appoDate.strftime("%Y-%m-%d")+'-','').replace('-',''))
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
                            timeBooking = int(hoursCita['GSI1SK'].replace('2#DT#'+appoDate.strftime("%Y-%m-%d")+'-','').replace('-',''))
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
                        timeBooking = int(res['DATE_APPO'][-5:].replace('-',''))
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

                    #GET CLOSED AND OPEN HOURS
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
                                'Hour': int(cancel['SKID'].replace('HR#','').replace('-','')),
                                'ServiceId': '',
                                'People': 0,
                                'TimeService': 0,
                                'Cancel': 1
                            }
                            timeExists = findHours(int(cancel['SKID'].replace('HR#','').replace('-','')), hoursBooks)
                            if timeExists == '':
                                hoursBooks.append(recordset)
                            else:
                                hoursBooks.remove(timeExists)
                                hoursBooks.append(recordset)
                        if int(cancel['AVAILABLE']) == 1:
                            recordset = {
                                'Hour': int(cancel['SKID'].replace('HR#','').replace('-','')),
                                'ServiceId': '',
                                'People': 0,
                                'TimeService': 0,
                                'Cancel': 0
                            }
                            timeExists = searchHours(int(cancel['SKID'].replace('HR#','').replace('-','')), hoursBooks)
                            if timeExists == '':
                                hoursBooks.append(recordset)

                    for item in hoursBooks:
                        if item['Cancel'] == 1:
                            timeExists = searchHours(str(item['Hour']).rjust(4,'0')[0:2]+':'+str(item['Hour']).rjust(4,'0')[-2:], hoursData)
                            if timeExists != '':
                                hoursData.remove(timeExists)
                            recordset = {
                                'Hour': str(item['Hour']).rjust(4,'0')[0:2]+':'+str(item['Hour']).rjust(4,'0')[-2:],
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
                                'Hour': str(item['Hour']).rjust(4,'0')[0:2]+':'+str(item['Hour']).rjust(4,'0')[-2:],
                                'TimeService': item['TimeService'],
                                'Available': custPerTime-item['People'],
                                'ServiceId': item['ServiceId'],
                                'Cancel': 0,
                                'Start': 1
                            }
                            hoursData.append(recordset)

                    validAppo = 0
                    hrInterval = int(hourDate[-2:])
                    if hrInterval == 0:
                        times = timeSerHours()
                    if hrInterval == 15:
                        times = timeSerHours15()
                    if hrInterval == 30:
                        times = timeSerHours30()
                    if hrInterval == 45:
                        times = timeSerHours45()
                    # y = range(0, bucket)
                    countTime = 0
                    for z in times:
                        if timeSerHours()[countTime] == bucket:
                            break
                        countTime = countTime + 1
                        # if z > bucket:
                        #     break
                        tempTime = str(int(hourDate[0:2]+hourDate[-2:])+z).zfill(4)
                        locTime = tempTime[0:2]+':'+tempTime[-2:]
                        # locTime = str(int(hourDate[0:2])+z).zfill(2)+':'+str(hourDate[3:5])
                        hrArr, start, available, ser = findHours(locTime, hoursData)
                        if hrArr != '':
                            if (ser == serviceId and int(available)-int(guest) >= 0 and hrArr['Cancel'] == 0) or (ser == '' and hrArr['Cancel'] == 0):
                                validAppo = 1
                            else:
                                validAppo = -1
                                break
                        else:
                            for item in dateAppo:
                                ini = int(float(item['I'])*100)
                                fin = int(float(item['F'])*100)
                                if int(locTime[0:2]+locTime[-2:]) >= ini and int(locTime[0:2]+locTime[-2:]) < fin:
                                    if numCustomer > 0:
                                        validAppo = 1
                                        break
                                    else:
                                        validAppo = -1
                                        break
                #PROCEDE A GUARDAR LA CITA
                if validAppo == 1:
                    recordset = {}
                    items = []
                    appoId = str(uuid.uuid4()).replace("-","")
                    recordset = {
                        "Put": {
                            "TableName": "TuCita247",
                            "Item": {
                                "PKID": {"S": 'RES#CUS#'+customerId}, 
                                "SKID": {"S": 'APPO#'+appoId}, 
                                "NAME": {"S": name},
                                "PHONE": {"S": phone},
                                "BUSINESS_NAME": {"S": businessName},
                                "BUSINESS_ADDR": {"S": businessAddr},
                                "DATE_APPO": {"S": dateAppointment},
                                "DATE_OPE": {"S": dateOpeRes},
                                "DOOR": {"S": door},
                                "ON_BEHALF": {"N": str(onbehalf)},
                                "PEOPLE_QTY": {"N": str(guest)},
                                "COMMENTS": {"S": comments},
                                "SERVICEID": {"S": serviceId},
                                "SERVICE_NAME": {"S": servName},
                                "DISABILITY": {"N": str(disability) if disability != '' else None},
                                "GSI1PK": {"S": 'RES#BUS#'+businessId+'#LOC#'+locationId+'#PRO#'+providerId}, 
                                "GSI1SK": {"S": '1#DT#' + dateAppointment[0:10]},
                                "GSI2PK": {"S": 'RES#APPO'},
                                "GSI2SK": {"S": dateOpeRes + '#CUS#'+customerId}
                            },
                            "ConditionExpression": "attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                            "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
                            }
                        }
                    logger.info(cleanNullTerms(recordset))
                    items.append(cleanNullTerms(recordset))

                    logger.info(items)
                    response = dynamodb.transact_write_items(
                        TransactItems = items
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