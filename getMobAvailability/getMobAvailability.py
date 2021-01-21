import sys
import logging
import json

import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr
from dynamodb_json import json_util as json_dynamodb

import string
import math

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

def getKey(obj):
  return obj['Time24']
  
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

def findTime(time, hours):
    for item in hours:
        if item['Hour'] == time:
            return item
    item = ''
    return item
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
        if item['ServiceId'] == serviceId or item['ServiceId'] == '':
            if item['Hour'] < time and item['Hour']+interval >= time:
                count = count + int(item['People'])
            if item['Hour'] == time:
                count = count + int(item['People'])
    return count

def findAvailability(time, hours, serviceId, interval, services):
    count = 0
    for item in hours:
        if (item['ServiceId'] == serviceId or item['ServiceId'] == '') and item['Cancel'] == 0:
            if item['Time24'] < time and item['Time24']+interval >= time:
                count = count + int(item['Available'])
                break
            if item['Time24'] == time:
                count = count + int(item['Available'])
                break
        if (item['ServiceId'] == serviceId or item['ServiceId'] == '') and item['Cancel'] == 1:
            count = -1
            break
        if item['ServiceId'] != serviceId and item['ServiceId'] != '':
            newInterval = findServiceTime(item['ServiceId'], services)
            if item['Time24'] < time and item['Time24']+newInterval-1 >= time:
                count = -1
                break
            if item['Time24'] == time:
                count = -1
                break
    return count

def searchTime(time, hours, serviceId):
    for item in hours:
        if item['Time24'] == time and (item['ServiceId'] == serviceId or item['ServiceId'] == ''):
            return item
        if item['Time24'] == time and item['ServiceId'] != serviceId:
            return '0'
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
        businessId = event['pathParameters']['businessId']
        locationId = event['pathParameters']['locationId']
        providerId = event['pathParameters']['providerId']
        serviceId = event['pathParameters']['serviceId']
        appoDate = datetime.datetime.strptime(event['pathParameters']['appoDate'], '%m-%d-%Y')

        country_date = dateutil.tz.gettz(findTimeZone(businessId, locationId))
        today = datetime.datetime.now(tz=country_date)
        dateOpe = today.strftime("%Y-%m-%d-%H-%M-%S")
        
        dayName = appoDate.strftime("%A")[0:3].upper()
        dateStd = appoDate.strftime("%Y-%m-%d")
        dateMonth = appoDate.strftime("%Y-%m")
        dateFin = appoDate + datetime.timedelta(days=90)
        dateFin = dateFin.strftime("%Y-%m-%d")

        statusPlan = 0
        numberAppos = 0
        availablePackAppos = 0
        result = {}
        hoursData = []
        services = []
        hours = []
        currHour = ''
        isCurrDay = 0
        bucket = 0

        if appoDate.strftime("%Y-%m-%d") < today.strftime("%Y-%m-%d"):
            statusCode = 200
            body = json.dumps({'Message': 'Date time invalid', 'Hours': [], 'Code': 200})
            response = {
                'statusCode' : statusCode,
                'headers' : {
                    "content-type" : "application/json",
                    "access-control-allow-origin" : "*"
                },
                'body' : body
            }
            return response

        if appoDate.strftime("%Y-%m-%d") == today.strftime("%Y-%m-%d"):
            currHour = today.strftime("%H:%M")
            currHour = int(str(currHour)[0:2])
            isCurrDay = 1

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

                if bucket == 0:
                    statusCode = 500
                    body = json.dumps({'Message': 'No data for this service provider', 'Code': 500})
                    return
                    
                #GET OPERATION HOURS FROM SPECIFIC LOCATION
                getCurrDate = dynamodb.query(
                    TableName = "TuCita247",
                    ReturnConsumedCapacity = 'TOTAL',
                    KeyConditionExpression = 'PKID = :businessId AND SKID = :providerId',
                    ExpressionAttributeValues = {
                        ':businessId': {'S': 'BUS#'+businessId+'#LOC#'+locationId},
                        ':providerId': {'S': 'PRO#'+providerId}
                    },
                    Limit = 1
                )
                for currDate in json_dynamodb.loads(getCurrDate['Items']):
                    periods = []
                    dayOffValid = True

                    opeHours = json.loads(currDate['OPERATIONHOURS'])
                    # numCustomer = currDate['CUSTOMER_PER_BUCKET']
                    # bucket = currDate['BUCKET_INTERVAL']
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
                    #OBTIENE LAS CITAS DEL DIA SOLICITADO
                    getAppos = dynamodb.query(
                        TableName="TuCita247",
                        IndexName="TuCita247_Index",
                        ReturnConsumedCapacity='TOTAL',
                        KeyConditionExpression='GSI1PK = :key01 and begins_with(GSI1SK, :key02)',
                        ExpressionAttributeValues={
                            ':key01': {'S': 'BUS#'+businessId+'#LOC#'+locationId+'#PRO#'+providerId},
                            ':key02': {'S': '1#DT#'+dateStd}
                        }
                    )
                    for hrsData in json_dynamodb.loads(getAppos['Items']):
                        timeBooking = int(hrsData['GSI1SK'].replace('1#DT#'+dateStd+'-','')[0:2])
                        cxTime = findServiceTime(hrsData['SERVICEID'], services)
                        recordset = {
                            'Hour': timeBooking,
                            'ServiceId': hrsData['SERVICEID'],
                            'People': hrsData['PEOPLE_QTY'],
                            'TimeService': cxTime,
                            'Cancel': 0
                        }
                        resAppo = findHoursAppo(timeBooking, hoursBooks, hrsData['SERVICEID'])
                        if resAppo == '':
                            hoursBooks.append(recordset)
                        else:
                            hoursBooks.remove(resAppo)
                            recordset['People'] = int(hrsData['PEOPLE_QTY'])+int(resAppo['People']) 
                            hoursBooks.append(recordset)
                    
                    #OBTIENE LAS CITAS Q VAN EN PROCESO DEL DIA ACTUAL SI FUERA NECESARIO
                    if str(dateOpe[0:10]) == str(dateStd):
                        getAppos02 = dynamodb.query(
                            TableName="TuCita247",
                            IndexName="TuCita247_Index",
                            ReturnConsumedCapacity='TOTAL',
                            KeyConditionExpression='GSI1PK = :key01 and begins_with(GSI1SK, :key02)',
                            ExpressionAttributeValues={
                                ':key01': {'S': 'BUS#'+businessId+'#LOC#'+locationId+'#PRO#'+providerId},
                                ':key02': {'S': '2#DT#'+dateStd}
                            }
                        )
                        for hrCita in json_dynamodb.loads(getAppos02['Items']):
                            timeBooking = int(hrCita['GSI1SK'].replace('2#DT#'+dateStd+'-','')[0:2])
                            cxTime = findServiceTime(hrCita['SERVICEID'], services)
                            citasProgress = {
                                'Hour': timeBooking,
                                'ServiceId': hrCita['SERVICEID'],
                                'People': hrCita['PEOPLE_QTY'],
                                'TimeService': cxTime,
                                'Cancel': 0
                            }
                            resAppo = findHoursAppo(timeBooking, hoursBooks, hrCita['SERVICEID'])
                            if resAppo == '':
                                hoursBooks.append(citasProgress)
                            else:
                                hoursBooks.remove(resAppo)
                                citasProgress['People'] = int(hrCita['PEOPLE_QTY'])+int(resAppo['People']) 
                                hoursBooks.append(citasProgress)
                    #OBTIENE LAS CITAS EN RESERVA DE UN DIA
                    getReservas = dynamodb.query(
                        TableName="TuCita247",
                        IndexName="TuCita247_Index",
                        ReturnConsumedCapacity='TOTAL',
                        KeyConditionExpression='GSI1PK = :key01 and begins_with(GSI1SK, :key02)',
                        ExpressionAttributeValues={
                            ':key01': {'S': 'RES#BUS#'+businessId+'#LOC#'+locationId+'#PRO#'+providerId},
                            ':key02': {'S': '1#DT#'+dateStd}
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
                    
                    #GET CANCEL OR OPEN HOURS
                    getCurrHours = dynamodb.query(
                        TableName="TuCita247",
                        ReturnConsumedCapacity='TOTAL',
                        KeyConditionExpression='PKID = :key',
                        ExpressionAttributeValues = {
                            ':key': {'S': 'LOC#'+locationId+'#PRO#'+providerId+'#DT#'+dateStd}
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
                            timeExists = findTime(int(cancel['SKID'].replace('HR#','')[0:2]), hoursBooks)
                            if timeExists == '':
                                hoursBooks.append(recordset)
                            else:
                                hoursBooks.remove(timeExists)
                                hoursBooks.append(recordset)
                        if int(cancel['AVAILABLE']) == 1:
                            recordset = {
                                'Hour': int(cancel['SKID'].replace('HR#','')[0:2]),
                                'ServiceId': '',
                                'People': 99,
                                'TimeService': 0,
                                'Cancel': 0
                            }
                            timeExists = findTime(int(cancel['SKID'].replace('HR#','')[0:2]), hoursBooks)
                            if timeExists == '':
                                hoursBooks.append(recordset)
                    logger.info('display hoursbook')
                    logger.info(hoursBooks)
                    mergeBooks = []
                    logger.info('Init For HoursBooks')
                    for data in hoursBooks:
                        if data['Cancel'] == 0 and (int(data['TimeService']) > 1):
                            times = range(0, data['TimeService'])
                            timeInterval = []
                            count = 0
                            for hr in times:

                                newTime = str(int(data['Hour'])+hr)
                                time24hr = int(newTime) 
                                newTime = newTime.rjust(2,'0')+':00'
                                result = findTime(time24hr, mergeBooks)
                                if result != '':
                                    recordset = {
                                        'Hour': time24hr,
                                        'ServiceId': data['ServiceId'],
                                        'People': int(data['People'])+int(result['People']),
                                        'TimeService': data['TimeService'],
                                        'Cancel': 0
                                    }
                                    mergeBooks.remove(result)
                                    mergeBooks.append(recordset)
                                else:
                                    recordset = {
                                        'Hour': time24hr,
                                        'ServiceId': data['ServiceId'],
                                        'People': int(data['People']),
                                        'TimeService': data['TimeService'],
                                        'Cancel': 0
                                    }
                                    mergeBooks.append(recordset)
                        else:
                            mergeBooks.append(data)
                    hoursBooks = []
                    hoursBooks = mergeBooks  
                    logger.info('display mergeBooks')
                    logger.info(mergeBooks)        
                    for item in hoursBooks:
                        logger.info(item)
                        if item['Cancel'] == 1:
                            timeExists = findTime(str(item['Hour']).rjust(2,'0')+':00', hoursData)
                            if timeExists != '':
                                hoursData.remove(timeExists)

                            recordset = {
                                'Hour': str(item['Hour']).rjust(2,'0')+':00',
                                'Time24': item['Hour'],
                                'ServiceId': '',
                                'Available': 0,
                                'TimeService': 0,
                                'Cancel': 1
                            }
                            hoursData.append(recordset)
                        else:
                            custPerTime = 0
                            if 'ServiceId' in item:
                                if item['ServiceId'] != '':
                                    custPerTime = findService(item['ServiceId'], services)
                            
                            if (int(item['TimeService']) > 1):
                                times = range(0, item['TimeService'])
                                timeInterval = []
                                #CONSOLIDA HORAS DE BOOKINGS
                                count = -1
                                for hr in times:
                                    newTime = str(int(item['Hour'])+hr)
                                    time24hr = int(newTime) 
                                    newTime = newTime.rjust(2,'0')+':00'
                                    result = findTime(time24hr, hoursBooks)
                                    if result != '':
                                        if result['Cancel'] != 1:
                                            if result['ServiceId'] == item['ServiceId'] or result['ServiceId'] == '':
                                                if result['ServiceId'] != '':
                                                    if count == -1 or count < result['People']:
                                                        count = result['People']
                                            if result['ServiceId'] != item['ServiceId'] and result['ServiceId'] != '':
                                                count = custPerTime
                                                break
                                        else:
                                            count = custPerTime
                                    else:
                                        noExiste = 0
                                        for timeAv in dateAppo:
                                            ini = int(timeAv['I'])
                                            fin = int(timeAv['F'])-1
                                            # logger.info('ini ' + str(ini) + ' -- ' + str(fin) + ' hr ' + str(newTime[0:2]))
                                            if int(newTime[0:2]) >= ini and int(newTime[0:2]) <= fin:
                                                # logger.info('ingreso -- ' + str(count))
                                                noExiste = 1
                                                break
                                        if noExiste == 0:
                                            count = -1
                                            break
                                if count == -1:
                                    count = custPerTime          
                                recordset = {
                                    'Hour': newTime,
                                    'Time24': time24hr,
                                    'ServiceId': item['ServiceId'],
                                    'TimeService': item['TimeService'],
                                    'Available': custPerTime-count,
                                    'Cancel': 0
                                }
                                hoursData.append(recordset)
                            else:
                                recordset = {
                                    'Hour': str(item['Hour']).rjust(2,'0')+':00',
                                    'Time24': item['Hour'],
                                    'ServiceId': item['ServiceId'],
                                    'TimeService': item['TimeService'],
                                    'Available': custPerTime-item['People'] if custPerTime > 0 else item['People'],
                                    'Cancel': 0
                                }
                                hoursData.append(recordset)
                    
                    logger.info(hoursData)
                    prevFin = 0
                    ini = 0
                    fin = 24
                    scale = 10
                    for h in range(ini, fin):
                        hStd = str(h).zfill(2) + ':00'
                        res = h if h < 13 else h-12
                        h = str(res).zfill(2) + ':00 ' + 'AM' if h < 12 else str(res).zfill(2) + ':00 ' + 'PM'
                        found = searchTime(int(hStd[0:2]), hoursData, serviceId)
                        time24hr = int(hStd[0:2])
                        if found == '':
                            count = 0
                            for item in dateAppo:
                                ini = int(item['I'])
                                fin = int(item['F'])-1
                                prevCount = -1
                                # logger.info('Data hr: ' + hStd[0:2] + ' -- ini: ' + str(ini) + ' -- fin: ' + str(fin))
                                if int(hStd[0:2]) >= ini and int(hStd[0:2])+bucket-1 <= fin:
                                    if int(bucket) > 1:
                                        for citas in range(1, bucket):
                                            nextHr = time24hr+citas
                                            getApp = searchTime(int(nextHr), hoursData, serviceId)
                                            if getApp != '' and getApp != '0':
                                                if getApp['Available'] <= 0:
                                                    count = 0
                                                    break
                                                else:
                                                    if count == 0 or count > getApp['Available']:
                                                        count = getApp['Available']
                                            if getApp == '0':
                                                count = 0
                                                break
                                            if getApp == '':
                                                entro = 0
                                                for item02 in dateAppo:
                                                    ini02 = int(item02['I'])
                                                    fin02 = int(item02['F'])-1
                                                    if int(nextHr) >= ini and int(nextHr) <= fin:
                                                        entro = 1
                                                        break
                                                if entro == 0:
                                                    count = 0
                                                    break
                                                if count == 0 or count >= +numCustomer:
                                                    count = +numCustomer
                                    else:
                                        count = +numCustomer
                                    break
                            if count > 0:
                                if isCurrDay == 1 and time24hr > currHour:
                                    recordset = {
                                        'Hour': h,
                                        'Time24': time24hr,
                                        'Available': count
                                    }
                                    hours.append(recordset)
                                if isCurrDay == 0:
                                    recordset = {
                                        'Hour': h,
                                        'Time24': time24hr,
                                        'Available': count
                                    }
                                    hours.append(recordset)
                        else:
                            if found != '0':
                                if bucket > 1:
                                    count = found['Available']
                                    for citas in range(1,bucket):
                                        available = searchTime(int(found['Time24'])+citas, hoursData, serviceId)
                                        if available != '' and available != '0':
                                            if available['Available'] <= 0:
                                                count = 0
                                                break
                                            else:
                                                if count == 0 or count > available['Available']:
                                                    count = available['Available']
                                        if available == '0':
                                            count = 0
                                            break
                                        if available == '':
                                            entro = 0
                                            for item02 in dateAppo:
                                                ini02 = int(item02['I'])
                                                fin02 = int(item02['F'])-1
                                                if int(int(found['Time24'])+citas) >= ini and int(int(found['Time24'])+citas) <= fin:
                                                    entro = 1
                                                    break
                                            if entro == 0:
                                                count = 0
                                                break
                                            else:
                                                if count > +numCustomer:
                                                    count = +numCustomer
                                    if count == 99:
                                        count = numCustomer
                                    if count > 0:
                                        if isCurrDay == 1 and time24hr > currHour:
                                            recordset = {
                                                'Hour': h,
                                                'Time24': time24hr,
                                                'Available': count
                                            }
                                            hours.append(recordset)
                                        if isCurrDay == 0:
                                            recordset = {
                                                'Hour': h,
                                                'Time24': time24hr,
                                                'Available': count
                                            }
                                            hours.append(recordset)
                                else:
                                    if int(found['Cancel']) == 0 and int(found['Available']) > 0:
                                        if isCurrDay == 1 and time24hr > currHour:
                                            recordset = {
                                                'Hour': h,
                                                'Time24': time24hr,
                                                'Available': found['Available']
                                            }
                                            hours.append(recordset)
                                        if isCurrDay == 0:
                                            recordset = {
                                                'Hour': h,
                                                'Time24': time24hr,
                                                'Available': found['Available']
                                            }
                                            hours.append(recordset)
                        hours.sort(key=getKey)
                statusCode = 200
                body = json.dumps({'Hours': hours, 'Code': 200})
        
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