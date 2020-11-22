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

def searchHours(time, hours):
    for item in hours:
        if item['Hour'] == time:
            return item
    item = ''
    return item

def searchTime(time, hours, serviceId):
    for item in hours:
        if item['Time24'] == time and item['ServiceId'] == serviceId:
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

def lambda_handler(event, context):
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']

    try:
        statusCode = ''
        businessId = event['pathParameters']['businessId']
        locationId = event['pathParameters']['locationId']
        providerId = event['pathParameters']['providerId']
        serviceId = event['pathParameters']['serviceId']
        

        country_date = dateutil.tz.gettz('America/Puerto_Rico')
        today = datetime.datetime.now(tz=country_date)
        dateOpe = today.strftime("%Y-%m-%d-%H-%M-%S")
        appoDate = today

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
                    KeyConditionExpression = 'PKID = :businessId AND SKID BETWEEN :key and :fin',
                    ExpressionAttributeValues = {
                        ':businessId': {'S': 'BUS#' + businessId},
                        ':key': {'S': 'PACK#' + appoDate.strftime("%Y-%m-%d")},
                        ':fin': {'S': 'PACK#' + dateFin}
                    }
                )
                for availablePlan in json_dynamodb.loads(avaiAppoPack['Items']):
                    if availablePlan['AVAILABLE'] > 0:
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

                #GET SERVICES 
                service = dynamodb.query(
                    TableName="TuCita247",
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='PKID = :businessId AND SKID = :serviceId',
                    ExpressionAttributeValues={
                        ':businessId': {'S': 'BUS#'+businessId},
                        ':serviceId': {'S': 'SER#'+serviceId}
                    }
                )
                for serv in json_dynamodb.loads(service['Items']):
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
                                'People': 99,
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
                                custPerTime = findService(item['ServiceId'], services)
                            
                            if (int(item['TimeService']) > 1):
                                times = range(0, item['TimeService'])
                                timeInterval = []
                                #CONSOLIDA HORAS DE BOOKINGS
                                for hr in times:
                                    count = 0
                                    newTime = str(int(item['Hour'])+hr)
                                    time24hr = int(newTime) 
                                    newTime = newTime.rjust(2,'0')+':00'

                                    count = findUsedHours(time24hr, hoursBooks, item['ServiceId'], int(item['TimeService'])-1)        
                                    res = range(1, int(item['TimeService']))
                                    for citas in res:
                                        nextHr = time24hr+citas
                                        newHr = str(nextHr).rjust(2,'0')+'-00'
                                        getApp = searchHours(nextHr, hoursBooks)
                                        if getApp != '':
                                            if getApp['ServiceId'] != item['ServiceId']:
                                                count = custPerTime
                                                break
                                        tempCount = findUsedHours(nextHr, hoursBooks, item['ServiceId'], int(item['TimeService'])-1)
                                        if tempCount > count:
                                            count = tempCount

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
                                    'Available': custPerTime-item['People'],
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
                                fin = int(item['F'])
                                prevCount = -1
                                if int(hStd[0:2]) >= ini and int(hStd[0:2]) <= fin:
                                    if int(bucket) > 1:
                                        for citas in range(1, bucket):
                                            nextHr = time24hr+citas
                                            getApp = searchTime(int(nextHr), hoursData, serviceId)
                                            if getApp != '':
                                                if getApp != '0':
                                                    if prevCount == -1:
                                                        count = getApp['Available']
                                                        prevCount = 0
                                                    if prevCount == 0:
                                                        if count > getApp['Available']:
                                                            count = getApp['Available']
                                                else:
                                                    count = 0
                                                    break
                                            else:
                                                if prevCount == -1:
                                                    count = +numCustomer
                                                    prevCount = 0
                                                if prevCount == 0:
                                                    if count > numCustomer:
                                                        count = +numCustomer
                                    else:
                                        count = +numCustomer
                            if count > 0:
                                if isCurrDay == 1 and time24hr >= currHour:
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
                                if found['TimeService'] > 1:
                                    if found['Available'] > 0:
                                        count = 0
                                        for item in dateAppo:
                                            ini = int(item['I'])
                                            fin = int(item['F'])
                                            prevCount = -1
                                            if int(hStd[0:2]) >= ini and int(hStd[0:2]) <= fin:
                                                for citas in range(1, bucket):
                                                    nextHr = time24hr+citas
                                                    if nextHr == 24:
                                                        count = 0
                                                        break
                                                    if nextHr >= ini and nextHr <= fin:
                                                        getApp = searchTime(int(nextHr), hoursData, serviceId)
                                                        if getApp != '':
                                                            if getApp != '0':
                                                                if prevCount == -1:
                                                                    count = getApp['Available']
                                                                    prevCount = 0
                                                                if prevCount == 0:
                                                                    if count > getApp['Available']:
                                                                        count = getApp['Available']
                                                            else:
                                                                count = 0
                                                                break
                                                        else:
                                                            if prevCount == -1:
                                                                count = +numCustomer
                                                                prevCount = 0
                                                            if prevCount == 0:
                                                                if count > numCustomer:
                                                                    count = +numCustomer
                                                    else:
                                                        count = 0
                                                        break
                                        if count > 0:
                                            if isCurrDay == 1 and time24hr >= currHour:
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
                                        if isCurrDay == 1 and time24hr >= currHour:
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
            "access-control-allow-origin" : cors
        },
        'body' : body
    }
    return response