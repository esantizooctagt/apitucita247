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

def findHoursTime(time, hours):
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
        
        country_date = dateutil.tz.gettz(findTimeZone(businessId, locationId))
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
            mins = ''
            if int(currHour[-2:]) < 15:
                mins = '00'
            if int(currHour[-2:]) < 30 and int(currHour[-2:]) > 15:
                mins = '15'
            if int(currHour[-2:]) < 45 and int(currHour[-2:]) > 30:
                mins = '30'
            if int(currHour[-2:]) < 59:
                mins = '45'
            currHour = int(str(currHour)[0:2]+mins)
            # currHour = int(str(currHour)[0:2])
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
                    dayHours = repValues(opeHours[dayName]) if dayName in opeHours else ''
                    # numCustomer = currDate['CUSTOMER_PER_BUCKET']
                    # bucket = currDate['BUCKET_INTERVAL']
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
                        timeBooking = int(hrsData['GSI1SK'].replace('1#DT#'+dateStd+'-','')[0:5].replace('-',''))
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
                            timeBooking = int(hrCita['GSI1SK'].replace('2#DT#'+dateStd+'-','')[0:5].replace('-',''))
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
                        timeBooking = int(str(res['DATE_APPO'][-5:].replace('-','')))
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
                                'Hour': int(cancel['SKID'].replace('HR#','').replace('-','')),
                                'ServiceId': '',
                                'People': 0,
                                'TimeService': 0,
                                'Cancel': 1
                            }
                            timeExists = findHoursTime(int(cancel['SKID'].replace('HR#','').replace('-','')), hoursBooks)
                            if timeExists == '':
                                hoursBooks.append(recordset)
                            else:
                                hoursBooks.remove(timeExists)
                                hoursBooks.append(recordset)
                        if int(cancel['AVAILABLE']) == 1:
                            recordset = {
                                'Hour': int(cancel['SKID'].replace('HR#','').replace('-','')),
                                'ServiceId': '',
                                'People': 99,
                                'TimeService': 0,
                                'Cancel': 0
                            }
                            timeExists = findHoursTime(int(cancel['SKID'].replace('HR#','').replace('-','')), hoursBooks)
                            if timeExists == '':
                                hoursBooks.append(recordset)
                    
                    logger.info(hoursBooks)
                    mergeBooks = []
                    for data in hoursBooks:                    
                        if data['Cancel'] == 0 and (int(data['TimeService']) > 15):
                            # times = range(0, data['TimeService'])
                            timeInterval = []
                            count = 0
                            hrInterval = int(str(data['Hour']).rjust(4,'0')[-2:])
                            if hrInterval == 0:
                                times = timeSerHours()
                            if hrInterval == 15:
                                times = timeSerHours15()
                            if hrInterval == 30:
                                times = timeSerHours30()
                            if hrInterval == 45:
                                times = timeSerHours45()
                            countTime = 0
                            for hr in times:
                                if timeSerHours()[countTime] == int(data['TimeService']):
                                    break
                                # if hr > int(data['TimeService']):
                                #     break
                                countTime = countTime + 1
                                newTime = str(int(data['Hour'])+hr)
                                time24hr = int(newTime) 
                                newTime = newTime.rjust(4,'0')[0:2]+':'+newTime.rjust(4,'0')[-2:]
                                result = findHoursTime(time24hr, mergeBooks)

                                if result != '':
                                    recordset = {
                                        'Hour': time24hr,
                                        'Time24': data['Hour'],
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
                                        'Time24': data['Hour'],
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
                    for item in hoursBooks:
                        if item['Cancel'] == 1:
                            timeExists = findHoursTime(str(item['Hour']).rjust(4,'0')[0:2]+':'+str(item['Hour']).rjust(4,'0')[-2:], hoursData)
                            if timeExists != '':
                                hoursData.remove(timeExists)

                            recordset = {
                                'Hour': str(item['Hour']).rjust(4,'0')[0:2]+':'+str(item['Hour']).rjust(4,'0')[-2:],
                                'Time24': item['Hour'],
                                'TimeService': 1,
                                'ServiceId': '',
                                'Bucket': 0,
                                'Available': 0,
                                'Used': 0,
                                'Cancel': 1
                            }
                            hoursData.append(recordset)
                        else:
                            custPerTime = 0
                            if 'ServiceId' in item:
                                custPerTime = findService(item['ServiceId'], services)
                            
                            if (int(item['TimeService']) > 15):
                                # times = range(0, item['TimeService'])
                                timeInterval = []
                                #CONSOLIDA HORAS DE BOOKINGS
                                count = -1
                                hrInterval = int(str(item['Hour']).rjust(4,'0')[-2:])
                                if hrInterval == 0:
                                    times = timeSerHours()
                                if hrInterval == 15:
                                    times = timeSerHours15()
                                if hrInterval == 30:
                                    times = timeSerHours30()
                                if hrInterval == 45:
                                    times = timeSerHours45()
                                countTime = 0
                                for hr in times:
                                    if timeSerHours()[countTime] == int(item['TimeService']):
                                        break
                                    # if hr > int(item['TimeService']):
                                    #     break
                                    # logger.info(hr)
                                    countTime = countTime + 1
                                    newTime = str(int(item['Hour'])+hr)
                                    time24hr = int(newTime) 
                                    newTime = newTime.rjust(4,'0')[0:2]+':'+newTime.rjust(4,'0')[-2:]
                                    result = findHoursTime(time24hr, hoursBooks)
                                    # logger.info(result)
                                    if result != '':
                                        if result['Cancel'] != 1:
                                            if result['ServiceId'] == serviceId or result['ServiceId'] == '':
                                                if result['ServiceId'] != '':
                                                    if count == -1 or count < result['People']:
                                                        count = result['People']
                                            if result['ServiceId'] != serviceId and result['ServiceId'] != '':
                                                count = custPerTime
                                                break
                                        else:
                                            count = custPerTime
                                    else:
                                        noExiste = 0
                                        for timeAv in dayHours:
                                            ini = int(timeAv['I'])*100
                                            fin = (int(timeAv['F'])*100)-55
                                            # logger.info('ini ' + str(ini) + ' -- ' + str(fin) + ' hr ' + str(newTime[0:2]))
                                            if time24hr >= ini and time24hr <= fin:
                                                # logger.info('ingreso -- ' + str(count))
                                                noExiste = 1
                                                break
                                        if noExiste == 0:
                                            count = -1
                                            break
                                if count == -1:
                                    count = custPerTime          
                                recordset = {
                                    'Hour': str(item['Hour']).rjust(4,'0')[0:2]+':'+str(item['Hour']).rjust(4,'0')[-2:],
                                    'Time24': int(item['Hour']),
                                    'TimeService': item['TimeService'],
                                    'ServiceId': item['ServiceId'],
                                    'Bucket': custPerTime,
                                    'Available': custPerTime-count,
                                    'Used': count,
                                    'Cancel': 0
                                }
                                hoursData.append(recordset)
                            else:
                                recordset = {
                                    'Hour': str(item['Hour']).rjust(4,'0')[0:2]+':'+str(item['Hour']).rjust(4,'0')[-2:],
                                    'Time24': item['Hour'],
                                    'TimeService': item['TimeService'],
                                    'ServiceId': item['ServiceId'],
                                    'Bucket': custPerTime,
                                    'Available': custPerTime-item['People'],
                                    'Used': int(item['People']),
                                    'Cancel': 0
                                }
                                hoursData.append(recordset)
                    
                    logger.info(hoursData)
                    prevFin = 0
                    # ini = 0
                    # fin = 24
                    # scale = 10
                    for h in workHours():
                        hStd = h[0:2]+':'+h[-2:]
                        # hStd = str(h).zfill(2) + ':00'
                        # res = h if h < 13 else h-12
                        # h = str(res).zfill(2) + ':00 ' + 'AM' if h < 12 else str(res).zfill(2) + ':00 ' + 'PM'
                        found = searchTime(int(h), hoursData, serviceId)
                        time24hr = int(h)
                        if found == '':
                            count = 0
                            for item in dateAppo:
                                ini = int(item['I'])*100
                                fin = (int(float(item['F'])*100))-55 if str(int(float(item['F'])*100))[-2:] == "00" else (int(float(item['F'])*100))-15
                                prevCount = -1
                                # logger.info('Data hr: ' + hStd[0:2] + ' -- ini: ' + str(ini) + ' -- fin: ' + str(fin))
                                # if int(hStd[0:2]) >= ini and int(hStd[0:2])+bucket-1 <= fin:
                                hrInterval = int(h[-2:])
                                if hrInterval == 0:
                                    times = timeSerHours()
                                if hrInterval == 15:
                                    times = timeSerHours15()
                                if hrInterval == 30:
                                    times = timeSerHours30()
                                if hrInterval == 45:
                                    times = timeSerHours45()
                                addVal = timeSerHours().index(bucket)
                                if int(h) >= ini and int(h)+times[addVal] <= fin:
                                    if int(bucket) > 15:
                                        # for citas in range(1, bucket):
                                        countTime = 0
                                        for citas in times:
                                            if timeSerHours()[countTime] == bucket:
                                                break
                                            countTime = countTime + 1
                                            # if citas > bucket:
                                            #     break
                                            if citas > 0:
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
                                                        ini02 = int(item02['I'])*100
                                                        fin02 = (int(float(item02['F'])*100))-55 if str(int(float(item02['F'])*100))[-2:] == "00" else (int(float(item02['F'])*100))-15
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
                                if isCurrDay == 1 and time24hr >= currHour:
                                    recordset = {
                                        'Hour': hStd + ' AM' if int(h) < 1200 else (hStd + ' PM' if int(h) < 1245 else str(int(h)-1200).rjust(4,'0')[0:2]+':'+str(int(h)-1200).rjust(4,'0')[-2:] + ' PM'),
                                        'Time24': time24hr,
                                        'Available': count
                                    }
                                    hours.append(recordset)
                                if isCurrDay == 0:
                                    recordset = {
                                        'Hour': hStd + ' AM' if int(h) < 1200 else (hStd + ' PM' if int(h) < 1245 else str(int(h)-1200).rjust(4,'0')[0:2]+':'+str(int(h)-1200).rjust(4,'0')[-2:] + ' PM'),
                                        'Time24': time24hr,
                                        'Available': count
                                    }
                                    hours.append(recordset)
                        else:
                            if found != '0':
                                if bucket > 15:
                                    count = found['Available']
                                    hrInterval = int(h[-2:])
                                    if hrInterval == 0:
                                        times = timeSerHours()
                                    if hrInterval == 15:
                                        times = timeSerHours15()
                                    if hrInterval == 30:
                                        times = timeSerHours30()
                                    if hrInterval == 45:
                                        times = timeSerHours45()
                                    # for citas in range(1,bucket):
                                    countTime = 0
                                    for citas in times:
                                        if timeSerHours()[countTime] == bucket:
                                            break
                                        countTime = countTime + 1
                                        # if citas > bucket:
                                        #     break
                                        if citas > 0:
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
                                                    ini02 = int(item02['I'])*100
                                                    fin02 = (int(float(item02['F'])*100))-55 if str(int(float(item02['F'])*100))[-2:] == "00" else (int(float(item02['F'])*100))-15
                                                    if int(int(found['Time24'])+citas) >= ini02 and int(int(found['Time24'])+citas) <= fin02:
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
                                        if isCurrDay == 1 and time24hr >= currHour:
                                            recordset = {
                                                'Hour': hStd + ' AM' if int(h) < 1200 else (hStd + ' PM' if int(h) < 1245 else str(int(h)-1200).rjust(4,'0')[0:2]+':'+str(int(h)-1200).rjust(4,'0')[-2:] + ' PM'),
                                                'Time24': time24hr,
                                                'Available': count
                                            }
                                            hours.append(recordset)
                                        if isCurrDay == 0:
                                            recordset = {
                                                'Hour': hStd + ' AM' if int(h) < 1200 else (hStd + ' PM' if int(h) < 1245 else str(int(h)-1200).rjust(4,'0')[0:2]+':'+str(int(h)-1200).rjust(4,'0')[-2:] + ' PM'),
                                                'Time24': time24hr,
                                                'Available': count
                                            }
                                            hours.append(recordset)
                                else:
                                    if int(found['Cancel']) == 0 and (int(found['Available']) > 0 or int(found['Available']) == -99):
                                        if isCurrDay == 1 and time24hr >= currHour:
                                            recordset = {
                                                'Hour': hStd + ' AM' if int(h) < 1200 else (hStd + ' PM' if int(h) < 1245 else str(int(h)-1200).rjust(4,'0')[0:2]+':'+str(int(h)-1200).rjust(4,'0')[-2:] + ' PM'),
                                                'Time24': time24hr,
                                                'Available': 1 if found['Available'] == -99 else found['Available']
                                            }
                                            hours.append(recordset)
                                        if isCurrDay == 0:
                                            recordset = {
                                                'Hour': hStd + ' AM' if int(h) < 1200 else (hStd + ' PM' if int(h) < 1245 else str(int(h)-1200).rjust(4,'0')[0:2]+':'+str(int(h)-1200).rjust(4,'0')[-2:] + ' PM'),
                                                'Time24': time24hr,
                                                'Available': 1 if found['Available'] == -99 else found['Available']
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