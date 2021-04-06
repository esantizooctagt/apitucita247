import sys
import logging
import json

from decimal import *
import math
import datetime
import dateutil.tz
from datetime import timezone, timedelta
from operator import itemgetter

import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr
from dynamodb_json import json_util as json_dynamodb

import os

REGION = 'us-east-1'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.client('dynamodb', region_name=REGION)
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def findHours(time, hours):
    for item in hours:
        if item['Time'] == time:
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
        if item['ServiceId'] == serviceId:
            if item['Time'] < time and item['Time']+interval >= time:
                count = count + int(item['People'])
            if item['Time'] == time:
                count = count + int(item['People'])
    return count

def findHoursAppo(time, hours, service):
    for item in hours:
        if item['Time'] == time and item['ServiceId'] == service:
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
        bucket = 0
        businessId = event['pathParameters']['businessId']
        locationId = event['pathParameters']['locationId']
        providerId = event['pathParameters']['providerId']
        monday = datetime.datetime.strptime(event['pathParameters']['initDay'], '%Y-%m-%d')

        country_date = dateutil.tz.gettz(findTimeZone(businessId, locationId))
        today = datetime.datetime.now(tz=country_date)
        dateOpe = today.strftime("%Y-%m-%d")
        
        #GET OPERATION HOURS
        response = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :businessId AND SKID = :providerId',
            ExpressionAttributeValues={
                ':businessId': {'S': 'BUS#'+businessId+'#LOC#'+locationId},
                ':providerId': {'S': 'PRO#'+providerId}
            },
            Limit = 1
        )
        Hours = []
        Monday = []
        Tuesday = []
        Wednesday = []
        Thursday = []
        Friday = []
        Saturday = []
        Sunday = []
        services = []
        for row in json_dynamodb.loads(response['Items']):
            # bucket = row['CUSTOMER_PER_BUCKET'] if 'CUSTOMER_PER_BUCKET' in row else 0
            daysOff = row['DAYS_OFF'] if 'DAYS_OFF' in row else []
            # interval = row['BUCKET_INTERVAL'] if 'BUCKET_INTERVAL' in row else 0 
            opeHours = json.loads(row['OPERATIONHOURS'])

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

            x = range(0,7)
            minVal = 24
            maxVal = 0
            # for n in range(3,4):
            for n in x:
                dayOffValid = True
                #DIA DE OPERACION
                nextDate = monday + datetime.timedelta(days=n)
                #NOMBRE DEL DIA
                dayName = nextDate.strftime("%A")[0:3].upper()
                #HORAS DE TRABAJO DEL PROVEEDOR
                dayHours = opeHours[dayName] if dayName in opeHours else ''
                #VALIDA SI ES DIA DE DESCANSO PARA EL PROVEEDOR SI ES SALE DEL PROCESO Y VA AL SIGUIENTE DIA SINO SIGUE
                if daysOff != []:
                    dayOffValid = nextDate.strftime("%Y-%m-%d") not in daysOff

                if dayOffValid == True:
                    hoursData = []
                    hoursBooks = []
                    #OBTIENE LAS CITAS DE ESE DIA
                    getAppos = dynamodb.query(
                        TableName="TuCita247",
                        IndexName="TuCita247_Index",
                        ReturnConsumedCapacity='TOTAL',
                        KeyConditionExpression='GSI1PK = :key01 and begins_with(GSI1SK, :key02)',
                        ExpressionAttributeValues={
                            ':key01': {'S': 'BUS#'+businessId+'#LOC#'+locationId+'#PRO#'+providerId},
                            ':key02': {'S': '1#DT#'+nextDate.strftime("%Y-%m-%d")}
                        }
                    )
                    for hours in json_dynamodb.loads(getAppos['Items']):
                        timeBooking = int(hours['GSI1SK'].replace('1#DT#'+nextDate.strftime("%Y-%m-%d")+'-','')[0:5].replace('-',''))
                        cxTime = findServiceTime(hours['SERVICEID'], services)
                        recordset = {
                            'Time': timeBooking,
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
                    
                    #OBTIENE LAS CITAS DEL DIA ACTUAL Y CON ESTADO 2
                    if nextDate.strftime("%Y-%m-%d") == dateOpe:
                        getAppos02 = dynamodb.query(
                            TableName="TuCita247",
                            IndexName="TuCita247_Index",
                            ReturnConsumedCapacity='TOTAL',
                            KeyConditionExpression='GSI1PK = :key01 and begins_with(GSI1SK, :key02)',
                            ExpressionAttributeValues={
                                ':key01': {'S': 'BUS#'+businessId+'#LOC#'+locationId+'#PRO#'+providerId},
                                ':key02': {'S': '2#DT#'+nextDate.strftime("%Y-%m-%d")}
                            }
                        )
                        for hours in json_dynamodb.loads(getAppos02['Items']):
                            timeBooking = int(hours['GSI1SK'].replace('2#DT#'+nextDate.strftime("%Y-%m-%d")+'-','')[0:5].replace('-',''))
                            cxTime = findServiceTime(hours['SERVICEID'], services)
                            recordset = {
                                'Time': timeBooking,
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
                    
                    #OBTIENE LAS CITAS DEL DIA ACTUAL Y CON ESTADO 3
                    if nextDate.strftime("%Y-%m-%d") == dateOpe:
                        getAppos02 = dynamodb.query(
                            TableName="TuCita247",
                            IndexName="TuCita247_Index",
                            ReturnConsumedCapacity='TOTAL',
                            KeyConditionExpression='GSI1PK = :key01 and begins_with(GSI1SK, :key02)',
                            ExpressionAttributeValues={
                                ':key01': {'S': 'BUS#'+businessId+'#LOC#'+locationId+'#PRO#'+providerId},
                                ':key02': {'S': '3#DT#'+nextDate.strftime("%Y-%m-%d")}
                            }
                        )
                        for hours in json_dynamodb.loads(getAppos02['Items']):
                            timeBooking = int(hours['GSI1SK'].replace('3#DT#'+nextDate.strftime("%Y-%m-%d")+'-','')[0:5].replace('-',''))
                            cxTime = findServiceTime(hours['SERVICEID'], services)
                            recordset = {
                                'Time': timeBooking,
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

                    #OBTIENE LAS CITAS EN RESERVA DE UN DIA
                    getReservas = dynamodb.query(
                        TableName="TuCita247",
                        IndexName="TuCita247_Index",
                        ReturnConsumedCapacity='TOTAL',
                        KeyConditionExpression='GSI1PK = :key01 and begins_with(GSI1SK, :key02)',
                        ExpressionAttributeValues={
                            ':key01': {'S': 'RES#BUS#'+businessId+'#LOC#'+locationId+'#PRO#'+providerId},
                            ':key02': {'S': '1#DT#'+nextDate.strftime("%Y-%m-%d")}
                        }
                    )
                    for res in json_dynamodb.loads(getReservas['Items']):
                        timeBooking = int(str(res['DATE_APPO'][-5:]).replace('-',''))
                        cxTime = findServiceTime(res['SERVICEID'], services)
                        recordset = {
                            'Time': timeBooking,
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
                    getAvailability = dynamodb.query(
                        TableName="TuCita247",
                        ReturnConsumedCapacity='TOTAL',
                        KeyConditionExpression='PKID = :usedData',
                        ExpressionAttributeValues={
                            ':usedData': {'S': 'LOC#'+locationId+'#PRO#'+providerId+'#DT#'+nextDate.strftime("%Y-%m-%d")}
                        },
                        ScanIndexForward=True
                    )
                    for cancel in json_dynamodb.loads(getAvailability['Items']):
                        if int(cancel['CANCEL']) == 1:
                            recordset = {
                                'Time': int(cancel['SKID'].replace('HR#','').replace('-','')),
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
                                'Time': int(cancel['SKID'].replace('HR#','').replace('-','')),
                                'ServiceId': '',
                                'People': 0,
                                'TimeService': 0,
                                'Cancel': 0
                            }
                            timeExists = findHours(int(cancel['SKID'].replace('HR#','').replace('-','')), hoursBooks)
                            if timeExists == '':
                                hoursBooks.append(recordset)
                    # logger.info('display hoursbook')
                    # logger.info(hoursBooks)
                    mergeBooks = []
                    for data in hoursBooks:
                        if data['Cancel'] == 0 and (int(data['TimeService']) > 15):
                            # times = range(0, data['TimeService'])
                            timeInterval = []
                            count = 0
                            hrInterval = int(str(data['Time']).rjust(4,'0')[-2:])
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
                                countTime = countTime + 1
                                newTime = str(int(data['Time'])+hr)
                                time24hr = int(newTime) 
                                newTime = newTime.rjust(4,'0')[0:2]+':'+newTime.rjust(4,'0')[-2:]
                                result = findHours(time24hr, mergeBooks)
                                if result != '':
                                    recordset = {
                                        'Time': time24hr,
                                        'ServiceId': data['ServiceId'],
                                        'People': int(data['People'])+int(result['People']),
                                        'TimeService': data['TimeService'],
                                        'Cancel': 0
                                    }
                                    mergeBooks.remove(result)
                                    mergeBooks.append(recordset)
                                else:
                                    recordset = {
                                        'Time': time24hr,
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
                            timeExists = findHours(str(item['Time']).rjust(4,'0')[0:2]+':'+str(item['Time']).rjust(4,'0')[-2:], hoursData)
                            if timeExists != '':
                                hoursData.remove(timeExists)

                            recordset = {
                                'Time': str(item['Time']).rjust(4,'0')[0:2]+':'+str(item['Time']).rjust(4,'0')[-2:],
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
                                logger.info('--eval--')
                                logger.info(item)
                                # times = range(0, item['TimeService'])
                                timeInterval = []
                                #CONSOLIDA HORAS DE BOOKINGS
                                count = -1
                                hrInterval = int(str(item['Time']).rjust(4,'0')[-2:])
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
                                    countTime = countTime + 1
                                    newTime = str(int(item['Time'])+hr)
                                    time24hr = int(newTime) 
                                    newTime = newTime.rjust(4,'0')[0:2]+':'+newTime.rjust(4,'0')[-2:]
                                    result = findHours(time24hr, hoursBooks)
                                    # logger.info(result)
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
                                    'Time': str(item['Time']).rjust(4,'0')[0:2]+':'+str(item['Time']).rjust(4,'0')[-2:],
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
                                    'Time': str(item['Time']).rjust(4,'0')[0:2]+':'+str(item['Time']).rjust(4,'0')[-2:],
                                    'TimeService': item['TimeService'],
                                    'ServiceId': item['ServiceId'],
                                    'Bucket': custPerTime,
                                    'Available': custPerTime-item['People'],
                                    'Used': int(item['People']),
                                    'Cancel': 0
                                }
                                hoursData.append(recordset)
                    logger.info('result hours data')
                    logger.info(hoursData)
                    ini = 0
                    fin = 0
                    interval = 1
                    bucket = 1
                    initRange = 0
                    iniVal = 0
                    finVal = 0
                    rangeIni = 0
                    scale = 10
                    # for h in range(int(scale*0), int(scale*24), int(scale*interval)):
                    for h in workHours():
                        # if (h/scale).is_integer():
                        #     h = str(math.trunc(h/scale)).zfill(2) + ':00' 
                        # else:
                        #     h = str(math.trunc(h/scale)).zfill(2) + ':30'
                        time24 = int(h)
                        hStd = h[0:2]+':'+h[-2:]
                        if findHours(hStd, hoursData) != '':
                            record = findHours(hStd, hoursData)
                            # if int(h[0:2]) > 12:
                            #     h = str(int(h[0:2])-12).zfill(2) + h[2:5] + ' PM'
                            # else:
                            #     h = h + ' AM' if int(h[0:2]) < 1200 else h + ' PM'
                            # h = hStd + (' AM' if int(h) < 1200 else ' PM')
                            h = (hStd if int(h) <= 1245 else (str(int(h)-1200).rjust(4,'0')[0:2]+':'+str(int(h)-1200).rjust(4,'0')[-2:])) + (' AM' if int(h) < 1200 else ' PM')
                            recordset = {
                                'Time': h,
                                'Time24': time24,
                                'Bucket': 1 if record['ServiceId'] == '' else record['Bucket'],
                                'Available': 1 if record['ServiceId'] == '' else record['Available'],
                                'ServiceId': record['ServiceId'],
                                'Used': 0 if record['ServiceId'] == '' else record['Used'],
                                'Cancel': record['Cancel']
                            }
                            
                            if n == 0:
                                Monday.append(recordset)
                            if n == 1:
                                Tuesday.append(recordset)
                            if n == 2:
                                Wednesday.append(recordset)
                            if n == 3:
                                Thursday.append(recordset)
                            if n == 4:
                                Friday.append(recordset)
                            if n == 5:
                                Saturday.append(recordset)
                            if n == 6:
                                Sunday.append(recordset)
                    for dt in dayHours:
                        ini = int(dt['I'])*100
                        fin = (int(dt['F'])*100)-55
                        # for h in range(int(scale*ini), int(scale*fin), int(scale*interval)):
                        for h in workHours():
                            if int(h) >= ini and int(h) <= fin:
                                time24 = int(h)
                                recordset = ''
                                # if (h/scale).is_integer():
                                #     h = str(math.trunc(h/scale)).zfill(2) + ':00' 
                                # else:
                                #     h = str(math.trunc(h/scale)).zfill(2) + ':30'
                                hStd = h[0:2]+':'+h[-2:]
                                if findHours(hStd, hoursData) == '':
                                    # if int(h[0:2]) > 12:
                                    #     h = str(int(h[0:2])-12).zfill(2) + h[2:5] + ' PM'
                                    # else:
                                    #     h = h + ' AM' if int(h[0:2]) < 12 else h + ' PM'
                                    h = (hStd if int(h) <= 1245 else (str(int(h)-1200).rjust(4,'0')[0:2]+':'+str(int(h)-1200).rjust(4,'0')[-2:])) + (' AM' if int(h) < 1200 else ' PM')
                                    recordset = {
                                        'Time': h,
                                        'Time24': time24,
                                        'Bucket': 1,
                                        'Available': 1,
                                        'ServiceId': '',
                                        'Used': 0,
                                        'Cancel': 0
                                    }

                                if recordset != '':
                                    if n == 0:
                                        Monday.append(recordset)
                                    if n == 1:
                                        Tuesday.append(recordset)
                                    if n == 2:
                                        Wednesday.append(recordset)
                                    if n == 3:
                                        Thursday.append(recordset)
                                    if n == 4:
                                        Friday.append(recordset)
                                    if n == 5:
                                        Saturday.append(recordset)
                                    if n == 6:
                                        Sunday.append(recordset)

            scale = 10
            recordset = {}
            # for val in  range(int(scale*0), int(scale*24), int(scale*interval)):
            for val in workHours():
                # if (val/scale).is_integer():
                #     h24 = str(math.trunc(val/scale)).zfill(2) + ':00' 
                # else:
                #     h24 = str(math.trunc(val/scale)).zfill(2) + ':30'
                
                # if int(h24[0:2]) > 12:
                #     h = str(int(h24[0:2])-12).zfill(2) + h24[2:5] + ' PM'
                # else:
                #     h = h24 + ' AM' if int(h24[0:2]) < 12 else h24 + ' PM'
                h = (val[0:2]+':'+val[-2:] if int(val) <= 1245 else (str(int(val)-1200).rjust(4,'0')[0:2]+':'+str(int(val)-1200).rjust(4,'0')[-2:])) + (' AM' if int(val) < 1200 else ' PM')
                recordset = {
                    'Time': h,
                    'Time24H': int(val)
                }
                Hours.append(recordset)

            statusCode = 200
            body = json.dumps({'Hours': Hours, 'Monday': Monday, 'Tuesday': Tuesday, 'Wednesday': Wednesday, 'Thursday': Thursday, 'Friday': Friday, 'Saturday': Saturday, 'Sunday': Sunday,'Code': 200})

        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Message': 'No data for this service provider', 'Code': 500})
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