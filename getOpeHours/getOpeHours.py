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

def findBookings(timeIni, timeFin, hours, service, intervalo):
    qty = 0
    temporal = []
    temporalFin = []
    for item in hours:
        if item['ServiceId'] == service:
            if item['Time'] >= timeIni and item['Time'] <= timeFin:
                logger.info("data "+str(item['Time'])+" -- "+str(item['People']))
                temporal.append(int(item['People']))
                qty = qty + item['People']    
            else:
                if item['Time']+intervalo >= timeIni and item['Time']+intervalo <= timeFin:
                    logger.info("data 00 "+str(item['Time'])+" -- "+str(item['People']))
                    temporalFin.append(int(item['People']))
                    qty = qty + item['People']
    x= list(set(temporal).intersection(temporalFin))
    result = 0
    if intervalo == 1:
        for y in x:
            result = result + y
        
    return qty-result
    
def findUsedHours(time, hours, serviceId):
    count = 0
    for item in time:
        if item['ServiceId'] == serviceId:
            if item['Time'] < time and item['Time'] >= time:
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
        bucket = 0
        businessId = event['pathParameters']['businessId']
        locationId = event['pathParameters']['locationId']
        providerId = event['pathParameters']['providerId']
        monday = datetime.datetime.strptime(event['pathParameters']['initDay'], '%Y-%m-%d')
        
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
                    'CustomerPerTime': int(serv['CUSTOMER_PER_TIME'])
                }
                services.append(recordset)

            x = range(0,7)
            minVal = 24
            maxVal = 0
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
                        timeBooking = int(hours['GSI1SK'].replace('1#DT#'+nextDate.strftime("%Y-%m-%d")+'-','')[0:2])
                        recordset = {
                            'Time': timeBooking,
                            'ServiceId': hours['SERVICEID'],
                            'People': hours['PEOPLE_QTY']
                        }
                        resAppo = findHoursAppo(timeBooking, hoursBooks, hours['SERVICEID'])
                        if resAppo == '':
                            hoursBooks.append(recordset)
                        else:
                            hoursBooks.remove(resAppo)
                            recordset['People'] = int(hours['PEOPLE_QTY'])+int(resAppo['People']) 
                            hoursBooks.append(recordset)
                    
                    getAvailability = dynamodb.query(
                        TableName="TuCita247",
                        ReturnConsumedCapacity='TOTAL',
                        KeyConditionExpression='PKID = :usedData',
                        ExpressionAttributeValues={
                            ':usedData': {'S': 'LOC#'+locationId+'#PRO#'+providerId+'#DT#'+nextDate.strftime("%Y-%m-%d")}
                        },
                        ScanIndexForward=True
                    )
                    bookings = json_dynamodb.loads(getAvailability['Items'])
                    for item in bookings:
                        custPerTime = 0
                        if 'SERVICEID' in item:
                            custPerTime = findService(item['SERVICEID'], services)
                        
                        if (int(item['TIME_SERVICE']) > 1):
                            times = range(0, item['TIME_SERVICE'])
                            changes = range(0, item['TIME_SERVICE'])
                            timeInterval = []

                            #CONSOLIDA HORAS DE BOOKINGS
                            for hr in times:
                                count = 0
                                newTime = str(int(item['SKID'].replace('HR#','')[0:2])+hr)
                                time24hr = int(newTime) 
                                newTime = newTime.rjust(2,'0')+':'+item['SKID'].replace('HR#','')[3:5]

                                # horaIni = int(item['SKID'].replace('HR#','')[0:2])+hr
                                # horaFin = int(item['SKID'].replace('HR#','')[0:2])+int(item['TIME_SERVICE'])-1+hr
                                # logger.info("Eval Time " + str(item['SKID'].replace('HR#','')) + " sub hora " + str(hr) + " ini " + str(horaIni) + " fin " + str(horaFin))   
                                count = findUsedHours(time24hr, hoursBooks, item['SERVICEID'])
                                
                                # if getApp != '':
                                #     count = findBookings(horaIni, horaFin, hoursBooks, item['SERVICEID'], int(item['TIME_SERVICE'])-1)
                                #     logger.info("cont " + str(count))
                                # else:
                                #     if availableHour(time24hr, newTime.replace(':','-'), dayHours, locationId, providerId, item['SERVICEID'], nextDate) == False:
                                #         count = custPerTime
                                #     else:
                                #         count = findBookings(horaIni, horaFin, hoursBooks, item['SERVICEID'], int(item['TIME_SERVICE'])-1)
                                     
                                res = range(1, int(item['TIME_SERVICE']))
                                for citas in res:
                                    nextHr = time24hr+citas
                                    newHr = str(nextHr).rjust(2,'0')+'-'+item['SKID'].replace('HR#','')[3:5]
                                    getApp = findHours(nextHr, hoursBooks)
                                    if getApp != '':
                                        if getApp['ServiceId'] != item['SERVICEID']:
                                            count = custPerTime
                                            break
                                        # else:
                                        #     nexCount = findBookings(nextHr, nextHr+1, hoursBooks, item['SERVICEID'], int(item['TIME_SERVICE'])-1)
                                        #     count = count + nexCount
                                    else:
                                        if availableHour(nextHr, newHr, dayHours, locationId, providerId, item['SERVICEID'], nextDate) == False:
                                            count = custPerTime
                                            break
                                    tempCount = findUsedHours(nextHr, hoursBooks, item['SERVICEID'])
                                        # else:
                                        #     nexCount = findBookings(nextHr, nextHr+1, hoursBooks, item['SERVICEID'], int(item['TIME_SERVICE'])-1)
                                        #     count = count + nexCount
                                    
                                            
                                recordset = {
                                    'Time': newTime,
                                    'TimeService': item['TIME_SERVICE'],
                                    'ServiceId': item['SERVICEID'],
                                    'Bucket': custPerTime,
                                    'Available': custPerTime-count,
                                    'Used': count,
                                    'Cancel': 0
                                }
                                hoursData.append(recordset)
                        else:
                            if int(item['CANCEL']) == 1:
                                timeExists = findHours(item['SKID'].replace('HR#','').replace('-',':'), hoursData)
                                if timeExists != '':
                                    hoursData.remove(timeExists)

                                recordset = {
                                    'Time': item['SKID'].replace('HR#','').replace('-',':'),
                                    'TimeService': 1,
                                    'ServiceId': '',
                                    'Bucket': 0,
                                    'Available': 0,
                                    'Used': 0,
                                    'Cancel': 1
                                }
                                hoursData.append(recordset)
                            else:
                                recordset = {
                                    'Time': item['SKID'].replace('HR#','').replace('-',':'),
                                    'TimeService': item['TIME_SERVICE'],
                                    'ServiceId': item['SERVICEID'],
                                    'Bucket': custPerTime,
                                    'Available': item['AVAILABLE'],
                                    'Used': custPerTime-int(item['AVAILABLE']),
                                    'Cancel': 0
                                }
                                hoursData.append(recordset)
                                    
                    ini = 0
                    fin = 0
                    interval = 1
                    bucket = 1
                    initRange = 0
                    iniVal = 0
                    finVal = 0
                    for dt in dayHours:
                        ini = Decimal(dt['I'])
                        fin = Decimal(dt['F'])
                        scale = 10
                        initRange = initRange + 1
                        if len(dayHours) == 1 or initRange == 2:
                            for h in range(int(scale*fin), int(scale*24), int(scale*interval)):
                                if (h/scale).is_integer():
                                    h = str(math.trunc(h/scale)).zfill(2) + ':00' 
                                else:
                                    h = str(math.trunc(h/scale)).zfill(2) + ':30'
                                if findHours(h, hoursData) != '':
                                    record = findHours(h, hoursData)
                                    if int(h[0:2]) > 12:
                                        h = str(int(h[0:2])-12).zfill(2) + h[2:5] + ' PM'
                                    else:
                                        h = h + ' AM' if int(h[0:2]) < 12 else h + ' PM'
                                    recordset = {
                                        'Time': h,
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
                        if len(dayHours) == 2:
                            if initRange == 1:
                                iniVal = Decimal(dt['I'])
                                finVal = Decimal(dt['F'])
                            if initRange == 2:
                                for h in range(int(scale*finVal), int(scale*ini), int(scale*interval)):
                                    if (h/scale).is_integer():
                                        h = str(math.trunc(h/scale)).zfill(2) + ':00' 
                                    else:
                                        h = str(math.trunc(h/scale)).zfill(2) + ':30'
                                    if findHours(h, hoursData) != '':
                                        record = findHours(h, hoursData)
                                        if int(h[0:2]) > 12:
                                            h = str(int(h[0:2])-12).zfill(2) + h[2:5] + ' PM'
                                        else:
                                            h = h + ' AM' if int(h[0:2]) < 12 else h + ' PM'
                                        recordset = {
                                            'Time': h,
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
                        
                        if minVal > ini:
                            minVal = ini
                        if maxVal < fin:
                            maxVal = fin
                        for h in range(int(scale*ini), int(scale*fin), int(scale*interval)):
                            if (h/scale).is_integer():
                                h = str(math.trunc(h/scale)).zfill(2) + ':00' 
                            else:
                                h = str(math.trunc(h/scale)).zfill(2) + ':30'
                            
                            if findHours(h, hoursData) == '':
                                if int(h[0:2]) > 12:
                                    h = str(int(h[0:2])-12).zfill(2) + h[2:5] + ' PM'
                                else:
                                    h = h + ' AM' if int(h[0:2]) < 12 else h + ' PM'
                                recordset = {
                                    'Time': h,
                                    'Bucket': 1,
                                    'Available': 1,
                                    'ServiceId': '',
                                    'Used': 0,
                                    'Cancel': 0
                                }
                            else:
                                record = findHours(h, hoursData)
                                h = record['Time']
                                if int(h[0:2]) > 12:
                                    h = str(int(h[0:2])-12).zfill(2) + h[2:5] + ' PM'
                                else:
                                    h = h + ' AM' if int(h[0:2]) < 12 else h + ' PM'

                                recordset = {
                                    'Time': h,
                                    'Bucket': record['Bucket'],
                                    'Available': record['Available'],
                                    'ServiceId': record['ServiceId'],
                                    'Used': record['Used'],
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

            scale = 10
            recordset = {}
            for val in  range(int(scale*minVal), int(scale*maxVal), int(scale*interval)):
                if (val/scale).is_integer():
                    h24 = str(math.trunc(val/scale)).zfill(2) + ':00' 
                else:
                    h24 = str(math.trunc(val/scale)).zfill(2) + ':30'
                
                if int(h24[0:2]) > 12:
                    h = str(int(h24[0:2])-12).zfill(2) + h24[2:5] + ' PM'
                else:
                    h = h24 + ' AM' if int(h24[0:2]) < 12 else h24 + ' PM'
                recordset = {
                    'Time': h,
                    'Time24H': h24
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