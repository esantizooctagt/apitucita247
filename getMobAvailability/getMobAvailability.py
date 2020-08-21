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

dynamodb = boto3.client('dynamodb', region_name='us-east-1')
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

def searchHours(time, hours):
    for item in hours:
        if item['Hour'] == time:
            return item
    item = ''
    return item

def lambda_handler(event, context):
    try:
        statusCode = ''
        businessId = event['pathParameters']['businessId']
        locationId = event['pathParameters']['locationId']
        providerId = event['pathParameters']['providerId']
        serviceId = event['pathParameters']['serviceId']
        appoDate = datetime.datetime.strptime(event['pathParameters']['appoDate'], '%m-%d-%Y')

        country_date = dateutil.tz.gettz('America/Puerto_Rico')
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
        hours = []
        currHour = ''

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
            dueDate = datetime.datetime.strptime(stat['DUE_DATE'], '%Y-%m-%d').strftime("%Y-%m-%d")
            if dueDate > today.strftime("%Y-%m-%d") and stat['STATUS'] == 1:
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
                
            #SIN DISPONIBILIDAD DE CITAS
            if numberAppos == 0:
                statusCode = 404
                body = json.dumps({'Message': 'No appointments available', 'Data': result, 'Code': 400})
            else:
                #ENTRA SI HAY CITAS DISPONIBLES YA SEA DE PLAN O PAQUETE VIGENTE
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
                    
                    #GET SUMMARIZE APPOINTMENTS FROM A SPECIFIC LOCATION AND PROVIDER FOR SPECIFIC DATE
                    getCurrHours = dynamodb.query(
                        TableName = "TuCita247",
                        ReturnConsumedCapacity = 'TOTAL',
                        KeyConditionExpression = 'PKID = :key',
                        ExpressionAttributeValues = {
                            ':key': {'S': 'LOC#'+locationId+'#PRO#'+providerId+'#DT#'+dateStd}
                        },
                        ScanIndexForward=True
                    )

                    hoursData = []
                    bookings = json_dynamodb.loads(getCurrHours['Items'])
                    for item in bookings:
                        if (int(item['TIME_SERVICE']) > 1):
                            times = range(0, item['TIME_SERVICE'])
                            changes = range(0, item['TIME_SERVICE'])
                            count = 0
                            timeInterval = []
                            #CONSOLIDA HORAS DE BOOKINGS
                            for hr in times:
                                newTime = str(int(item['SKID'].replace('HR#','')[0:2])+hr)
                                time24hr = newTime.rjust(2,'0')+'-'+item['SKID'].replace('HR#','')[3:5] 
                                newTime = newTime.rjust(2,'0')+':'+item['SKID'].replace('HR#','')[3:5]
                                 
                                getAppos = dynamodb.query(
                                    TableName="TuCita247",
                                    IndexName="TuCita247_Index",
                                    ReturnConsumedCapacity='TOTAL',
                                    KeyConditionExpression='GSI1PK = :key01 and GSI1SK = :key02',
                                    ExpressionAttributeValues={
                                        ':key01': {'S': 'BUS#'+businessId+'#LOC#'+locationId+'#PRO#'+providerId},
                                        ':key02': {'S': '1#DT#'+appoDate.strftime("%Y-%m-%d")+'-'+time24hr}
                                    }
                                )
                                for row in json_dynamodb.loads(getAppos['Items']):
                                    if row['PKID'] != '':
                                        count = count +1
                                recordset = {
                                    'Hour': newTime,
                                    'Available': row['CUSTOMER_PER_TIME']-count
                                }
                                
                                timeExists = searchHours(newTime, hoursData)
                                newAva = item['CUSTOMER_PER_TIME']-count
                                if timeExists == '':
                                    hoursData.append(recordset)
                                else:
                                    if timeExists['Available'] < item['CUSTOMER_PER_TIME']-count:
                                        newAva = timeExists['Available'] 
                                        
                                    hoursData.remove(timeExists)
                                    timeExists['Available'] = newAva
                                    hoursData.append(timeExists)
                        else:
                            recordset = {
                                'Hour': item['SKID'].replace('HR#','').replace('-',':'),
                                'Available': item['AVAILABLE']
                            }
                            hoursData.append(recordset)

                    for item in bookings:
                        if (int(item['TIME_SERVICE']) > 1):
                            # VALIDA HORA INICIAL DEL SERVICIO QUE SE PUEDA EJECUTAR O NO
                            checkHours = int(item['SKID'].replace('HR#','')[0:2])
                            y = range(1, int(item['TIME_SERVICE']))
                            availability = 0
                            for z in y:
                                opeTime = int(checkHours-z)
                                if len(dateAppo) >= 1:
                                    if opeTime >= int(dateAppo[0]['I']) and opeTime <= int(dateAppo[0]['F'])-1:
                                        checkDisp = searchHours(str(opeTime).rjust(2,'0')+':'+item['SKID'].replace('HR#','')[3:5], hoursData)
                                        if checkDisp == '':
                                            availability = 1
                                        else: 
                                            actHour = searchHours(str(checkHours).rjust(2,'0')+':'+item['SKID'].replace('HR#','')[3:5], hoursData)
                                            if item['SERVICEID'] == checkDisp['ServiceId'] and checkDisp['Available'] >= actHour['Available']:
                                                availability = 1
                                if len(dateAppo) == 2:
                                    if opeTime >= int(dateAppo[1]['I']) and opeTime <= int(dateAppo[1]['F'])-1:
                                        checkDisp = searchHours(str(opeTime).rjust(2,'0')+':'+item['SKID'].replace('HR#','')[3:5], hoursData)
                                        if checkDisp == '':
                                            availability = 1
                                        else: 
                                            actHour = searchHours(str(checkHours).rjust(2,'0')+':'+item['SKID'].replace('HR#','')[3:5], hoursData)
                                            if item['SERVICEID'] == checkDisp['ServiceId'] and checkDisp['Available'] >= actHour['Available']:
                                                availability = 1
    
                            if availability == 0:
                                actHour = searchHours(str(checkHours).rjust(2,'0')+':'+item['SKID'].replace('HR#','')[3:5], hoursData)
                                checkHours = str(checkHours+1).rjust(2,'0')+':'+item['SKID'].replace('HR#','')[3:5]
                                searchHour = searchHours(checkHours, hoursData)
                                if searchHour != '':
                                    hoursData.remove(actHour)
                                    actHour['Available'] = searchHour['Available']
                                    hoursData.append(actHour)
    
                            # VALIDA HORA FINAL DEL SERVICIO QUE SE PUEDA EJECUTAR O NO
                            checkHours = int(item['SKID'].replace('HR#','')[0:2])+int(item['TIME_SERVICE'])-1
                            y = range(1, int(item['TIME_SERVICE']))
                            availability = 0
                            for z in y:
                                opeTime = int(checkHours+z)
                                logger.info(opeTime)
                                if len(dateAppo) >= 1:
                                    if opeTime >= int(dateAppo[0]['I']) and opeTime <= int(dateAppo[0]['F'])-1:
                                        checkDisp = searchHours(str(opeTime).rjust(2,'0')+':'+item['SKID'].replace('HR#','')[3:5], hoursData)
                                        if checkDisp == '':
                                            availability = 1
                                        else: 
                                            actHour = searchHours(str(checkHours).rjust(2,'0')+':'+item['SKID'].replace('HR#','')[3:5], hoursData)
                                            if item['SERVICEID'] == checkDisp['ServiceId'] and checkDisp['Available'] >= actHour['Available']:
                                                availability = 1
                                if len(dateAppo) == 2:
                                    if opeTime >= int(dateAppo[1]['I']) and opeTime <= int(dateAppo[1]['F'])-1:
                                        checkDisp = searchHours(str(opeTime).rjust(2,'0')+':'+item['SKID'].replace('HR#','')[3:5], hoursData)
                                        if checkDisp == '':
                                            availability = 1
                                        else: 
                                            actHour = searchHours(str(checkHours).rjust(2,'0')+':'+item['SKID'].replace('HR#','')[3:5], hoursData)
                                            if item['SERVICEID'] == checkDisp['ServiceId'] and checkDisp['Available'] >= actHour['Available']:
                                                availability = 1
    
                            if availability == 0:
                                actHour = searchHours(str(checkHours).rjust(2,'0')+':'+item['SKID'].replace('HR#','')[3:5], hoursData)
                                checkHours = str(checkHours-1).rjust(2,'0')+':'+item['SKID'].replace('HR#','')[3:5]
                                searchHour = searchHours(checkHours, hoursData)
                                if searchHour != '':
                                    hoursData.remove(actHour)
                                    actHour['Available'] = searchHour['Available']
                                    hoursData.append(actHour)
                                    
                    for item in dateAppo:
                        ini = Decimal(item['I'])
                        fin = Decimal(item['F'])
                        scale = 10
                        for h in range(int(scale*ini), int(scale*fin), int(scale*bucket)):
                            if (h/scale).is_integer():
                                hStd = str(math.trunc(h/scale)).zfill(2) + ':00'
                                res = math.trunc(h/scale) if math.trunc(h/scale) < 13 else math.trunc(h/scale)-12
                                h = str(res).zfill(2) + ':00 ' + 'AM' if math.trunc(h/scale) < 13 else str(res).zfill(2) + ':00 ' + 'PM'
                            else:
                                hStd = str(math.trunc(h/scale)).zfill(2) + ':30'
                                res = math.trunc(h/scale) if math.trunc(h/scale) < 13 else math.trunc(h/scale)-12
                                h = str(res).zfill(2) + ':30 ' + 'AM' if math.trunc(h/scale) < 13 else  str(res).zfill(2) + ':30 ' + 'PM'
                            available = numCustomer
                            for x in hoursData:
                                if x['Hour'] == hStd:
                                    available = int(x['Available'])
                                    break
                            if int(available) > 0:
                                if currHour != '':
                                    if int(hStd.replace(':','')) > int(currHour.replace(':','')):
                                        recordset = {
                                            'Hour': h,
                                            'Available': available
                                        }
                                        hours.append(recordset)
                                else:
                                    recordset = {
                                        'Hour': h,
                                        'Available': available
                                    }
                                    hours.append(recordset)

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