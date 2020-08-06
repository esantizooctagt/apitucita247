import sys
import logging
import json

from decimal import *
import math
import datetime
import dateutil.tz
from datetime import timezone, timedelta

import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr
from dynamodb_json import json_util as json_dynamodb

import os

REGION = 'us-east-1'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.client('dynamodb', region_name='us-east-1')
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def findHours(time, hours):
    for item in hours:
        if item['Time'] == time:
            return item
        # if int(item['TimeService']) < interval:
        #     newTime = datetime.datetime.strptime(time, '%H:%M') + timedelta(hours=interval)
        #     newTime = newTime.strftime("%H:%M")
        #     if item['Time'] >= time and item['Time'] <= newTime:
        #         return {'Time': time, 'TimeService': item['TimeService'], 'Bucket': 0, 'Available': 0, 'Used': 0}
    item = ''
    return item

def lambda_handler(event, context):
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']

    try:
        statusCode = ''
        # interval = 0
        bucket = 0
        businessId = event['pathParameters']['businessId']
        locationId = event['pathParameters']['locationId']
        providerId = event['pathParameters']['providerId']
        # serviceId = event['pathParameters']['serviceId']
        monday = datetime.datetime.strptime(event['pathParameters']['initDay'], '%Y-%m-%d')
        
        # #GET SERVICES 
        # service = dynamodb.query(
        #     TableName="TuCita247",
        #     ReturnConsumedCapacity='TOTAL',
        #     KeyConditionExpression='PKID = :businessId AND begins_with(SKID , :serviceId)',
        #     ExpressionAttributeValues={
        #         ':businessId': {'S': 'BUS#'+businessId},
        #         ':serviceId': {'S': 'SER#'}
        #     }
        # )
        # services = []
        # serv = {}
        # for serv in json_dynamodb.loads(service['Items']):
        #     serv = { 
        #         'interval': serv['TIME_SERVICE'],
        #         'bucket': serv['CUSTOMER_PER_TIME']
        #     }
        #     services.append(serv)

        # if interval == 0:
        #     statusCode = 500
        #     body = json.dumps({'Message': 'No data for this service provider', 'Code': 500})
        #     return
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
        for row in json_dynamodb.loads(response['Items']):
            # bucket = row['CUSTOMER_PER_BUCKET'] if 'CUSTOMER_PER_BUCKET' in row else 0
            daysOff = row['DAYS_OFF'] if 'DAYS_OFF' in row else []
            # interval = row['BUCKET_INTERVAL'] if 'BUCKET_INTERVAL' in row else 0 
            opeHours = json.loads(row['OPERATIONHOURS'])

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
                    getAvailability = dynamodb.query(
                        TableName="TuCita247",
                        ReturnConsumedCapacity='TOTAL',
                        KeyConditionExpression='PKID = :usedData',
                        ExpressionAttributeValues={
                            ':usedData': {'S': 'LOC#'+locationId+'#PRO#'+providerId+'#DT#'+nextDate.strftime("%Y-%m-%d")}
                        }
                    )
                    hoursData = []
                    for item in json_dynamodb.loads(getAvailability['Items']):
                        if (int(item['TIME_SERVICE']) > 1):
                            times = range(0, item['TIME_SERVICE'])
                            for hr in times:
                                newTime = str(int(item['SKID'].replace('HR#','')[0:2])+hr)
                                newTime = newTime.rjust(2,'0')+':'+item['SKID'].replace('HR#','')[3:5]
                                recordset = {
                                    'Time': newTime,
                                    'TimeService': item['TIME_SERVICE'],
                                    'ServiceId': item['SERVICEID'],
                                    'Bucket': bucket,
                                    'Available': item['AVAILABLE'],
                                    'Used': +bucket-int(item['AVAILABLE'])
                                }
                                hoursData.append(recordset)
                        else:
                            recordset = {
                                'Time': item['SKID'].replace('HR#','').replace('-',':'),
                                'TimeService': item['TIME_SERVICE'],
                                'ServiceId': item['SERVICEID'],
                                'Bucket': bucket,
                                'Available': item['AVAILABLE'],
                                'Used': +bucket-int(item['AVAILABLE'])
                            }
                            hoursData.append(recordset)
                    
                    ini = 0
                    fin = 0
                    interval = 1
                    for dt in dayHours:
                        ini = Decimal(dt['I'])
                        fin = Decimal(dt['F'])
                        scale = 10
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
                                    h = str(int(h[0:2])-12) + h[2:5] + ' PM'
                                else:
                                    h = h + ' AM'
                                recordset = {
                                    'Time': h,
                                    # 'Bucket': 1,
                                    'Available': 1,
                                    'Used': 0
                                }
                            else:
                                record = findHours(h, hoursData)
                                h = record['Time']
                                if int(h[0:2]) > 12:
                                    h = str(int(h[0:2])-12) + h[2:5] + ' PM'
                                else:
                                    h = h + ' AM'
                                # if record['TimeService'] == interval:
                                recordset = {
                                    'Time': h,
                                    # 'Bucket': bucket, #record['Bucket'],
                                    'Available': record['Available'],
                                    'Used': record['Used']
                                }
                                # else:
                                #     recordset = {
                                #         'Time': h,
                                #         # 'Bucket': bucket,
                                #         'Available': 0,
                                #         'Used': 0
                                #     }
                            
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
                    h = str(int(h24[0:2])-12) + h24[2:5] + ' PM'
                else:
                    h = h24 + ' AM'
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