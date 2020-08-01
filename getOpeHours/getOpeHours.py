import sys
import logging
import json

from decimal import *
import math
import datetime
import dateutil.tz
from datetime import timezone

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
        businessId = event['pathParameters']['businessId']
        locationId = event['pathParameters']['locationId']
        providerId = event['pathParameters']['providerId']
        monday = datetime.datetime.strptime(event['pathParameters']['initDay'], '%Y-%m-%d')

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
            bucket = row['CUSTOMER_PER_BUCKET'] if 'CUSTOMER_PER_BUCKET' in row else 0
            daysOff = row['DAYS_OFF'] if 'DAYS_OFF' in row else []
            interval = row['BUCKET_INTERVAL'] if 'BUCKET_INTERVAL' in row else 0 
            opeHours = json.loads(row['OPERATIONHOURS'])

            x = range(0,7)
            minVal = 24
            maxVal = 0
            for n in x:
                dayOffValid = True
                nextDate = monday + datetime.timedelta(days=n)
                dayName = nextDate.strftime("%A")[0:3].upper()
                dayHours = opeHours[dayName] if dayName in opeHours else ''
                if daysOff != []:
                    dayOffValid = nextDate.strftime("%Y-%m-%d") not in daysOff

                if dayOffValid == True:
                    getAvailability = dynamodb.query(
                        TableName="TuCita247",
                        ReturnConsumedCapacity='TOTAL',
                        KeyConditionExpression='PKID = :usedData',
                        ExpressionAttributeValues={
                            ':usedData': {'S': 'LOC#'+locationId+'#DT#'+nextDate.strftime("%Y-%m-%d")}
                        }
                    )
                    hoursData = []
                    for item in json_dynamodb.loads(getAvailability['Items']):
                        recordset = {
                            'Time': item['SKID'].replace('HR#','').replace('-',':'),
                            'Bucket': bucket,
                            'Available': item['AVAILABLE'],
                            'Used': +bucket-int(item['AVAILABLE'])
                        }
                        hoursData.append(recordset)

                    ini = 0
                    fin = 0
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
                                    'Bucket': bucket,
                                    'Available': bucket,
                                    'Used': 0
                                }
                            else:
                                record = findHours(h, hoursData)
                                h = record['Time']
                                if int(h[0:2]) > 12:
                                    h = str(int(h[0:2])-12) + h[2:5] + ' PM'
                                else:
                                    h = h + ' AM'
                                recordset = {
                                    'Time': h,
                                    'Bucket': record['Bucket'],
                                    'Available': record['Available'],
                                    'Used': record['Used']
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
            body = json.dumps({'Message': 'Error on get purpose', 'Code': 500})
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