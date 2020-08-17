import sys
import logging
import json

import boto3
import botocore.exceptions 
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
from dynamodb_json import json_util as json_dynamodb

from decimal import *
import datetime
import dateutil.tz
from datetime import timezone

import uuid
import string
import math
import random

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

def searchHours(time, hours):
    for item in hours:
        if item['Hour'] == time:
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
        letters = string.ascii_uppercase + string.digits
        data = json.loads(event['body'])
        businessId = data['BusinessId']
        locationId = data['LocationId']
        providerId = data['ProviderId']
        serviceId = data['ServiceId']
        door = data['Door'] if 'Door' in data else ''
        phone = data['Phone']
        name = data['Name']
        email = data['Email'] if 'Email' in data else ''
        dob = data['DOB'] if 'DOB' in data else ''
        gender = data['Gender'] if 'Gender' in data else ''
        preference = data['Preference'] if 'Preference' in data else ''
        disability = data['Disability'] if 'Disability' in data else ''
        guests = data['Guests']
        customerId = str(uuid.uuid4()).replace("-","")
        status = data['Status'] if 'Status' in data else 0
        appoDate = datetime.datetime.strptime(data['AppoDate'], '%Y-%m-%d') if 'AppoDate' in data else ''
        hourDate = data['AppoHour'] if 'AppoHour' in data else ''
        typeCita = data['Type'] if 'Type' in data else ''
        dateAppointment = appoDate.strftime("%Y-%m-%d") + '-' + data['AppoHour'].replace(':','-')
        existe = 0
        opeHours = ''
        daysOff = []
        dateAppo = '' 
        qrCode = 'VALID' if typeCita == 2 else ''.join(random.choice(letters) for i in range(6))

        country_date = dateutil.tz.gettz('America/Puerto_Rico')
        today = datetime.datetime.now(tz=country_date)
        dateOpe = today.strftime("%Y-%m-%d-%H-%M-%S")

        dayName = appoDate.strftime("%A")[0:3].upper()
        dateIni= today.strftime("%Y-%m-%d")
        dateFin = today + datetime.timedelta(days=90)
        dateFin = dateFin.strftime("%Y-%m-%d")

        statusPlan = 0
        numberAppos = 0
        bucket = 0
        result = {}
        hoursData = []
        hours = []
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
                    KeyConditionExpression = 'PKID = :businessId and SKID = :key',
                    ExpressionAttributeValues = {
                        ':businessId': {'S': 'BUS#' + businessId + '#LOC#' + locationId},
                        ':key': {'S': 'PRO#' + providerId}
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
                            ':key': {'S': 'LOC#' + locationId + '#PRO#' + providerId + '#DT#' + appoDate.strftime("%Y-%m-%d")}
                        }
                    )
                    hoursData = []
                    for row in json_dynamodb.loads(getCurrHours['Items']):
                        if (int(row['TIME_SERVICE']) > 1):
                            times = range(0, row['TIME_SERVICE'])
                            availableAppo = 1
                            count = 0
                            for hr in times:
                                newTime = str(int(row['SKID'].replace('HR#','')[0:2])+hr)
                                time24hr = newTime.rjust(2,'0')+'-'+row['SKID'].replace('HR#','')[3:5]
                                newTime = str(newTime.zfill(2))+':'+row['SKID'].replace('HR#','')[3:5]

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
                                for item in json_dynamodb.loads(getAppos['Items']):
                                    if item['PKID'] != '':
                                        count = count +1

                                recordset = {
                                    'Hour': newTime,
                                    'TimeService': row['TIME_SERVICE'],
                                    'Available': row['CUSTOMER_PER_TIME']-count,
                                    'ServiceId': row['SERVICEID'],
                                    'Start': 1 if hr == 0 else 0
                                }
                                
                                if row['CUSTOMER_PER_TIME']-count == 0:
                                    availableAppo = 0

                                timeExists = searchHours(newTime, hoursData)
                                if timeExists == '':
                                    hoursData.append(recordset)
                            
                            if availableAppo == 0:
                                for hr in times:
                                    newTime = str(int(row['SKID'].replace('HR#','')[0:2])+hr)
                                    newTime = newTime.rjust(2,'0')+':'+row['SKID'].replace('HR#','')[3:5]

                                    recordset = {
                                        'Hour': newTime,
                                        'TimeService': row['TIME_SERVICE'],
                                        'Available': 0,
                                        'ServiceId': row['SERVICEID'],
                                        'Start': 1 if hr == 0 else 0
                                    }
                                    timeExists = searchHours(newTime, hoursData)
                                    if timeExists != '':
                                        hoursData.remove(timeExists)
                                        hoursData.append(recordset)
                        else:
                            recordset = {
                                'Hour': row['SKID'].replace('HR#','').replace('-',':'),
                                'TimeService': row['TIME_SERVICE'],
                                'Available': row['AVAILABLE'],
                                'ServiceId': row['SERVICEID'],
                                'Start': 1
                            }
                            hoursData.append(recordset)

                    validAppo = 0
                    existe = 0
                    notAvailable = 0
                    y = range(0, bucket)
                    for z in y:
                        locTime = str(int(hourDate[0:2])+z).zfill(2)+':'+str(hourDate[3:5])
                        hrArr, start, available, ser = findHours(locTime, hoursData)
                        if hrArr != '':
                            if ser == serviceId or ser == '':
                                validAppo = 1
                                if z == 0:
                                    existe = start
                                notAvailable = 0 if available > 0 else 1
                                if notAvailable == 1:
                                    break
                            else:
                                validAppo = 0
                                notAvailable = 1
                                break
                        else:
                            validAppo = 0
                            for item in dateAppo:
                                ini = Decimal(item['I'])
                                fin = Decimal(item['F'])
                                if int(locTime[0:2]) >= ini and int(locTime[0:2]) < fin:
                                    if numCustomer > 0:
                                        validAppo = 1
                                        if z == 0:
                                            existe = 0
                                        break
                                    else:
                                        notAvailable = 1
                                        validAppo = 0
                                        break
                            if validAppo == 0:
                                break

                #PROCEDE A GUARDAR LA CITA
                if validAppo == 1:
                    existePhone = 0
                    if phone != '00000000000':
                        # SEARCH FOR PHONE NUMBER
                        getPhone = dynamodb.query(
                            TableName = "TuCita247",
                            ReturnConsumedCapacity = 'TOTAL',
                            KeyConditionExpression = 'PKID = :phone',
                            ExpressionAttributeValues = {
                                ':phone': {'S': 'MOB#' + phone}
                            }
                        )
                        for phoneNumber in json_dynamodb.loads(getPhone['Items']):
                            existePhone = 1
                            customerId = phoneNumber['SKID'].replace('CUS#','')
                            name = (phoneNumber['NAME'] if name == "" else name)
                            email = (phoneNumber['EMAIL'] if 'EMAIL' in phoneNumber and email == '' else email)
                            dob = (phoneNumber['DOB'] if 'DOB' in phoneNumber and dob == '' else dob)
                            gender = (phoneNumber['GENDER'] if 'GENDER' in phoneNumber and gender == '' else gender)
                            preference = (phoneNumber['PREFERENCES'] if 'PREFERENCES' in phoneNumber and preference == '' else preference)
                            disability = (phoneNumber['DISABILITY'] if 'DISABILITY' in phoneNumber and disability == '' else disability)

                    recordset = {}
                    items = []
                    if existePhone == 0:
                        recordset = {
                            "Put": {
                                "TableName": "TuCita247",
                                "Item": {
                                    "PKID": {"S": 'MOB#' + phone}, 
                                    "SKID": {"S": 'CUS#' + customerId}, 
                                    "STATUS": {"N": "1"}, 
                                    "NAME": {"S": name}, 
                                    "EMAIL": {"S":  email if email != '' else None },
                                    "DOB": {"S": dob if dob != '' else None },
                                    "DISABILITY": {"N": disability if disability != '' else None},
                                    "GENDER": {"S": gender if gender != '' else None},
                                    "PREFERENCES": {"N": str(preference) if str(preference) != '' else None},
                                    "GSI1PK": {"S": "CUS#" + customerId}, 
                                    "GSI1SK": {"S": "CUS#" + customerId}
                                },
                                "ConditionExpression": "attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                                "ReturnValuesOnConditionCheckFailure": "ALL_OLD" 
                                }
                            }
                        # logger.info(cleanNullTerms(recordset))
                        items.append(cleanNullTerms(recordset))

                        if phone != '00000000000':
                            recordset = {
                            "Put": {
                                "TableName": "TuCita247",
                                "Item": {
                                    "PKID": {"S": 'MOB#' + phone},
                                    "SKID": {"S": 'MOB#' + phone}
                                },
                                "ConditionExpression": "attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                                "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
                                }
                            }
                            items.append(cleanNullTerms(recordset))
                    
                    appoId = str(uuid.uuid4()).replace("-","")
                    recordset = {}
                    recordset = {
                        "Put": {
                            "TableName": "TuCita247",
                            "Item": {
                                "PKID": {"S": 'APPO#'+appoId}, 
                                "SKID": {"S": 'APPO#'+appoId}, 
                                "STATUS": {"N": "1" if status == 0 else str(status)}, 
                                "NAME": {"S": name},
                                "DATE_APPO": {"S": dateAppointment},
                                "PHONE": {"S": phone},
                                "DOOR": {"S": door},
                                "ON_BEHALF": {"N": "0"},
                                "PEOPLE_QTY": {"N": str(guests) if str(guests) != '' else None},
                                "DISABILITY": {"N": str(disability) if str(disability) != '' else None},
                                "QRCODE": {"S": qrCode},
                                "TYPE": {"N": "2" if qrCode == 'VALID' else "1"},
                                "TIMECHECKIN": {"S": str(dateOpe) if status == 3 else None},
                                "SERVICEID": {"S": serviceId},
                                "GSI1PK": {"S": 'BUS#' + businessId + '#LOC#' + locationId + '#PRO#' + providerId}, 
                                "GSI1SK": {"S": ('1' if status == 0 else str(status)) + '#DT#' + dateAppointment}, 
                                "GSI2PK": {"S": 'CUS#' + customerId},
                                "GSI2SK": {"S": '5#' if str(status) == '5' else str(status) + '#DT#' + dateAppointment},
                                "GSI3PK": {"S": 'BUS#' + businessId + '#LOC#' + locationId + '#' + dateAppointment[0:10] if qrCode != 'VALID' else None}, 
                                "GSI3SK": {"S": 'QR#' + qrCode if qrCode != 'VALID' else None},
                                "GSI4PK": {"S": 'BUS#' + businessId + '#LOC#' + locationId if status == 3 else None},
                                "GSI4SK": {"S": str(status) + "#DT#" + str(dateAppointment) + "#" + appoId if status == 3 else None},
                                "GSI5PK": {"S": 'BUS#' + businessId if status == 3 else None},
                                "GSI5SK": {"S": dateAppointment[0:10] + '#APPO#' + appoId if status == 3 else None},
                                "GSI6PK": {"S": 'BUS#' + businessId + '#LOC#' + locationId if status == 3 else None},
                                "GSI6SK": {"S": dateAppointment[0:10] + '#APPO#' + appoId if status == 3 else None},
                                "GSI7PK": {"S": 'BUS#' + businessId + '#LOC#' + locationId + '#PRO#' + providerId if status == 3 else None},
                                "GSI7SK": {"S": dateAppointment[0:10] + '#APPO#' + appoId if status == 3 else None},
                            },
                            "ConditionExpression": "attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                            "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
                            }
                        }
                    # logger.info(cleanNullTerms(recordset))
                    items.append(cleanNullTerms(recordset))

                    if qrCode != 'VALID':
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
                        # logger.info(recordset)
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
                    
                    # logger.info(recordset)
                    items.append(recordset)

                    if existe == 1:
                        #update
                        recordset = {
                            "Update":{
                                "TableName": "TuCita247",
                                "Key": {
                                    "PKID": {"S": 'LOC#' + locationId + '#PRO#' + providerId + '#DT#' + dateAppointment[0:10]}, 
                                    "SKID": {"S": 'HR#'+data['AppoHour'].replace(':','-')}, 
                                    "TIME_SERVICE": {"N": str(bucket)},
                                    "CUSTOMER_PER_TIME": {"N": str(numCustomer)},
                                    "SERVICEID": {"S": serviceId}
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
                        #put
                        recordset = {
                        "Put": {
                            "TableName": "TuCita247",
                            "Item": {
                                "PKID": {"S": 'LOC#' + locationId + '#PRO#' + providerId + '#DT#' + dateAppointment[0:10]}, 
                                "SKID": {"S": 'HR#'+data['AppoHour'].replace(':','-')},
                                "TIME_SERVICE": {"N": str(bucket)},
                                "CUSTOMER_PER_TIME": {"N": str(numCustomer)},
                                "SERVICEID": {"S": serviceId},
                                "AVAILABLE": {"N": str(numCustomer-1)}
                            },
                            "ConditionExpression": "attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                            "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
                            }
                        }
                    # logger.info(recordset)
                    items.append(recordset)

                    if status == 3:
                        recordset = {
                            "Update": {
                                "TableName": "TuCita247",
                                "Key": {
                                    "PKID": {"S": 'BUS#' + businessId}, 
                                    "SKID": {"S": 'LOC#' + locationId}, 
                                },
                                "UpdateExpression": "SET PEOPLE_CHECK_IN = PEOPLE_CHECK_IN + :increment",
                                "ExpressionAttributeValues": { 
                                    ":increment": {"N": str(guests)}
                                },
                                "ConditionExpression": "attribute_exists(PKID) AND attribute_exists(SKID)",
                                "ReturnValuesOnConditionCheckFailure": "ALL_OLD" 
                            }
                        }
                        items.append(recordset)

                        recordset = {
                            "Update": {
                                "TableName": "TuCita247",
                                "Key": {
                                    "PKID": {"S": 'BUS#' + businessId + '#LOC#' + locationId}, 
                                    "SKID": {"S": 'PRO#' + providerId}, 
                                },
                                "UpdateExpression": "SET PEOPLE_CHECK_IN = PEOPLE_CHECK_IN + :increment",
                                "ExpressionAttributeValues": { 
                                    ":increment": {"N": str(guests)}
                                },
                                "ConditionExpression": "attribute_exists(PKID) AND attribute_exists(SKID)",
                                "ReturnValuesOnConditionCheckFailure": "ALL_OLD" 
                            }
                        }
                        items.append(recordset)
                    
                    logger.info(items)
                    response = dynamodb.transact_write_items(
                        TransactItems = items
                    )

                    appoInfo = {
                        'AppId': appoId,
                        'ClientId': customerId,
                        'Name': name,
                        'Phone': phone,
                        'OnBehalf': 0,
                        'Guests': 0 if guests == '' else int(guests),
                        'Door': door,
                        'Disability': 0 if disability == '' else int(disability),
                        'DateFull': dateAppointment,
                        'DateAppo': str(int(today.strftime("%H"))-12).rjust(2,'0') + ':00 PM' if int(today.strftime("%H")) > 12 else today.strftime("%H").rjust(2,'0') + ':00 AM'
                    }

                    statusCode = 200
                    body = json.dumps({'Message': 'Appointment added successfully', 'Code': 200, 'Appointment': appoInfo})
                else:
                    statusCode = 404
                    body = json.dumps({'Message': 'Invalid date and time', 'Code': 400})

        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Message': 'Error !!!', 'Code': 400})

    except ClientError as error:
        statusCode = 500
        body = json.dumps({'Message': str(error), 'Code': 400})

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