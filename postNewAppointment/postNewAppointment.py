import sys
import logging
import json

import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr
from dynamodb_json import json_util as json_dynamodb

import string
import random

from decimal import *

import datetime
import dateutil.tz
from datetime import timezone

import uuid
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

def lambda_handler(event, context):
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']
        
    try:
        data = json.loads(event['body'])
        businessId = data['BusinessId']
        locationId = data['LocationId']
        door = data['Door']
        phone = data['Phone']
        name = data['Name']
        email = data['Email']
        dob = data['DOB']
        gender = data['Gender']
        preference = data['Preference']
        disability = data['Disability']
        companions = data['Companions']
        customerId = str(uuid.uuid4()).replace("-","")
        existe = 0
        opeHours = ''
        daysOff = []
        dateAppo = '' 

        letters = string.ascii_uppercase
        qrCode = ''.join(random.choice(letters) for i in range(10))

        country_date = dateutil.tz.gettz('America/Puerto_Rico')
        today = datetime.datetime.now(tz=country_date)
        dayName = today.strftime("%A")[0:3].upper()

        getCurrDate = dynamodb.query(
            TableName = "TuCita247",
            ReturnConsumedCapacity = 'TOTAL',
            KeyConditionExpression = 'PKID = :businessId and begins_with ( SKID , :locationId ) ',
            ExpressionAttributeValues = {
                ':businessId': {'S': 'BUS#' + businessId},
                ':locationId': {'S': 'LOC#' + locationId}
            },
            Limit = 1
        )
        for currDate in json_dynamodb.loads(getCurrDate['Items']):
            periods = []
            opeHours = json.loads(currDate['OPERATIONHOURS'])
            daysOff = currDate['DAYS_OFF'].split(',')
            dateAppo = opeHours[dayName] if dayName in opeHours else ''

            dayOffValid = today.strftime("%Y-%m-%d") not in daysOff
            periods = dateAppo
            for item in periods:
                ini = Decimal(item['I'])
                fin = Decimal(item['F'])
                currHour = Decimal(today.strftime("%H"))
                if  currHour >= ini and currHour <= fin:
                    dateAppo = today.strftime("%Y-%m-%d") + '-' + today.strftime("%H").ljust(2,'0')  + '-00'
                    break
                    
        if dayOffValid == False:
            statusCode = 500
            body = json.dumps({'Message': 'Date is not valid', 'Code': 400})
        else:
            if phone != '0000000000':
                # SEARCH FOR PHONE NUMBER
                getPhone = dynamodb.query(
                    TableName = "TuCita247",
                    ReturnConsumedCapacity = 'TOTAL',
                    KeyConditionExpression = 'PKID = :phone',
                    ExpressionAttributeValues = {
                        ':phone': {'S': 'MOB#' + phone}
                    },
                    Limit = 1
                )
                for phoneNumber in json_dynamodb.loads(getPhone['Items']):
                    existe = 1
                    customerId = phoneNumber['SKID'].replace('CUS#','')
                    name = (phoneNumber['NAME'] if name == "" else name)
                    email = (phoneNumber['EMAIL'] if email == "" else email)
                    dob = (phoneNumber['DOB'] if dob == "" else dob)
                    gender = (phoneNumber['GENDER'] if gender == "" else gender)
                    preference = (phoneNumber['PREFERENCES'] if preference == "" else preference)
                    disability = (phoneNumber['DISABILITY'] if disability == "" else disability)
            
            recordset = {}
            items = []
            if existe == 0:
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
                            "GENDER": {"S": gender if gender != '' else None},
                            "PREFERENCES": {"N": str(preference) if str(preference) != '' else None},
                            "GSI1PK": {"S": "CUS#TOT"}, 
                            "GSI1SK": {"S": name + '#' + customerId}
                        },
                        "ConditionExpression": "attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                        "ReturnValuesOnConditionCheckFailure": "ALL_OLD" 
                        }
                    }
                logger.info(cleanNullTerms(recordset))
                items.append(cleanNullTerms(recordset))
            
            appoId = str(uuid.uuid4()).replace("-","")
            recordset = {}
            recordset = {
                "Put": {
                    "TableName": "TuCita247",
                    "Item": {
                        "PKID": {"S": 'APPO#'+appoId}, 
                        "SKID": {"S": 'APPO#'+appoId}, 
                        "STATUS": {"N": "1"}, 
                        "NAME": {"S": name}, 
                        "GSI1PK": {"S": 'BUS#' + businessId + '#LOC#' + locationId}, 
                        "GSI1SK": {"S": '1#DT#' + dateAppo}, 
                        "DATE_APPO": {"S": dateAppo}, 
                        "GSI2PK": {"S": 'CUS#' + customerId},
                        "GSI2SK": {"S": '1#DT#' + dateAppo},
                        "PHONE": {"S": phone},
                        "DOOR": {"S": door},
                        "ON_BEHALF": {"N": "0"},
                        "PEOPLE_QTY": {"N": str(companions) if str(companions) != '' else None},
                        "DISABILITY": {"N": disability if disability != '' else None},
                        "QRCODE": {"S": qrCode if phone != '0000000000' else None},
                        "TYPE": {"N": "2"}
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
            appoInfo = {
                'AppId': appoId,
                'ClientId': customerId,
                'Name': name,
                'Phone': phone,
                'OnBehalf': 0,
                'Companions': 0 if companions == '' else int(companions),
                'Door': door,
                'Disability': 0 if disability == '' else int(disability),
                'DateFull': dateAppo,
                'DateAppo': dateAppo[-5:].replace('-',':')
            }

            statusCode = 200
            body = json.dumps({'Message': 'Appointment added successfully', 'Code': 200, 'Appointment': appoInfo})
        
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