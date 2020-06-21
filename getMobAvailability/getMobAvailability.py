import sys
import logging
import json

import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr
from dynamodb_json import json_util as json_dynamodb

import string

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

def lambda_handler(event, context):
    try:
        statusCode = ''
        businessId = event['pathParameters']['businessId']
        locationId = event['pathParameters']['locationId']
        appoDate = datetime.strptime(event['pathParameters']['appoDate'], '%Y/%m/%d')

        country_date = dateutil.tz.gettz('America/Puerto_Rico')
        today = datetime.datetime.now(tz=country_date)
        dateOpe = today.strftime("%Y-%m-%d-%H-%M-%S")

        dayName = appoDate.strftime("%A")[0:3].upper()
        dateStd = appoDate.strftime("%Y-%m-%d")
        dateMonth = appoDate.strftime("%Y-%m")

        statusPlan = 0
        numberAppos = 0
        availablePackAppos = 0
        result = {}
        hours = []
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
            statusPlan = stat['STATUS']
            numberAppos = stat['APPOINTMENTS']

        if statusPlan == 0 or statusPlan == 2:
            statusCode = 404
            body = json.dumps({'Message': 'Disabled plan', 'Data': result, 'Code': 400})
        else:
            #CITAS DISPONIBLES PARA EL MES ENVIADO
            availableAppoPlan = dynamodb.query(
                TableName = "TuCita247",
                ReturnConsumedCapacity = 'TOTAL',
                KeyConditionExpression = 'PKID = :businessId AND SKID = :key',
                ExpressionAttributeValues = {
                    ':businessId': {'S': 'BUS#' + businessId},
                    ':key': {'S': 'APPOS#' + dateMonth}
                },
                Limit = 1
            )
            for avaiPlanAppo in json_dynamodb.loads(availableAppoPlan['Items']):
                numberAppos = avaiPlanAppo['AVAILABLE']
            #CITAS DISPONIBLES DE PAQUETES ADQUIRIDOS QUE VENCEN A FUTURO
            avaiAppoPack = dynamodb.query(
                TableName = "TuCita247",
                ReturnConsumedCapacity = 'TOTAL',
                KeyConditionExpression = 'PKID = :businessId AND SKID >= :key',
                ExpressionAttributeValues = {
                    ':businessId': {'S': 'BUS#' + businessId},
                    ':key': {'S': 'PACK#' + dateStd}
                }
            )
            availablePackAppos = 0
            for availablePlan in json_dynamodb.loads(avaiAppoPack['Items']):
                availablePackAppos = availablePackAppos + availablePlan['AVAILABLE']

            #ENTRA SI HAY CITAS DISPONIBLES YA SEA DE PLAN O PAQUETE VIGENTE
            if numberAppos > 0 or availablePackAppos > 0:
                #GET OPERATION HOURS FROM SPECIFIC LOCATION
                getCurrDate = dynamodb.query(
                    TableName = "TuCita247",
                    ReturnConsumedCapacity = 'TOTAL',
                    KeyConditionExpression = 'PKID = :businessId and begins_with ( SKID , :key ) ',
                    ExpressionAttributeValues = {
                        ':businessId': {'S': 'BUS#' + businessId},
                        ':key': {'S': 'LOC#' + locationId}
                    },
                    Limit = 1
                )
                for currDate in json_dynamodb.loads(getCurrDate['Items']):
                    periods = []
                    dayOffValid = True

                    opeHours = json.loads(currDate['OPERATIONHOURS'])
                    numCustomer = currDate['CUSTOMER_PER_BUCKET']
                    bucket = currDate['BUCKET_INTERVAL']
                    daysOff = currDate['DAYS_OFF'].split(',') if 'DAYS_OFF' in currDate else []
                    dateAppo = opeHours[dayName] if dayName in opeHours else ''
                    if daysOff != []:
                        dayOffValid = appoDate.strftime("%Y-%m-%d") not in daysOff
                        if dayOffValid == False:
                            statusCode = 500
                            body = json.dumps({'Message': 'Day Off', 'Data': [], 'Code': 400})
                            break
                    
                    #GET OPERATION HOURS FROM SPECIFIC LOCATION
                    getCurrHours = dynamodb.query(
                        TableName = "TuCita247",
                        ReturnConsumedCapacity = 'TOTAL',
                        KeyConditionExpression = 'PKID = :key AND SKID > :hours',
                        ExpressionAttributeValues = {
                            ':key': {'S': 'LOC#' + locationId + '#' + dateStd},
                            '::hours': {'S': '0'}
                        }
                    )
                    for row in json_dynamodb.loads(getCurrHours['Items']):
                        recordset = {
                            'Hour': row['SKID'].split('#')[1].replace('-',':'),
                            'Available': row['SKID'].split('#')[0]
                        }
                        hours.append(recordset)

                    if hours == []:
                        for item in dateAppo:
                            ini = Decimal(item['I'])
                            fin = Decimal(item['F'])
                            scale = 10
                            for h in range(int(scale*ini), int(scale*fin), int(scale*bucket)):
                                recordset = {
                                    'Hour': h/scale,
                                    'Available': numCustomer
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