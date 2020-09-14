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

import string

import os

REGION = 'us-east-1'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.client('dynamodb', region_name=REGION)
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']
        
    try:
        businessId = event['pathParameters']['businessId']

        country_date = dateutil.tz.gettz('America/Puerto_Rico')
        today = datetime.datetime.now(tz=country_date)

        dateIni= today.strftime("%Y-%m-%d")
        dateFin = today + datetime.timedelta(days=90)
        dateFin = dateFin.strftime("%Y-%m-%d")

        statusPlan = 0
        numberAppos = 0
        statusCode = ''

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
            statusCode = 200
            body = json.dumps({'Message': 'Disabled plan', 'Code': 404})
        else:
            if numberAppos == 0:
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
                        break

            #SIN DISPONIBILIDAD DE CITAS
            if numberAppos == 0:
                statusCode = 200
                body = json.dumps({'Message': 'No appointments available', 'Code': 400})
            else:
                statusCode = 200
                body = json.dumps({'Message': 'Appos available', 'Code': 200})

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