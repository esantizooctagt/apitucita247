import sys
import logging
import json

import boto3
import botocore.exceptions
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
from dynamodb_json import json_util as json_dynamodb

import datetime
import dateutil.tz
from datetime import timezone

import os

REGION = 'us-east-1'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.client('dynamodb', region_name=REGION)
ses = boto3.client('ses',region_name=REGION)
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):
    stage = event['headers']
    cors = stage['origin']
        
    try:
        businessId = event['pathParameters']['businessId']
        plan = event['pathParameters']['plan']
        appos = event['pathParameters']['appos']
        order = event['pathParameters']['order']
        total = float(event['pathParameters']['total'])
        subId = event['pathParameters']['subId']

        dueDate = ''
        language = ''
        email = ''
        phone = ''
        name = ''
        country_date = dateutil.tz.gettz('America/Puerto_Rico')
        today = datetime.datetime.now(tz=country_date)
        dueDate = (today + datetime.timedelta(days=31)).strftime("%Y-%m-%d")
        
        lanData = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :pkid AND SKID = :skid',
            ExpressionAttributeValues={
                ':pkid': {'S': 'BUS#' + businessId},
                ':skid': {'S': 'METADATA'}
            }
        )
        for lang in json_dynamodb.loads(lanData['Items']):
            language = lang['LANGUAGE'] if 'LANGUAGE' in lang else 'es'
            email = lang['EMAIL']
            phone = lang['PHONE']
            name = lang['NAME']

        dataPlan = dynamodb.query(
            TableName = "TuCita247",
            ReturnConsumedCapacity = 'TOTAL',
            KeyConditionExpression = 'PKID = :businessId AND SKID = :key',
            ExpressionAttributeValues = {
                ':businessId': {'S': 'BUS#' + businessId},
                ':key': {'S': 'PLAN'}
            },
            Limit = 1
        )
        expire = 0
        available = 0
        for data in json_dynamodb.loads(dataPlan['Items']):
            expire = int(data['EXPIRE']) if 'EXPIRE' in data else 1
            if expire == 0:
                available = int(data['AVAILABLE'])+int(appos)
            else:
                available = int(appos)

        items = []
        rows = {}
        rows = {
            "Update": {
                "TableName": "TuCita247",
                "Key": {
                    "PKID": {"S": 'BUS#' + businessId },
                    "SKID": {"S": 'PLAN' }
                },
                "UpdateExpression":"SET #n = :name, APPOINTMENTS = :appos, AVAILABLE = :available, #o = :order, #s = :status, GSI1PK = :duedate, DUE_DATE = :duedate, EXPIRE = :expire, SUBID = :subId",
                "ExpressionAttributeNames":{'#n': 'NAME','#o': 'ORDER','#s': 'STATUS'},
                "ExpressionAttributeValues": { 
                    ":name": {"S": plan},
                    ":appos": {"N": str(appos)},
                    ":order": {"S": order},
                    ":status": {"N": str(1)},
                    ":duedate": {"S": dueDate},
                    ":available": {"N": str(available)},
                    ":expire": {"N": str(1) if total == 0 else str(0)},
                    ":subId": {"N": str(subId)}
                },
                "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
            },
        }
        items.append(rows)

        rows = {
            "Update": {
                "TableName": "TuCita247",
                "Key": {
                    "PKID": {"S": 'BUS#' + businessId },
                    "SKID": {"S": 'METADATA' }
                },
                "UpdateExpression":"set GSI2PK = :gsi2pk",
                "ExpressionAttributeValues": { 
                    ":gsi2pk": {"S": "PLAN#"+plan} 
                },
                "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
            },
        }
        items.append(rows)

        logger.info(items)
        response = dynamodb.transact_write_items(
            TransactItems = items
        )

        SENDER = "Tu Cita 24/7 <no-reply@tucita247.com>"
        SUBJECT = "Tu Cita 24/7 - Successful payment" if language == 'en' else "Tu Cita 24/7 - Pago exitoso"
        if language == 'en':
            BODY_TEXT = ("Successful payment. Business: "+name+" Email: "+email+" Phone number: "+phone+" Plan: "+plan)
        else:
            BODY_TEXT = ("Pago exitoso. Negocio: "+name+" Correo electrónico: "+email+" Teléfono: "+phone+" Plan: "+plan)
        if language == 'en':
            BODY_HTML = """<html>
            <head></head>
            <body>
            <h1>Successful payment</h1>
            <p><strong>Business: </strong>""" +name+ """</p>
            <p><strong>Email: </strong>"""+email+"""</p>
            <p><strong>Phone number: </strong>""" +phone+ """</p>
            <p><strong>Plan: </strong>""" +plan+ """</p>
            </body>
            </html>"""
        else:
            BODY_HTML = """<html>
            <head></head>
            <body>
            <h1>Pago exitoso</h1>
            <p><strong>Negocio: </strong>""" +name+ """</p>
            <p><strong>Correo electrónico: </strong>"""+email+"""</p>
            <p><strong>Teléfono: </strong>""" +phone+ """</p>
            <p><strong>Plan: </strong>""" +plan+ """</p>
            </body>
            </html>"""
        
        CHARSET = "UTF-8"
        response = ses.send_email(
            Destination={
                'ToAddresses': [
                    email
                ],
                'BccAddresses': [
                    'rita@lutorio.com', 'carlos@lutorio.com', 'esantizo@octagt.com'
                ],
            },
            Message={
                'Body': {
                    'Html': {
                        'Charset': CHARSET,
                        'Data': BODY_HTML,
                    },
                    'Text': {
                        'Charset': CHARSET,
                        'Data': BODY_TEXT,
                    },
                },
                'Subject': {
                    'Charset': CHARSET,
                    'Data': SUBJECT,
                },
            },
            Source=SENDER
        )

        statusCode = 200
        body = json.dumps({'Message': 'Business updated successfully', 'Code': 200})

        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Message': 'Error on update special day', 'Code': 500})
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