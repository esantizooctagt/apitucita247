import sys
import logging
import json

import boto3
import botocore.exceptions
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
from dynamodb_json import json_util as json_dynamodb

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
        order = event['pathParameters']['order']
        typeCancel = int(event['pathParameters']['typeCancel'])
        respCode = int(event['pathParameters']['respCode'])
        plan = event['pathParameters']['plan']
 
        language = ''
        email = ''
        phone = ''
        name = ''
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

        if typeCancel == 2:
            items = []
            rows = {}
            rows = {
                "Update": {
                    "TableName": "TuCita247",
                    "Key": {
                        "PKID": {"S": 'BUS#' + businessId },
                        "SKID": {"S": 'PLAN' }
                    },
                    "UpdateExpression":"SET #o = :order, #s = :status",
                    "ExpressionAttributeNames":{'#o': 'ORDER','#s': 'STATUS'},
                    "ExpressionAttributeValues": {
                        ":order": {"S": order},
                        ":status": {"N": str(0)}
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
                    "UpdateExpression":"set #s = :status",
                    "ExpressionAttributeValues": { 
                        ":status": {"N": str(0)}
                    },
                    "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
                },
            }
            items.append(rows)

            logger.info(items)
            response = dynamodb.transact_write_items(
                TransactItems = items
            )
        
        if respCode == 1:
            SENDER = "Tu Cita 24/7 <no-reply@tucita247.com>"
            SUBJECT = "Tu Cita 24/7 - Payment failed" if language == 'en' else "Tu Cita 24/7 - Pago fallido"
            if language == 'en':
                BODY_TEXT = ("Payment failed. Business: "+name+" Email: "+email+" Phone number: "+phone+" Plan: "+plan)
            else:
                BODY_TEXT = ("Pago fallido. Negocio: "+name+" Correo electrónico: "+email+" Teléfono: "+phone+" Plan: "+plan)
            if language == 'en':
                BODY_HTML = """<html>
                <head></head>
                <body>
                <h1>Payment failed</h1>
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
                <h1>Pago fallido</h1>
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