import sys
import logging
import json
import requests
import boto3
import botocore.exceptions
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
from dynamodb_json import json_util as json_dynamodb
# from woocommerce import API
import os

REGION = 'us-east-1'
KEY = os.environ['wpKey']
SECRET = os.environ['wpSecret']
CLOUDSEARCH = os.environ['cloudSearch']

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.client('dynamodb', region_name=REGION)
ses = boto3.client('ses', region_name=REGION)
search = boto3.client('cloudsearchdomain', endpoint_url=CLOUDSEARCH)
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']
        
    try:
        businessId = event['pathParameters']['businessId']
        sns = int(event['pathParameters']['sns'])
        value = int(event['pathParameters']['type'])

        response = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :businessId AND SKID = :metadata',
            ExpressionAttributeValues={
                ':businessId': {'S': 'BUS#' + businessId},
                ':metadata': {'S': 'PLAN'}
            }
        )
        subId = ''
        for item in json_dynamodb.loads(response['Items']):
            subId = str(item['SUBID']) if 'SUBID' in item else ''

        items = []
        rows = {}
        rows = {
            "Update": {
                "TableName": "TuCita247",
                "Key": {
                    "PKID": {"S": 'BUS#' + businessId },
                    "SKID": {"S": 'PLAN' }
                },
                "UpdateExpression":"SET #s = :status",
                "ExpressionAttributeNames":{'#s': 'STATUS'},
                "ExpressionAttributeValues": {
                    ":status": {"N": '0' if value == 1 else '2' if value == 2 else '1'}
                },
                "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
            },
        }
        items.append(rows)

        if value == 3:
            rows = {
                "Update": {
                    "TableName": "TuCita247",
                    "Key": {
                        "PKID": {"S": 'BUS#' + businessId },
                        "SKID": {"S": 'METADATA' }
                    },
                    "UpdateExpression":"set #s = :status, GSI4PK = :search, GSI4SK = :search",
                    "ExpressionAttributeNames":{'#s': 'STATUS'},
                    "ExpressionAttributeValues": { 
                        ":status": {"N": '1'},
                        ":search": {"S": 'SEARCH'}
                    },
                    "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
                },
            }
            items.append(rows)
        else:
            rows = {
                "Update": {
                    "TableName": "TuCita247",
                    "Key": {
                        "PKID": {"S": 'BUS#' + businessId },
                        "SKID": {"S": 'METADATA' }
                    },
                    "UpdateExpression":"set #s = :status",
                    "ExpressionAttributeNames":{'#s': 'STATUS'},
                    "ExpressionAttributeValues": { 
                        ":status": {"N": '0' if value == 1 else '2' if value == 2 else '1'}
                    },
                    "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
                },
            }
            items.append(rows)

        logger.info(items)
        response = dynamodb.transact_write_items(
            TransactItems = items
        )

        respEmail = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :businessId AND SKID = :metadata',
            ExpressionAttributeValues={
                ':businessId': {'S': 'BUS#' + businessId},
                ':metadata': {'S': 'METADATA'}
            }
        )
        email = ''
        language = 'en'
        for item in json_dynamodb.loads(respEmail['Items']):
            email = str(item['EMAIL']) if 'EMAIL' in item else ''
            language = str(item['LANGUAGE']) if 'LANGUAGE' in item else ''
            
        if value == 1:    
            #EMAIL
            SENDER = "Tu Cita 24/7 <no-reply@tucita247.com>"
            RECIPIENT = email
            data = ''
            subj = ''
            if language == 'en':
                LAN = 'EN'
                subj = 'Your account has been suspended'
                data = "<h3><strong>Hi! Your account has been temporarily suspended.</strong></h3><br><p>For more information, or if you think your account was suspended by mistake, please contact us at <a href='mailto:support@tucita247.com'>support@tucita247.com</a>.</p><br><p>Thank you for using tucita247.com!</p>"
            else:
                LAN = 'ES'
                subj = 'Su cuenta ha sido suspendida'
                data = "<h3><strong>¡Hola! Su cuenta ha sido suspendida.</strong></h3><br><p>Para más información, o si entiende que su cuenta se suspendió por error, comuníquese con nosotros a <a href='mailto:support@tucita247.com'>support@tucita247.com</a>.</p><br><p>¡Gracias por usar tucita247.com!</p>"

            logger.info("prev send email")
            response = ses.send_templated_email(
                Source=SENDER,
                Destination={
                    'ToAddresses': [
                        RECIPIENT,
                    ],
                },
                Template ='REACTIVATE_' + LAN, 
                TemplateData='{ "data": "'+ data +'", "subject" : "'+subj+'" }'
            )

        if value == 2:
            record = [{
                "type" : "delete",
                "id" : 'BUS#' + businessId
            }]
            response = search.upload_documents(
                documents=json.dumps(record),
                contentType='application/json'
            )

            #EMAIL
            SENDER = "Tu Cita 24/7 <no-reply@tucita247.com>"
            RECIPIENT = email
            data = ''
            subj = ''
            if language == 'en':
                LAN = 'EN'
                subj = 'Your account has been canceled'
                data = "<h3><strong>Hi! Your account has been canceled.</strong></h3><br><p>For more information, or if you think your account was canceled by mistake, please contact us at <a href='mailto:support@tucita247.com'>support@tucita247.com</a>.</p><br><p>Thank you for using tucita247.com!</p>"
            else:
                LAN = 'ES'
                subj = 'Su cuenta ha sido cancelada'
                data = "<h3><strong>¡Hola! Su cuenta ha sido cancelada.</strong></h3><br><p>Para más información, o si entiende que su cuenta se canceló por error, comuníquese con nosotros a <a href='mailto:support@tucita247.com'>support@tucita247.com</a>.</p><br><p>¡Gracias por usar tucita247.com!</p>"

            logger.info("prev send email")
            response = ses.send_templated_email(
                Source=SENDER,
                Destination={
                    'ToAddresses': [
                        RECIPIENT,
                    ],
                },
                Template ='REACTIVATE_' + LAN, 
                TemplateData='{ "data": "'+ data +'", "subject" : "'+subj+'" }'
            )

        if value == 3:
            #EMAIL
            SENDER = "Tu Cita 24/7 <no-reply@tucita247.com>"
            RECIPIENT = email
            data = ''
            subj = ''
            if language == 'en':
                LAN = 'EN'
                subj = 'Your account has been reactivated'
                data = "<h3><strong>Hi! Your account has been reactivated.</strong></h3><br><p>You can visit <a href='https://console.tucita247.com/en'>https://console.tucita247.com/en</a> and log in with your account.</p><p>For more information, please contact us at <a href='mailto:support@tucita247.com'>support@tucita247.com</a>.</p><br><p>Thank you for using tucita247.com!</p>"
            else:
                LAN = 'ES'
                subj = 'Su cuenta ha sido reactivada'
                data = "<h3><strong>¡Hola! Su cuenta ha sido reactivada.</strong></h3><br><p>Puede visitar <a href='https://console.tucita247.com/es'>https://console.tucita247.com/es</a> e ingresar con los datos de su cuenta.</p><p>Para más información, comuníquese con nosotros a <a href='mailto:support@tucita247.com'>support@tucita247.com</a>.</p><br><p>¡Gracias por usar tucita247.com!</p>"

            logger.info("prev send email")
            response = ses.send_templated_email(
                Source=SENDER,
                Destination={
                    'ToAddresses': [
                        RECIPIENT,
                    ],
                },
                Template ='REACTIVATE_' + LAN, 
                TemplateData='{ "data": "'+ data +'", "subject" : "'+subj+'" }'
            )

        if subId != '':
            url = 'https://tucita247.com/wp-json/wc/v1/subscriptions/'+subId
            params = dict(
                status='cancelled', 
                consumer_key=KEY,
                consumer_secret=SECRET
            )
            subscriptions = requests.put(url=url, params=params)

        if sns == 1:
            #EMAIL
            msg = "Cuenta cancelada"
            SENDER = "Tu Cita 24/7 <no-reply@tucita247.com>"
            # RECIPIENT = 'rita@lutorio.com', 'carlos@lutorio.com'
            SUBJECT = "Cuenta Cancelada - Tu Cita 24/7"
            BODY_TEXT = (msg)
                        
            # The HTML body of the email.
            BODY_HTML = """<html>
            <head></head>
            <body>
            <h1>Tu Cita 24/7</h1>
            <p>""" + msg + """</p>
            </body>
            </html>"""

            CHARSET = "UTF-8"

            response = ses.send_email(
                Destination={
                    'ToAddresses': [
                        'rita@lutorio.com', 'carlos@lutorio.com',
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