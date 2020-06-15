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

from twilio.rest import Client
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

import os

REGION = 'us-east-1'

twilioAccountSID = os.environ['twilioAccountSID']
twilioAccountToken = os.environ['twilioAccountToken']
fromNumber = os.environ['fromNumber']

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
dynamodbQuery = boto3.client('dynamodb', region_name='us-east-1')
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']
        
    try:
        statusCode = ''
        data = json.loads(event['body'])
        appointmentId = event['pathParameters']['id']
        
        status = data['Status']
        dateAppo = data['DateAppo']
        reasonId = data['Reason'] if 'Reason' in data else ''
        customerId = data['CustomerId'] if 'CustomerId' in data else ''

        country_date = dateutil.tz.gettz('America/Puerto_Rico')
        today = datetime.datetime.now(tz=country_date)
        dateOpe = today.strftime("%Y-%m-%d-%H-%M-%S")

        table = dynamodb.Table('TuCita247')
        e = {'#s': 'STATUS'}
        if reasonId != '':
            v = {':status': status, ':key01': str(status) + '#DT#' + str(dateAppo), ':reason': reasonId, ':dateope': dateOpe}
        else:
            v = {':status': status, ':key01': str(status) + '#DT#' + str(dateAppo), ':dateope': dateOpe}
        
        response = table.update_item(
            Key={
                'PKID': 'APPO#' + appointmentId,
                'SKID': 'APPO#' + appointmentId
            },
            UpdateExpression="set #s = :status, GSI1SK = :key01, GSI2SK = :key01" + (", TIMECHEK = :dateope" if str(status) == "2" else "") + (", TIMECANCEL = :dateope" if str(status) == "5" else "") + (", REASONID = :reason" if reasonId != "" else ""),
            ExpressionAttributeNames=e,
            ExpressionAttributeValues=v,
            ReturnValues="UPDATED_NEW"
        )

        statusCode = 200
        body = json.dumps({'Message': 'Appointment updated successfully', 'Code': 200, 'Appo': json_dynamodb.loads(response['Attributes'])})

        logger.info(response)
        #PASA A PRE-CHECK IN Y ENVIA NOTIFICACION POR TWILIO A SMS y CORREO (TWILIO), ONESIGNAL (PUSH NOTIFICATION)
        if status == 2:
            # GET USER PREFERENCE NOTIFICATION
            response = dynamodbQuery.query(
                TableName="TuCita247",
                IndexName="TuCita247_Index",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='GSI1PK = :key AND GSI1SK = :key',
                ExpressionAttributeValues={
                    ':key': {'S': 'CUS#' + customerId}
                }
            )
            preference = ''
            for row in json_dynamodb.loads(response['Items']):
                preference = row['PREFERENCES'] if 'PREFERENCES' in row else ''
                mobile = row['PKID'].replace('MOB#','')
                email = row['EMAIL'] if 'EMAIL' in row else ''
            
            if preference == 1 and mobile != '':
                #SMS        
                to_number = mobile
                from_number = fromNumber
                bodyStr = 'You can go to the nearest entrance to check in'

                account_sid = twilioAccountSID
                auth_token = twilioAccountToken
                client = Client(account_sid, auth_token)
                
                message = client.messages.create(
                    from_= from_number,
                    to = to_number,
                    body = bodyStr
                )

            if preference == 2 and email != '':
                message = Mail(
                    from_email='Tu Cita 24/7 <no-reply@tucita247.com>',
                    to_emails=email,
                    subject='Tu Cita 24/7 Check-In',
                    html_content='<strong>You can yo to the nearest entrance to check in</strong>'
                )
                sg = SendGridAPIClient('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')
                response = sg.send(message)
                logger.info(response)
                # #EMAIL
                # SENDER = "Tu Cita 24/7 <no-reply@tucita247.com>"
                # RECIPIENT = email
                # SUBJECT = "Tu Cita 24/7 Check-In"
                # BODY_TEXT = ("You can yo to the nearest entrance to check in")
                            
                # # The HTML body of the email.
                # BODY_HTML = """<html>
                # <head></head>
                # <body>
                # <h1>Tu Cita 24/7</h1>
                # <p>You can yo to the nearest entrance to check in</p>
                # </body>
                # </html>"""

                # AWS_REGION = REGION
                # CHARSET = "UTF-8"

                # client = boto3.client('ses',region_name=AWS_REGION)
                # response = client.send_email(
                #     Destination={
                #         'ToAddresses': [
                #             RECIPIENT,
                #         ],
                #     },
                #     Message={
                #         'Body': {
                #             'Html': {
                #                 'Charset': CHARSET,
                #                 'Data': BODY_HTML,
                #             },
                #             'Text': {
                #                 'Charset': CHARSET,
                #                 'Data': BODY_TEXT,
                #             },
                #         },
                #         'Subject': {
                #             'Charset': CHARSET,
                #             'Data': SUBJECT,
                #         },
                #     },
                #     Source=SENDER
                # )

        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Message': 'Error on update appointment', 'Code': 500})
    except ClientError as e:  
        if e.response['Error']['Code']=='ConditionalCheckFailedException':  
            statusCode = 404
            body = json.dumps({'Message': 'Invalida qr code', 'Code': 400})
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