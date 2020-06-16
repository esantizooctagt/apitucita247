import sys
import logging
import json

import boto3
import botocore.exceptions
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

dynamodb = boto3.client('dynamodb', region_name='us-east-1')
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']
        
    try:
        statusCode = ''
        appointmentId = ''
        dateAppo = ''
        timeCheckIn = ''
        qty = 0
        existe = 0
        data = json.loads(event['body'])
        status = data['Status']
        qrCode = data['qrCode']
        businessId = data['BusinessId']
        locationId = data['LocationId']

        country_date = dateutil.tz.gettz('America/Puerto_Rico')
        today = datetime.datetime.now(tz=country_date)
        dateOpe = today.strftime("%Y-%m-%d")

        country_date = dateutil.tz.gettz('America/Puerto_Rico')
        outTime = datetime.datetime.now(tz=country_date)
        outTime = outTime.strftime('%Y-%m-%d %H:%M:%S.%f')
        outTime = datetime.datetime.strptime(outTime,'%Y-%m-%d %H:%M:%S.%f')

        e = {'#s': 'STATUS'}
        f = '#s = :stat'
        response = dynamodb.query(
            TableName="TuCita247",
            IndexName="TuCita247_Appos",
            ReturnConsumedCapacity='TOTAL',
            ExpressionAttributeNames=e,
            FilterExpression=f,
            KeyConditionExpression='GSI3PK = :key01 AND GSI3SK = :key02',
            ExpressionAttributeValues={
                ':key01': {'S': 'BUS#'+businessId+'#LOC#'+locationId+'#'+dateOpe},
                ':key02': {'S': 'QR#'+qrCode},
                ':stat' : {'N': '3'}
            }
        )
        for row in json_dynamodb.loads(response['Items']):
            appointmentId = row['PKID']
            dateAppo = row['DATE_APPO']
            qty = row['PEOPLE_QTY']
            customerId = row['GSI2PK'].replace('CUS#','')
            timeCheckIn = row['TIMECHECKIN'] + '-000000' if 'TIMECHECKIN' in row else ''

        if appointmentId != '':
            items = []
            dateOpe = today.strftime("%Y-%m-%d-%H-%M-%S")

            if timeCheckIn != '':
                inTime = datetime.datetime.strptime(timeCheckIn, '%Y-%m-%d-%H-%M-%S-%f')
                citaTime = outTime - inTime
                citaTime = int(str(citaTime).split(':')[0]) + int(str(citaTime).split(':')[1])/60
                dateAvg = str(dateAppo)[0:10]

                response = dynamodb.query(
                    TableName="TuCita247",
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='PKID = :key01 AND SKID = :key02',
                    ExpressionAttributeValues={
                        ':key01': {'S': 'LOC#'+locationId},
                        ':key02': {'S': 'DT#'+dateAvg}
                    }
                )
                for row in json_dynamodb.loads(response['Items']):
                    timeAct = row['TIME_APPO']
                    existe = 1

            recordset = {
                "Update": {
                    "TableName": "TuCita247",
                    "Key": {
                        "PKID": {"S": appointmentId}, 
                        "SKID": {"S": appointmentId}
                    },
                    "UpdateExpression": "SET #s = :status, GSI1SK = :key, GSI2SK = :key, TIMECHECKOUT = :dateOpe", 
                    "ExpressionAttributeValues": {
                        ":status": {"N": str(status)}, 
                        ":key": {"S": str(status) + '#DT#' + str(dateAppo)}, 
                        ":dateOpe": {"S": str(dateOpe)},
                        ":qrCode": {"S": qrCode}
                    },
                    "ExpressionAttributeNames": {'#s': 'STATUS'},
                    "ConditionExpression": "attribute_exists(PKID) AND attribute_exists(SKID) AND QRCODE = :qrCode",
                    "ReturnValuesOnConditionCheckFailure": "ALL_OLD" 
                }
            }
            items.append(recordset)

            recordset = {
                "Update": {
                    "TableName": "TuCita247",
                    "Key": {
                        "PKID": {"S": 'BUS#' + businessId}, 
                        "SKID": {"S": 'LOC#' + locationId}, 
                    },
                    "UpdateExpression": "SET PEOPLE_CHECK_IN = PEOPLE_CHECK_IN - :increment",
                    "ExpressionAttributeValues": { 
                        ":increment": {"N": str(qty)}
                    },
                    "ConditionExpression": "attribute_exists(PKID) AND attribute_exists(SKID)",
                    "ReturnValuesOnConditionCheckFailure": "ALL_OLD" 
                }
            }
            items.append(recordset)

            if citaTime != '':
                if existe == 1:
                    recordset = {
                        "Update":{
                            "TableName": "TuCita247",
                            "Key":{
                                "PKID": {"S": 'LOC#' + locationId},
                                "SKID": {"S": 'DT#' + dateAvg}
                            },
                            "UpdateExpression": "SET TIME_APPO = TIME_APPO + :citaTime, QTY_APPOS = QTY_APPOS + :qty",
                            "ExpressionAttributeValues": { 
                                ":citaTime": {"N": str(citaTime)},
                                ":qty": {"N": str(1)}
                            },
                            "ConditionExpression": "attribute_exists(PKID) AND attribute_exists(SKID)",
                            "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
                        }
                    }
                else:
                    recordset = {
                        "Put": {
                            "TableName": "TuCita247",
                            "Item":{
                                "PKID": {"S": 'LOC#' + locationId},
                                "SKID": {"S": 'DT#'+ dateAvg},
                                "GSI1PK": {"S": 'BUS#' + businessId},
                                "GSI1SK": {"S": 'LOC#' + locationId},
                                "TIME_APPO": {"N": str(citaTime)},
                                "QTY_APPOS": {"N": str(1)}
                            },
                            "ConditionExpression": "attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                            "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
                        }
                    }

                items.append(recordset)

            logger.info(items)
            tranAppo = dynamodb.transact_write_items(
                TransactItems = items
            )
            logger.info("transaction finished")
            #SEND NOTIFICATION CON LINK DE ENCUESTA
            dateOpe = today.strftime("%Y-%m-%d")
            response = dynamodb.query(
                TableName="TuCita247",
                IndexName="TuCita247_CustAppos",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='GSI2PK = :key AND GSI2SK <= :datePoll',
                ScanIndexForward=False,
                FilterExpression='DATE_FIN_POLL >= :datePoll',
                ExpressionAttributeValues={
                    ':key': {'S': 'BUS#' + businessId + '#LOC#' + locationId},
                    ':datePoll': {'S': '1#DT#' + dateOpe}
                },
                Limit=1
            )

            for poll in json_dynamodb.loads(response['Items']):
                pollId = poll['SKID'].replace('POLL#','')
            logger.info(pollId)

            if pollId != '':
                #PENDIENTE ENVIAR EL LINK
                link = 'https://console.tucita247.com/poll-response/' + pollId + '/' + customerId
                logger.info(link)
                response = dynamodb.query(
                    TableName="TuCita247",
                    IndexName="TuCita247_Index",
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='GSI1PK = :key AND GSI1SK = :key',
                    ExpressionAttributeValues={
                        ':key': {'S': 'CUS#' + customerId}
                    },
                    Limit = 1
                )
                preference = ''
                logger.info("preference vacio")
                logger.info(response)
                for row in json_dynamodb.loads(response['Items']):
                    preference = int(row['PREFERENCES']) if 'PREFERENCES' in row else 0
                    mobile = row['PKID'].replace('MOB#','')
                    email = row['EMAIL'] if 'EMAIL' in row else ''
                    logger.info(preference)
                    if preference == 1 and mobile != '':
                        #SMS        
                        to_number = mobile
                        from_number = fromNumber
                        bodyStr = 'Please fill the next poll ' + link

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
                            from_email='Tu Cita 24/7 - Service Poll <no-reply@tucita247.com>',
                            to_emails=email,
                            subject='Tu Cita 24/7 Check-In - Service Poll',
                            html_content='<strong>Please fill the next poll</strong><p>Link ' + link + '</p>'
                        )
                        sg = SendGridAPIClient('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')
                        response = sg.send(message)
                        logger.info(response)
                        # logger.info("send email")
                        # #EMAIL
                        # SENDER = "Tu Cita 24/7 - Service Poll <no-reply@tucita247.com>"
                        # RECIPIENT = email
                        # SUBJECT = "Tu Cita 24/7 Check-In - Service Poll"
                        # BODY_TEXT = ("Please fill the next poll \n\r " + link)
                                    
                        # # The HTML body of the email.
                        # BODY_HTML = """<html>
                        # <head></head>
                        # <body>
                        # <h1>Tu Cita 24/7 - Service Poll</h1>
                        # <p>Please fill the next poll</p>
                        # <p>Link """ + link + """</p>
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
                        # logger.info(link)

            logger.info(tranAppo)
            statusCode = 200
            body = json.dumps({'Message': 'Appointment updated successfully', 'Code': 200})
        else:
            statusCode = 404
            body = json.dumps({'Message': 'Invalid appointment, please verify', 'Code': 404})

        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Message': 'Error on update appointment', 'Code': 500})
    except dynamodb.exceptions.TransactionCanceledException as e:
        statusCode = 404
        body = json.dumps({'Message': 'QR Code invalid ' + str(e), 'Code': 404})
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