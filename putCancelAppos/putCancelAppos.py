import sys
import logging
import requests
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

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
dynamodbQuery = boto3.client('dynamodb', region_name='us-east-1')
sms = boto3.client('sns')
ses = boto3.client('ses',region_name=REGION)
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']
        
    try:
        businessId = event['pathParameters']['businessId']
        locationId = event['pathParameters']['locationId']
        providerId = event['pathParameters']['providerId']
        dateAppo = event['pathParameters']['dateAppo']

        country_date = dateutil.tz.gettz('America/Puerto_Rico')
        today = datetime.datetime.now(tz=country_date)
        dateOpe = today.strftime("%Y-%m-%d-%H-%M-%S")

        response = dynamodbQuery.query(
            TableName="TuCita247",
            IndexName="TuCita247_Index",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='GSI1PK = :gsi1pk AND GSI1SK = :gsi1sk_ini',
            ExpressionAttributeValues={
                ':gsi1pk': {'S': 'BUS#' + businessId + '#LOC#' + locationId + '#PRO#' + providerId},
                ':gsi1sk_ini': {'S': '1#DT#' + dateAppo}
            }
        )
        businessIdData = 'BUS#'+businessId+'#5'
        locationIdData = 'BUS#'+businessId+'#LOC#'+locationId+'#5'
        providerIdData = 'BUS#'+businessId+'#LOC#'+locationId+'#PRO#'+providerId+'#5'
        for row in json_dynamodb.loads(response['Items']):
            appoData = dateAppo[0:10]+'#'+row['PKID']
            table = dynamodb.Table('TuCita247')
            e = {'#s': 'STATUS'}
            v = {':status': 5, ':key01': str(5) + '#DT#' + str(dateAppo), ':key02': '#5', ':dateope': dateOpe, ':pkey05': businessIdData, ':skey05': appoData, ':pkey06': locationIdData, ':skey06': appoData, ':pkey07': providerIdData, ':skey07': appoData}
            response = table.update_item(
                Key={
                    'PKID': row['PKID'],
                    'SKID': row['PKID']
                },
                UpdateExpression="set #s = :status, GSI1SK = :key01, GSI2SK = :key02, TIMECANCEL = :dateope, GSI5PK = :pkey05, GSI5SK = :skey05, GSI6PK = :pkey06, GSI6SK = :skey06, GSI7PK = :pkey07, GSI7SK = :skey07",
                ExpressionAttributeNames=e,
                ExpressionAttributeValues=v,
                ReturnValues="UPDATED_NEW"
            )

            response = table.update_item(
                Key={
                    'PKID': 'LOC#'+locationId+'#PRO#'+providerId+'#DT#'+dateAppo[0:10],
                    'SKID': 'HR#'+dateAppo[-5:]
                },
                UpdateExpression="SET AVAILABLE = :available",
                ExpressionAttributeValues={':available': 0},
                ReturnValues="UPDATED_NEW"
            )

            # GET USER PREFERENCE NOTIFICATION
            response = dynamodbQuery.query(
                TableName="TuCita247",
                IndexName="TuCita247_Index",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='GSI1PK = :key AND GSI1SK = :key',
                ExpressionAttributeValues={
                    ':key': {'S': row['GSI2PK']}
                }
            )
            preference = ''
            playerId = ''
            for row in json_dynamodb.loads(response['Items']):
                preference = row['PREFERENCES'] if 'PREFERENCES' in row else ''
                mobile = row['PKID'].replace('MOB#','')
                email = row['EMAIL'] if 'EMAIL' in row else ''
                playerId = row['PLAYERID'] if 'PLAYERID' in row else ''
            
            #CODIGO UNICO DEL TELEFONO PARA PUSH NOTIFICATION ONESIGNAL
            if playerId != '':
                header = {"Content-Type": "application/json; charset=utf-8"}
                payload = {"app_id": "476a02bb-38ed-43e2-bc7b-1ded4d42597f",
                        "include_player_ids": [playerId],
                        "contents": {"en": "Your appointment was cancelled by the business"}}
                req = requests.post("https://onesignal.com/api/v1/notifications", headers=header, data=json.dumps(payload))

            if preference == 1 and mobile != '':
                #SMS
                to_number = mobile
                bodyStr = 'Your appointment was cancelled by the business'
                sms.publish(
                    PhoneNumber="+"+to_number,
                    Message=bodyStr
                )
                
            if preference == 2 and email != '':
                #EMAIL
                SENDER = "Tu Cita 24/7 <no-reply@tucita247.com>"
                RECIPIENT = email
                SUBJECT = "Tu Cita 24/7 Check-In"
                BODY_TEXT = ("Your appointment was cancelled by the business")
                            
                # The HTML body of the email.
                BODY_HTML = """<html>
                <head></head>
                <body>
                <h1>Tu Cita 24/7</h1>
                <p>Your appointment was cancelled by the business</p>
                </body>
                </html>"""

                CHARSET = "UTF-8"

                response = ses.send_email(
                    Destination={
                        'ToAddresses': [
                            RECIPIENT,
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
        body = json.dumps({'Message': 'Citas deleted successfully', 'Code': 200})

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