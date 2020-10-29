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

dynamodb = boto3.resource('dynamodb', region_name=REGION)
dynamodbQuery = boto3.client('dynamodb', region_name=REGION)
sms = boto3.client('sns')
ses = boto3.client('ses',region_name=REGION)
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def findService(serviceId, services):
    for item in services:
        if item['ServiceId'] == serviceId:
            return int(item['TimeService'])
    item = 0
    return item

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

        #GET SERVICES
        services = dynamodbQuery.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :key01 AND begins_with(SKID , :key02)',
            ExpressionAttributeValues={
                ':key01': {'S': 'BUS#' + businessId},
                ':key02': {'S': 'SER#'},
                ':status': {'N': str(1)}
            },
            FilterExpression='#s = :status',
            ExpressionAttributeNames={'#s': 'STATUS'}
        )
        serv = []
        for item in json_dynamodb.loads(services['Items']):
            recordset = {
                'ServiceId': item['SKID'].replace('SER#',''),
                'TimeService': item['TIME_SERVICE']
            }
            serv.append(recordset)

        response = dynamodbQuery.query(
            TableName="TuCita247",
            IndexName="TuCita247_Index",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='GSI1PK = :gsi1pk AND GSI1SK between :gsi1sk_ini and :gsi1sk_fin',
            ExpressionAttributeValues={
                ':gsi1pk': {'S': 'BUS#' + businessId + '#LOC#' + locationId + '#PRO#' + providerId},
                ':gsi1sk_ini': {'S': '1#DT#' + dateAppo[0:10] + '-00-00'},
                ':gsi1sk_fin': {'S': '1#DT#' + dateAppo}
            }
        )
        businessIdData = 'BUS#'+businessId+'#5'
        locationIdData = 'BUS#'+businessId+'#LOC#'+locationId+'#5'
        providerIdData = 'BUS#'+businessId+'#LOC#'+locationId+'#PRO#'+providerId+'#5'
        cancel = dateAppo[-5:]
        cancel = int(cancel[0:2])
        items = []
        entro = 0
        for row in json_dynamodb.loads(response['Items']):
            entro = 1
            timeService = findService(row['SERVICEID'], serv)
            if timeService != 0:
                citainiTemp = str(row['GSI1SK'])[-5:]
                citaini = int(citainiTemp[0:2])
                citafin = int(citaini)+int(timeService)-1
                if cancel >= citaini and cancel <= citafin:
                    appoData = row['DATE_APPO']+'#'+row['PKID']
                    recordset = {
                        "Update": {
                            "TableName": "TuCita247",
                            "Key": {
                                "PKID": {"S": row['PKID']}, 
                                "SKID": {"S": row['PKID']}, 
                            },
                            "UpdateExpression": "SET #s = :status, GSI1SK = :key01, GSI2SK = :key02, GSI5PK = :pkey05, GSI5SK = :skey05, GSI6PK = :pkey06, GSI6SK = :skey06, GSI7PK = :pkey07, GSI7SK = :skey07, TIMECANCEL = :dateope REMOVE GSI8PK, GSI8SK",
                            "ExpressionAttributeValues": { 
                                ":status": {"N": str(5)}, 
                                ":key01": {"S": str(5) + '#DT#' + row['DATE_APPO']}, 
                                ":key02": {"S": '#5'}, 
                                ":pkey05": {"S": businessIdData}, 
                                ":skey05": {"S": appoData}, 
                                ":pkey06": {"S": locationIdData}, 
                                ":skey06": {"S": appoData}, 
                                ":pkey07": {"S": providerIdData}, 
                                ":skey07": {"S": appoData},
                                ":dateope": {"S": dateOpe}
                            },
                            "ExpressionAttributeNames": {'#s': 'STATUS'},
                            "ConditionExpression": "attribute_exists(PKID) AND attribute_exists(SKID)",
                            "ReturnValuesOnConditionCheckFailure": "ALL_OLD" 
                        }
                    }
                    items.append(recordset)

                    if row['DATE_APPO'] != dateAppo:
                        response = dynamodbQuery.query(
                            TableName="TuCita247",
                            ReturnConsumedCapacity='TOTAL',
                            KeyConditionExpression='PKID = :key01 AND SKID = :key02',
                            ExpressionAttributeValues={
                                ':key01': {'S': 'LOC#' + locationId + '#PRO#' + providerId + '#DT#' + str(row['DATE_APPO'])[0:10]},
                                ':key02': {'S': 'HR#' + str(row['DATE_APPO'])[-5:]}
                            }
                        )
                        availableAppo = 0
                        custPerTime = 0
                        for item in json_dynamodb.loads(response['Items']):
                            availableAppo = int(item['AVAILABLE'])+int(row['PEOPLE_QTY'])
                            custPerTime = int(item['CUSTOMER_PER_TIME'])
                        if availableAppo == custPerTime:
                            recordset = {
                                "Delete": {
                                    "TableName": "TuCita247",
                                    "Key": {
                                        "PKID": {"S": 'LOC#'+locationId+'#PRO#'+providerId+'#DT#'+str(row['DATE_APPO'])[0:10]}, 
                                        "SKID": {"S": 'HR#' + str(row['DATE_APPO'])[-5:]}, 
                                    },
                                    "ConditionExpression": "attribute_exists(PKID) AND attribute_exists(SKID)",
                                    "ReturnValuesOnConditionCheckFailure": "ALL_OLD" 
                                }
                            }
                            items.append(recordset)

                        if availableAppo < custPerTime:
                            recordset = {
                                "Update": {
                                    "TableName": "TuCita247",
                                    "Key": {
                                        "PKID": {"S": 'LOC#'+locationId+'#PRO#'+providerId+'#DT#'+str(row['DATE_APPO'])[0:10]},
                                        "SKID": {"S": 'HR#'+str(row['DATE_APPO'])[-5:]},
                                    },
                                    "UpdateExpression": "AVAILABLE = AVAILABLE + :increment",
                                    "ExpressionAttributeValues": { 
                                        ":increment": {"N": str(row['PEOPLE_QTY'])}
                                    },
                                    "ConditionExpression": "attribute_exists(PKID) AND attribute_exists(SKID)", 
                                    "ReturnValuesOnConditionCheckFailure": "ALL_OLD" 
                                }
                            }
                            items.append(recordset)

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
                    language = ''
                    msg = ''
                    for row in json_dynamodb.loads(response['Items']):
                        preference = int(row['PREFERENCES']) if 'PREFERENCES' in row else 0
                        mobile = row['PKID'].replace('MOB#','')
                        email = row['EMAIL'] if 'EMAIL' in row else ''
                        playerId = row['PLAYERID'] if 'PLAYERID' in row else ''
                        language = str(row['LANGUAGE']).lower() if 'LANGUAGE' in row else 'en'
                    
                    if language == 'en':
                        msg = "Your appointment was cancelled by the business"
                    else:
                        msg = "Su cita fue cancelada por el negocio"
                    #CODIGO UNICO DEL TELEFONO PARA PUSH NOTIFICATION ONESIGNAL
                    if playerId != '':
                        header = {"Content-Type": "application/json; charset=utf-8"}
                        payload = {"app_id": "476a02bb-38ed-43e2-bc7b-1ded4d42597f",
                                "include_player_ids": [playerId],
                                "contents": {"en": msg}}
                        req = requests.post("https://onesignal.com/api/v1/notifications", headers=header, data=json.dumps(payload))

                    if preference == 1 and mobile != '':
                        #SMS
                        to_number = mobile
                        bodyStr = msg
                        sms.publish(
                            PhoneNumber="+"+to_number,
                            Message=bodyStr,
                            MessageAttributes={
                                    'AWS.SNS.SMS.SMSType': {
                                    'DataType': 'String',
                                    'StringValue': 'Transactional'
                                }
                            }
                        )
                        
                    if preference == 2 and email != '':
                        #EMAIL
                        SENDER = "Tu Cita 24/7 <no-reply@tucita247.com>"
                        RECIPIENT = email
                        SUBJECT = "Tu Cita 24/7 Check-In"
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

        if entro == 1:
            recordset = {
                "Update": {
                    "TableName": "TuCita247",
                    "Key": {
                        "PKID": {"S": 'LOC#'+locationId+'#PRO#'+providerId+'#DT#'+dateAppo[0:10]}, 
                        "SKID": {"S": 'HR#'+dateAppo[-5:]}, 
                    },
                    "UpdateExpression": "SET AVAILABLE = :available, CANCEL = :cancel, TIME_SERVICE = :service",
                    "ExpressionAttributeValues": {
                        ':available': {"N": str(0)},
                        ':cancel': {"N": str(1)},
                        ':service': {"N": str(1)}
                    }
                }
            }
            items.append(recordset)
        else:
            response = dynamodbQuery.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :key01 AND SKID = :key02',
                ExpressionAttributeValues={
                    ':key01': {'S': 'LOC#' + locationId + '#PRO#' + providerId + '#DT#' + dateAppo[0:10]},
                    ':key02': {'S': 'HR#'+dateAppo[-5:]}
                }
            )
            existSum = 0
            for item in json_dynamodb.loads(response['Items']):
                existSum = 1
            
            if existSum == 0:
                recordset = {
                    "Put": {
                        "TableName": "TuCita247",
                        "Item": {
                            "PKID": {"S": 'LOC#'+locationId+'#PRO#'+providerId+'#DT#'+dateAppo[0:10]}, 
                            "SKID": {"S": 'HR#'+dateAppo[-5:]},
                            "SERVICEID": {"S": ''},
                            "AVAILABLE": {"N": str(0)},
                            "CUSTOMER_PER_TIME": {"N": str(0)},
                            "TIME_SERVICE": {"N": str(1)},
                            "CANCEL": {"N": str(1)}
                        },
                        "ConditionExpression": "attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                        "ReturnValuesOnConditionCheckFailure": "ALL_OLD" 
                    }
                }
                items.append(recordset)
            else:
                recordset = {
                    "Update": {
                        "TableName": "TuCita247",
                        "Key": {
                            "PKID": {"S": 'LOC#'+locationId+'#PRO#'+providerId+'#DT#'+dateAppo[0:10]}, 
                            "SKID": {"S": 'HR#'+dateAppo[-5:]}, 
                        },
                        "UpdateExpression": "SET AVAILABLE = :available, CANCEL = :cancel, TIME_SERVICE = :service",
                        "ExpressionAttributeValues": {
                            ':available': {"N": str(0)},
                            ':cancel': {"N": str(1)},
                            ':service': {"N": str(1)}
                        }
                    }
                }
                items.append(recordset)
        
        logger.info(items)
        response = dynamodbQuery.transact_write_items(
            TransactItems = items
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