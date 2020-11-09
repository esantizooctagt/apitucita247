import sys
import logging
import requests
import json

import datetime
import dateutil.tz
from datetime import timezone

import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr
from dynamodb_json import json_util as json_dynamodb

import os

REGION = 'us-east-1'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.client('dynamodb', region_name=REGION)
sms = boto3.client('sns')
ses = boto3.client('ses', region_name=REGION)
lambdaInv = boto3.client('lambda')

logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):
    try:
        country_date = dateutil.tz.gettz('America/Puerto_Rico')
        today = datetime.datetime.now(tz=country_date)
        dateOpe = today.strftime("%Y-%m-%d-%H-%M")
        dateAppoFin = (today + datetime.timedelta(minutes=-5)).strftime("%Y-%m-%d-%H-%M")
        
        response = dynamodb.query(
            TableName="TuCita247",
            IndexName="TuCita247_Parent",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='GSI8PK = :key AND GSI8SK <= :skey',
            ExpressionAttributeValues={
                ':key': {'S': 'PRECHECKIN'},
                ':skey': {'S': dateAppoFin}
            }
        )
        logger.info(response)
        record = []
        recordset = {}
        for row in json_dynamodb.loads(response['Items']):
            businessId = ''
            locationId = ''
            providerId = ''
            businessName = ''
            busLanguage = ''

            appId = row['PKID']
            dateAppo = row['DATE_APPO']
            guests = row['PEOPLE_QTY']
            customerId = row['GSI2PK']
            appoData = str(row['DATE_APPO'])[0:10]+'#'+appId
            data = row['GSI1PK'].split('#')

            businessId = data[1]
            locationId = data[3]
            providerId = data[5]
            logger.info("Appo a procesar " + appId)
            getBusiness = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :key AND SKID = :skey',
                ExpressionAttributeValues={
                    ':key': {'S': 'BUS#'+businessId},
                    ':skey': {'S': 'LOC#'+locationId}
                }
            )
            for business in json_dynamodb.loads(getBusiness['Items']):
                businessName = business['NAME']

            getLanguage = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :key AND SKID = :skey',
                ExpressionAttributeValues={
                    ':key': {'S': 'BUS#'+businessId},
                    ':skey': {'S': 'METADATA'}
                }
            )
            for busLng in json_dynamodb.loads(getLanguage['Items']):
                busLanguage = busLng['LANGUAGE'] if 'LANGUAGE' in busLng else 'en'
            
            items = []
            # getData = dynamodb.query(
            #     TableName="TuCita247",
            #     ReturnConsumedCapacity='TOTAL',
            #     KeyConditionExpression='PKID = :key01 AND SKID = :key02',
            #     ExpressionAttributeValues={
            #         ':key01': {'S': 'LOC#'+locationId+'#PRO#'+providerId+'#DT#'+dateAppo[0:10]},
            #         ':key02': {'S': 'HR#' + dateAppo[-5:]}
            #     }
            # )
            # custQty = 0
            # available = 0
            # for app in json_dynamodb.loads(getData['Items']):
            #     custQty = int(app['CUSTOMER_PER_TIME'])
            #     available = int(app['AVAILABLE'])+int(guests)

            # if available < custQty:
            #     recordset = {
            #         "Update": {
            #             "TableName": "TuCita247",
            #             "Key": {
            #                 "PKID": {"S": 'LOC#'+locationId+'#PRO#'+providerId+'#DT#'+dateAppo[0:10]}, 
            #                 "SKID": {"S": 'HR#' + dateAppo[-5:]}, 
            #             },
            #             "UpdateExpression": "SET AVAILABLE = AVAILABLE + :increment",
            #             "ExpressionAttributeValues": { 
            #                 ":increment": {"N": str(guests)}
            #             },
            #             "ConditionExpression": "attribute_exists(PKID) AND attribute_exists(SKID)",
            #             "ReturnValuesOnConditionCheckFailure": "ALL_OLD" 
            #         }
            #     }
            #     items.append(recordset)

            # if available == custQty:
            #     recordset = {
            #         "Delete": {
            #             "TableName": "TuCita247",
            #             "Key": {
            #                 "PKID": {"S": 'LOC#'+locationId+'#PRO#'+providerId+'#DT#'+dateAppo[0:10]}, 
            #                 "SKID": {"S": 'HR#' + dateAppo[-5:]}, 
            #             },
            #             "ConditionExpression": "attribute_exists(PKID) AND attribute_exists(SKID)",
            #             "ReturnValuesOnConditionCheckFailure": "ALL_OLD" 
            #         }
            #     }
            #     items.append(recordset)

            recordset = {
                "Update": {
                    "TableName": "TuCita247",
                    "Key": {
                        "PKID": {"S": appId}, 
                        "SKID": {"S": appId}, 
                    },
                    "UpdateExpression": "SET #s = :status, GSI1SK = :key01, GSI2SK = :key02, REASONID = :reason, GSI5PK = :pkey05, GSI5SK = :skey05, GSI6PK = :pkey06, GSI6SK = :skey06, GSI7PK = :pkey07, GSI7SK = :skey07, GSI9SK = :key01, TIMECANCEL = :dateope REMOVE GSI8PK, GSI8SK",
                    "ExpressionAttributeValues": { 
                        ":status": {"N": str(5)}, 
                        ":key01": {"S": '5#DT#' + str(dateAppo)}, 
                        ":key02": {"S": '#5'}, 
                        ":reason": {"S": 'NOT SHOW'},  
                        ":pkey05": {"S": 'BUS#'+businessId+'#5'}, 
                        ":skey05": {"S": appoData}, 
                        ":pkey06": {"S": 'BUS#'+businessId+'#LOC#'+locationId+'#5'}, 
                        ":skey06": {"S": appoData}, 
                        ":pkey07": {"S": 'BUS#'+businessId+'#LOC#'+locationId+'#PRO#'+providerId+'#5'}, 
                        ":skey07": {"S": appoData},
                        ":dateope": {"S": dateOpe}
                    },
                    "ExpressionAttributeNames": {'#s': 'STATUS'},
                    "ConditionExpression": "attribute_exists(PKID) AND attribute_exists(SKID)",
                    "ReturnValuesOnConditionCheckFailure": "ALL_OLD" 
                }
            }
            items.append(recordset)

            logger.info(items)
            response = dynamodb.transact_write_items(
                TransactItems = items
            )
            logger.info("Appo Cancelada")
            data = {
                'BusinessId': businessId,
                'LocationId': locationId,
                'AppId': appId.replace('APPO#',''),
                'CustomerId': customerId.replace('CUS#',''),
                'Tipo': 'CANCEL'
            }
            lambdaInv.invoke(
                FunctionName='PostMessages',
                InvocationType='Event',
                Payload=json.dumps(data)
            )

            # GET USER PREFER  ENCE NOTIFICATION
            response = dynamodb.query(
                TableName="TuCita247",
                IndexName="TuCita247_Index",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='GSI1PK = :key AND GSI1SK = :key',
                ExpressionAttributeValues={
                    ':key': {'S': customerId}
                }
            )
            preference = 0
            playerId = ''
            language = ''
            for cust in json_dynamodb.loads(response['Items']):
                preference = int(cust['PREFERENCES']) if 'PREFERENCES' in cust else 0
                mobile = cust['PKID'].replace('MOB#','')
                email = cust['EMAIL'] if 'EMAIL' in cust else ''
                playerId = cust['PLAYERID'] if 'PLAYERID' in cust else ''
                language = str(cust['LANGUAGE']).lower() if 'LANGUAGE' in cust else busLanguage

            hrAppo = datetime.datetime.strptime(dateAppo, '%Y-%m-%d-%H-%M').strftime('%I:%M %p')
            dayAppo = datetime.datetime.strptime(dateAppo[0:10], '%Y-%m-%d').strftime('%b %d %Y')
            if language == "en":
                textMess = businessName + ' has canceled your booking for ' + dayAppo  + ', ' + hrAppo + '. Reason: NOT SHOW'
            else:
                textMess = businessName + ' ha cancelado su cita para ' + dayAppo + ', ' + hrAppo + '. Razón: NO SE PRESENTO'

            logger.info('Preference user ' + customerId + ' -- ' + str(preference))
            #CODIGO UNICO DEL TELEFONO PARA PUSH NOTIFICATION ONESIGNAL
            if playerId != '':
                header = {"Content-Type": "application/json; charset=utf-8"}
                payload = {"app_id": "476a02bb-38ed-43e2-bc7b-1ded4d42597f",
                        "include_player_ids": [playerId],
                        "contents": {"en": textMess }}
                req = requests.post("https://onesignal.com/api/v1/notifications", headers=header, data=json.dumps(payload))

            if int(preference) == 1 and mobile != '00000000000':
                #SMS
                to_number = mobile
                bodyStr = textMess
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
                
            if int(preference) == 2 and email != '':
                #EMAIL
                SENDER = "Tu Cita 24/7 <no-reply@tucita247.com>"
                RECIPIENT = email
                SUBJECT = "Tu Cita 24/7 Pre Check-In"
                BODY_TEXT = (textMess)
                            
                # The HTML body of the email.
                BODY_HTML = """<html>
                <head></head>
                <body>
                <h1>Tu Cita 24/7</h1>
                <p>""" + textMess + """</p>
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
        
        resultSet = { 
            'Code': 200,
            'Message': 'OK'
        }
        statusCode = 200
        body = json.dumps(resultSet)
    
        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Code': 500, 'Message': 'Error on load appointments'})
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