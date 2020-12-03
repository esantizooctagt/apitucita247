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
    stage = event['headers']

    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']

    try:
        country_date = dateutil.tz.gettz('America/Puerto_Rico')
        today = datetime.datetime.now(tz=country_date)
        dateOpe = today.strftime("%Y-%m-%d-%H-00")
        
        appoId = event['pathParameters']['AppointmentId']
        response = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :key AND SKID = :key',
            ExpressionAttributeValues={
                ':key': {'S': 'APPO#'+appoId}
            }
        )
        recordset = {}
        for row in json_dynamodb.loads(response['Items']):
            businessId = ''
            locationId = ''
            providerId = ''
            businessName = ''
            appId = ''

            appId = row['PKID']
            dateAppo = row['DATE_APPO']
            guests = row['PEOPLE_QTY']
            customerId = row['GSI2PK']
            appoData = str(row['DATE_APPO'])[0:10]+'#'+appId
            data = row['GSI1PK'].split('#')

            businessId = data[1]
            locationId = data[3]
            providerId = data[5]

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
            
            getAddr = dynamodb.query(
                TableName = "TuCita247",
                ReturnConsumedCapacity = 'TOTAL',
                KeyConditionExpression = 'PKID = :key01 AND SKID = :key02',
                ExpressionAttributeValues = {
                    ':key01': {"S": 'BUS#' + businessId},
                    ':key02': {"S": 'LOC#' + locationId}
                }
            )
            Addr = ''
            for addr in json_dynamodb.loads(getAddr['Items']):
                Addr = addr['ADDRESS']
            
            servs = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :businessId AND begins_with(SKID , :serv)',
                ExpressionAttributeValues={
                    ':businessId': {'S': 'BUS#'+businessId},
                    ':serv': {'S': 'SER#' }
                }
            )
            count = 0
            servName = ''
            bufferTime = ''
            for serv in json_dynamodb.loads(servs['Items']):
                count = count + 1
                if serv['SKID'].replace('SER#','') == row['SERVICEID']:
                    servName = serv['NAME']
                    bufferTime = serv['BUFFER_TIME']

            if count == 1:
                servName = ''
            
            provs =  dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :key AND begins_with(SKID , :provs)',
                ExpressionAttributeValues={
                    ':key': {'S': 'BUS#'+businessId+'#LOC#'+locationId},
                    ':provs': {'S': 'PRO#' }
                }
            )
            countp = 0
            provName = ''
            for prov in json_dynamodb.loads(provs['Items']):
                countp = countp + 1
                if prov['SKID'].replace('PRO#','') == providerId:
                    provName = prov['NAME']
            if countp == 1:
                provName = ''
                
            items = []
            recordset = {
                "Update": {
                    "TableName": "TuCita247",
                    "Key": {
                        "PKID": {"S": appId}, 
                        "SKID": {"S": appId}, 
                    },
                    "UpdateExpression": "SET #s = :status, GSI1SK = :key01, GSI2SK = :key01, GSI9SK = :key01 REMOVE GSI8PK, GSI8SK, TIMECHEK",
                    "ExpressionAttributeValues": { 
                        ":status": {"N": str(1)}, 
                        ":key01": {"S": '1#DT#' + str(dateAppo)}
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
            
            sTime = ''
            dateAppointment = str(row['DATE_APPO'])
            hTime = int(str(dateAppointment[-5:])[0:2])
            if hTime >= 12:
                if hTime == 12:
                    sTime = str(hTime) + ':00 PM'
                else:
                    hTime = hTime-12
                    sTime = str(hTime).rjust(2,'0') + ':00 PM'
            else:
                sTime = str(hTime).rjust(2,'0') + ':00 AM'
            data = {
                'BusinessId': businessId,
                'LocationId': locationId,
                'AppId': appId.replace('APPO#',''),
                'CustomerId': customerId.replace('CUS#',''),
                'ProviderId': providerId,
                'BufferTime': bufferTime,
                'Name': row['NAME'],
                'Provider': provName,
                'Service': servName,
                'Phone': row['PHONE'],
                'OnBehalf': row['ON_BEHALF'] if 'ON_BEHALF' in row else '',
                'Guests': 0 if guests == '' else int(guests),
                'Door': row['DOOR'] if 'DOOR' in row else '',
                'Disability': row['DISABILITY'] if 'DISABILITY' in row else 0,
                'DateFull': row['DATE_APPO'],
                'Type': row['TYPE'],
                'DateAppo': sTime,
                'Status': 1,
                'UnRead': '',
                'QRCode': row['QRCODE'],
                'Ready': 0,
                'NameBusiness': businessName,
                'Address': Addr,
                'DateTrans': row['DATE_TRANS'],
                'Qeue': 'PRE' if row['DATE_APPO'] < dateOpe else 'UPC',
                'Tipo': 'REVERSE'
            }
            lambdaInv.invoke(
                FunctionName='PostMessages',
                InvocationType='Event',
                Payload=json.dumps(data)
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