import sys
import logging
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
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def findService(serviceId, servs):
    for item in servs:
        if item['ServiceId'] == serviceId:
            return int(item['BufferTime'])
    return 1

def findServiceName(serviceId, servs, items):
    if items <= 1:
        return ''
    for item in servs:
        if item['ServiceId'] == serviceId:
            return item['Name']
    return ''

def findProvider(providerId, providers, items):
    if items <= 1:
        return ''
    for item in providers:
        if item['ProviderId'] == providerId:
            return item['Name']
    return ''

def lambda_handler(event, context):
    stage = event['headers']

    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']
    
    try:
        country_date = dateutil.tz.gettz('America/Puerto_Rico')
        today = datetime.datetime.now(tz=country_date)

        businessId = event['pathParameters']['businessId']
        locationId = event['pathParameters']['locationId']
        providerId = event['pathParameters']['providerId']
        status = event['pathParameters']['status']
        typeAppo = event['pathParameters']['type']
        appoType = int(event['pathParameters']['appoType'])

        dateAppoIni = ''
        dateAppoFin = ''

        #BOOKINGS -6 HORAS A LA HORA ACTUAL
        if appoType != 2:
            if status == '1' and typeAppo == '1':
                dateAppoIni = (today + datetime.timedelta(hours=-6)).strftime("%Y-%m-%d-%H-00") #00
                dateAppoFin = (today + datetime.timedelta(hours=-1)).strftime("%Y-%m-%d-%H-59")
            if status == '1' and typeAppo == '2':
                dateAppoIni = today.strftime("%Y-%m-%d-%H-00")
                dateAppoFin = (today + datetime.timedelta(hours=6)).strftime("%Y-%m-%d-%H-59")
            if status == '2':
                dateAppoIni = (today + datetime.timedelta(hours=-6)).strftime("%Y-%m-%d-%H-00")
                dateAppoFin = today.strftime("%Y-%m-%d-23-59")
        else:
            dateAppoIni = (today + datetime.timedelta(hours=-6)).strftime("%Y-%m-%d-%H-00")
            dateAppoFin = (today + datetime.timedelta(hours=6)).strftime("%Y-%m-%d-%H-59")

        #GET PROVS INFO
        provs = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :key AND begins_with(SKID , :skey)',
            ExpressionAttributeValues={
                ':key': {'S': 'BUS#'+businessId+'#LOC#'+locationId},
                ':skey': {'S': 'PRO#'}
            }
        )
        providers = []
        count = 0
        for prov in json_dynamodb.loads(provs['Items']):
            count = count + 1
            recordset = {
                'ProviderId': prov['SKID'].replace('PRO#',''),
                'Name': prov['NAME']
            }
            providers.append(recordset)

        #GET SERVICES INFO
        servs = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :key AND begins_with(SKID , :skey)',
            ExpressionAttributeValues={
                ':key': {'S': 'BUS#' + businessId},
                ':skey': {'S': 'SER#'}
            }
        )
        services = []
        counts = 0
        for serv in json_dynamodb.loads(servs['Items']):
            counts = counts + 1
            recordset = {
                'ServiceId': serv['SKID'].replace('SER#',''),
                'Name': serv['NAME'],
                'BufferTime': serv['BUFFER_TIME']
            }
            services.append(recordset)

        if providerId != '0':
            response = dynamodb.query(
                TableName="TuCita247",
                IndexName="TuCita247_Index",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='GSI1PK = :gsi1pk AND GSI1SK BETWEEN :gsi1sk_ini AND :gsi1sk_fin',
                FilterExpression='#t = :appoType' if status == '1' else '#t >= :appoType',
                ExpressionAttributeNames={'#t': 'TYPE'},
                ExpressionAttributeValues={
                    ':gsi1pk': {'S': 'BUS#' + businessId + '#LOC#' + locationId + '#PRO#' + providerId},
                    ':gsi1sk_ini': {'S': str(status) +'#DT#' + dateAppoIni},
                    ':gsi1sk_fin': {'S': str(status) +'#DT#' + dateAppoFin},
                    ':appoType': {'N': str(appoType)}
                }
            )
        else:
            response = dynamodb.query(
                TableName="TuCita247",
                IndexName="TuCita247_Index09",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='GSI9PK = :gsi9pk AND GSI9SK BETWEEN :gsi9sk_ini AND :gsi9sk_fin',
                FilterExpression='#t = :appoType' if status == '1' else '#t >= :appoType',
                ExpressionAttributeNames={'#t': 'TYPE'},
                ExpressionAttributeValues={
                    ':gsi9pk': {'S': 'BUS#' + businessId + '#LOC#' + locationId },
                    ':gsi9sk_ini': {'S': str(status) +'#DT#' + dateAppoIni},
                    ':gsi9sk_fin': {'S': str(status) +'#DT#' + dateAppoFin},
                    ':appoType': {'N': str(appoType)}
                }
            )
        
        record = []
        recordset = {}
        for row in json_dynamodb.loads(response['Items']):
            recordset = {
                'BusinessId': businessId,
                'LocationId': locationId,
                'ProviderId': row['GSI1PK'].replace('BUS#'+businessId+'#LOC#'+locationId+'#PRO#',''),
                'BufferTime': findService(row['SERVICEID'], services),
                'AppointmentId': row['PKID'].replace('APPO#',''),
                'ClientId': row['GSI2PK'].replace('CUS#',''),
                'Name': row['NAME'],
                'Phone': row['PHONE'],
                'Provider': findProvider(row['GSI1PK'].replace('BUS#'+businessId+'#LOC#'+locationId+'#PRO#',''), providers, count),
                'Service': findServiceName(row['SERVICEID'], services, counts),
                'OnBehalf': row['ON_BEHALF'],
                'Guests': row['PEOPLE_QTY'] if 'PEOPLE_QTY' in row else 0,
                'Door': row['DOOR'] if 'DOOR' in row else '',
                'Disability': row['DISABILITY'] if 'DISABILITY' in row else 0,
                'Type': row['TYPE'] if 'TYPE' in row else 0,
                'DateAppo': row['DATE_APPO'],
                'Unread': row['UNREAD'] if 'UNREAD' in row else 0,
                'CheckInTime': row['TIMECHEK'] if 'TIMECHEK' in row else '',
                'Purpose': row['PURPOSE'] if 'PURPOSE' in row else '',
                'QrCode': row['QRCODE'] if 'QRCODE' in row else '',
                'DateTrans': row['DATE_TRANS'] if 'DATE_TRANS' in row else today.strftime("%Y-%m-%d-%H-%M"),
                'Status': row['STATUS']
            }
            record.append(recordset)

        lastItem = ''
        while 'LastEvaluatedKey' in response:
            lastItem = response['LastEvaluatedKey']
            if providerId != '0':
                response = dynamodb.query(
                    TableName="TuCita247",
                    IndexName="TuCita247_Index",
                    ReturnConsumedCapacity='TOTAL',
                    ExclusiveStartKey= lastItem,
                    KeyConditionExpression='GSI1PK = :gsi1pk AND GSI1SK BETWEEN :gsi1sk_ini AND :gsi1sk_fin',
                    FilterExpression='#t = :appoType' if status == '1' else '#t >= :appoType',
                    ExpressionAttributeNames={'#t': 'TYPE'},
                    ExpressionAttributeValues={
                        ':gsi1pk': {'S': 'BUS#' + businessId + '#LOC#' + locationId + '#PRO#' + providerId},
                        ':gsi1sk_ini': {'S': str(status) +'#DT#' + dateAppoIni},
                        ':gsi1sk_fin': {'S': str(status) +'#DT#' + dateAppoFin},
                        ':appoType': {'N': str(appoType)}
                    }
                )
            else:
                response = dynamodb.query(
                    TableName="TuCita247",
                    IndexName="TuCita247_Index09",
                    ReturnConsumedCapacity='TOTAL',
                    ExclusiveStartKey= lastItem,
                    KeyConditionExpression='GSI9PK = :gsi9pk AND GSI9SK BETWEEN :gsi9sk_ini AND :gsi9sk_fin',
                    FilterExpression='#t = :appoType' if status == '1' else '#t >= :appoType',
                    ExpressionAttributeNames={'#t': 'TYPE'},
                    ExpressionAttributeValues={
                        ':gsi9pk': {'S': 'BUS#' + businessId + '#LOC#' + locationId},
                        ':gsi9sk_ini': {'S': str(status) +'#DT#' + dateAppoIni},
                        ':gsi9sk_fin': {'S': str(status) +'#DT#' + dateAppoFin},
                        ':appoType': {'N': str(appoType)}
                    }
                )
            for row in json_dynamodb.loads(response['Items']):
                recordset = {
                    'BusinessId': businessId,
                    'LocationId': locationId,
                    'ProviderId': row['GSI1PK'].replace('BUS#'+businessId+'#LOC#'+locationId+'#PRO#',''),
                    'BufferTime': findService(row['SERVICEID'], services),
                    'AppointmentId': row['PKID'].replace('APPO#',''),
                    'ClientId': row['GSI2PK'].replace('CUS#',''),
                    'Name': row['NAME'],
                    'Phone': row['PHONE'],
                    'Provider': findProvider(row['GSI1PK'].replace('BUS#'+businessId+'#LOC#'+locationId+'#PRO#',''), providers, count),
                    'Service': findServiceName(row['SERVICEID'], services, counts),
                    'OnBehalf': row['ON_BEHALF'],
                    'Guests': row['PEOPLE_QTY'] if 'PEOPLE_QTY' in row else 0,
                    'Door': row['DOOR'] if 'DOOR' in row else '',
                    'Disability': row['DISABILITY'] if 'DISABILITY' in row else 0,
                    'Type': row['TYPE'] if 'TYPE' in row else 0,
                    'DateAppo': row['DATE_APPO'],
                    'Unread': row['UNREAD'] if 'UNREAD' in row else 0,
                    'CheckInTime': row['TIMECHEK'] if 'TIMECHEK' in row else '',
                    'Purpose': row['PURPOSE'] if 'PURPOSE' in row else '',
                    'QrCode': row['QRCODE'] if 'QRCODE' in row else '',
                    'DateTrans': row['DATE_TRANS'] if 'DATE_TRANS' in row else today.strftime("%Y-%m-%d-%H-%M"),
                    'Status': row['STATUS']
                }
                record.append(recordset)
        
        resultSet = { 
            'Code': 200,
            'Appos': record
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
            "access-control-allow-origin" : cors
        },
        'body' : body
    }
    return response