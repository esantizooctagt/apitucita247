import sys
import json
import logging

import os

import datetime
import dateutil.tz
from datetime import timezone

import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr
from dynamodb_json import json_util as json_dynamodb

REGION='us-east-1'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.client('dynamodb', region_name=REGION)
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def getKey(obj):
  return obj['DateAppo']

def lambda_handler(event, context):    
    try:
        customerId = event['pathParameters']['customerId']
        typeAppo = int(event['pathParameters']['typeAppo'])
        dateAppo = event['pathParameters']['dateAppo']
        
        data = json.loads(event['body'])
        lastItem = data['lastItem'] if 'lastItem' in data else  '_'
        if lastItem != '_':
            lastItem = json.loads(lastItem)
        lastItemPrev = lastItem
        country_date = dateutil.tz.gettz('America/Puerto_Rico')
        today = datetime.datetime.now(tz=country_date)
        dateOpe = (today + datetime.timedelta(hours=-6)).strftime("%Y-%m-%d-%H-00")
        dateYest = (today + datetime.timedelta(days=-1)).strftime("%Y-%m-%d-23-59")
        
        if typeAppo == 0:
            if lastItem == '_':
                details = dynamodb.query(
                    TableName="TuCita247",
                    IndexName="TuCita247_Index10",
                    ReturnConsumedCapacity='TOTAL',
                    FilterExpression='#s <= :status',
                    ExpressionAttributeNames={'#s': 'STATUS'},
                    KeyConditionExpression='GSI10PK = :customerId AND GSI10SK >= :today',
                    ExpressionAttributeValues={
                        ':customerId': {'S': 'CUS#' + customerId},
                        ':today': {'S': dateOpe},
                        ':status': {'N': str(3)}
                    }
                    # Limit=5
                )
            else:
                details = dynamodb.query(
                    TableName="TuCita247",
                    IndexName="TuCita247_Index10",
                    ReturnConsumedCapacity='TOTAL',
                    FilterExpression='#s <= :status',
                    ExpressionAttributeNames={'#s': 'STATUS'},
                    ExclusiveStartKey=lastItem,
                    KeyConditionExpression='GSI10PK = :customerId AND GSI10SK >= :today',
                    ExpressionAttributeValues={
                        ':customerId': {'S': 'CUS#' + customerId},
                        ':today': {'S': dateOpe},
                        ':status': {'N': str(3)}
                    }
                    # Limit=5
                )

        else:
            if lastItem == '_':
                details = dynamodb.query(
                    TableName="TuCita247",
                    IndexName="TuCita247_Index10",
                    ReturnConsumedCapacity='TOTAL',
                    ScanIndexForward=False,
                    KeyConditionExpression='GSI10PK = :customerId AND GSI10SK < :dateYest',
                    ExpressionAttributeValues={
                        ':customerId': {'S': 'CUS#' + customerId},
                        ':dateYest': {'S': dateYest}
                    },
                    Limit=5
                )
            else:
                details = dynamodb.query(
                    TableName="TuCita247",
                    IndexName="TuCita247_Index10",
                    ReturnConsumedCapacity='TOTAL',
                    ScanIndexForward=False,
                    ExclusiveStartKey=lastItem,
                    KeyConditionExpression='GSI10PK = :customerId AND GSI10SK < :dateYest',
                    ExpressionAttributeValues={
                        ':customerId': {'S': 'CUS#' + customerId},
                        ':dateYest': {'S': dateYest}
                    },
                    Limit=5
                )

        recordset = {}
        record = []
        lastItem = ''
        lastItem = details['LastEvaluatedKey'] if 'LastEvaluatedKey' in details else  ''
        for item in json_dynamodb.loads(details['Items']):
            locs = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :businessId AND SKID = :locationId',
                ExpressionAttributeValues={
                    ':businessId': {'S': 'BUS#' + item['GSI1PK'].split('#')[1]},
                    ':locationId': {'S': 'LOC#' + item['GSI1PK'].split('#')[3] }
                },
                Limit = 1
            )
            ManualCheckIn = 0
            for locations in json_dynamodb.loads(locs['Items']):
                Address = locations['ADDRESS']
                ManualCheckIn = int(locations['MANUAL_CHECK_OUT'])
                TimeZone = locations['TIME_ZONE'] if 'TIME_ZONE' in locations else 'America/Puerto_Rico'

            bus = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :businessId AND SKID = :meta',
                ExpressionAttributeValues={
                    ':businessId': {'S': 'BUS#' + item['GSI1PK'].split('#')[1]},
                    ':meta': {'S': 'METADATA' }
                },
                Limit = 1
            )
            for business in json_dynamodb.loads(bus['Items']):
                Name = business['NAME']

            servs = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :businessId AND begins_with(SKID , :serv)',
                ExpressionAttributeValues={
                    ':businessId': {'S': 'BUS#' + item['GSI1PK'].split('#')[1]},
                    ':serv': {'S': 'SER#' }
                }
            )
            count = 0
            servName = ''
            for serv in json_dynamodb.loads(servs['Items']):
                count = count + 1
                if serv['SKID'].replace('SER#','') == item['SERVICEID']:
                    servName = serv['NAME']
            if count == 1:
                servName = ''
            
            provs =  dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :key AND begins_with(SKID , :provs)',
                ExpressionAttributeValues={
                    ':key': {'S': 'BUS#' + item['GSI1PK'].split('#')[1] + '#LOC#'+item['GSI1PK'].split('#')[3]},
                    ':provs': {'S': 'PRO#' }
                }
            )
            countp = 0
            provName = ''
            for prov in json_dynamodb.loads(provs['Items']):
                countp = countp + 1
                if prov['SKID'].replace('PRO#','') == item['GSI1PK'].split('#')[5]:
                    provName = prov['NAME']
            if countp == 1:
                provName = ''
            
            aplica = 1
            if typeAppo == 0 and ManualCheckIn == 1 and item['STATUS'] == 3:
                aplica = 0
            if aplica == 1:
                recordset = {
                    'AppointmentId': item['PKID'].replace('APPO#',''),
                    'Status': item['STATUS'],
                    'Address': Address,
                    'NameBusiness': Name,
                    'Name': item['NAME'],
                    'Phone': item['PHONE'],
                    'DateAppo': item['DATE_APPO'],
                    'Door': item['DOOR'] if 'DOOR' in item else '',
                    'OnBehalf': item['ON_BEHALF'] if 'ON_BEHALF' in item else 0,
                    'Comments': item['COMMENTS'] if 'COMMENTS' in item else '',
                    'PeopleQty': item['PEOPLE_QTY'] if 'PEOPLE_QTY' in item else 0,
                    'QRCode': item['QRCODE'] if 'QRCODE' in item else '',
                    'Disability': item['DISABILITY'] if 'DISABILITY' in item else 0,
                    'UnRead': item['UNREAD'] if 'UNREAD' in item else '',
                    'Ready': item['READY'] if 'READY' in item else 0,
                    'ServName': servName,
                    'ProvName': provName,
                    'ManualCheckOut': ManualCheckIn,
                    'TimeZone': TimeZone
                }
                record.append(recordset)

        if typeAppo != 0:
            if lastItemPrev == '_':
                completed = dynamodb.query(
                    TableName="TuCita247",
                    IndexName="TuCita247_Index10",
                    ReturnConsumedCapacity='TOTAL',
                    FilterExpression='#s >= :status',
                    ExpressionAttributeNames={'#s': 'STATUS'},
                    KeyConditionExpression='GSI10PK = :customerId AND GSI10SK >= :today',
                    ExpressionAttributeValues={
                        ':customerId': {'S': 'CUS#' + customerId},
                        ':today': {'S': dateOpe},
                        ':status': {'N': str(3)}
                    }
                )
                recordset = {}
                for item in json_dynamodb.loads(completed['Items']):
                    locs = dynamodb.query(
                        TableName="TuCita247",
                        ReturnConsumedCapacity='TOTAL',
                        KeyConditionExpression='PKID = :businessId AND SKID = :locationId',
                        ExpressionAttributeValues={
                            ':businessId': {'S': 'BUS#' + item['GSI1PK'].split('#')[1]},
                            ':locationId': {'S': 'LOC#' + item['GSI1PK'].split('#')[3] }
                        },
                        Limit = 1
                    )
                    ManualCheckIn = 0
                    for locations in json_dynamodb.loads(locs['Items']):
                        Address = locations['ADDRESS']
                        ManualCheckIn = int(locations['MANUAL_CHECK_OUT'])
                        TimeZone = locations['TIME_ZONE'] if 'TIME_ZONE' in locations else 'America/Puerto_Rico'

                    bus = dynamodb.query(
                        TableName="TuCita247",
                        ReturnConsumedCapacity='TOTAL',
                        KeyConditionExpression='PKID = :businessId AND SKID = :meta',
                        ExpressionAttributeValues={
                            ':businessId': {'S': 'BUS#' + item['GSI1PK'].split('#')[1]},
                            ':meta': {'S': 'METADATA' }
                        },
                        Limit = 1
                    )
                    for business in json_dynamodb.loads(bus['Items']):
                        Name = business['NAME']

                    servs = dynamodb.query(
                        TableName="TuCita247",
                        ReturnConsumedCapacity='TOTAL',
                        KeyConditionExpression='PKID = :businessId AND begins_with(SKID , :serv)',
                        ExpressionAttributeValues={
                            ':businessId': {'S': 'BUS#' + item['GSI1PK'].split('#')[1]},
                            ':serv': {'S': 'SER#' }
                        }
                    )
                    count = 0
                    servName = ''
                    for serv in json_dynamodb.loads(servs['Items']):
                        count = count + 1
                        if serv['SKID'].replace('SER#','') == item['SERVICEID']:
                            servName = serv['NAME']
                    if count == 1:
                        servName = ''

                    provs =  dynamodb.query(
                        TableName="TuCita247",
                        ReturnConsumedCapacity='TOTAL',
                        KeyConditionExpression='PKID = :key AND begins_with(SKID , :provs)',
                        ExpressionAttributeValues={
                            ':key': {'S': 'BUS#' + item['GSI1PK'].split('#')[1] + '#LOC#'+item['GSI1PK'].split('#')[3]},
                            ':provs': {'S': 'PRO#' }
                        }
                    )
                    countp = 0
                    provName = ''
                    for prov in json_dynamodb.loads(provs['Items']):
                        countp = countp + 1
                        if prov['SKID'].replace('PRO#','') == item['GSI1PK'].split('#')[5]:
                            provName = prov['NAME']
                    if countp == 1:
                        provName = ''

                    aplica = 1
                    if ManualCheckIn == 0 and item['STATUS'] == 3:
                        aplica = 0
                    if aplica == 1:
                        recordset = {
                            'AppointmentId': item['PKID'].replace('APPO#',''),
                            'Status': item['STATUS'],
                            'Address': Address,
                            'NameBusiness': Name,
                            'Name': item['NAME'],
                            'Phone': item['PHONE'],
                            'DateAppo': item['DATE_APPO'],
                            'Door': item['DOOR'] if 'DOOR' in item else '',
                            'OnBehalf': item['ON_BEHALF'] if 'ON_BEHALF' in item else 0,
                            'Comments': item['COMMENTS'] if 'COMMENTS' in item else '',
                            'PeopleQty': item['PEOPLE_QTY'] if 'PEOPLE_QTY' in item else 0,
                            'QRCode': item['QRCODE'] if 'QRCODE' in item else '',
                            'Disability': item['DISABILITY'] if 'DISABILITY' in item else 0,
                            'UnRead': item['UNREAD'] if 'UNREAD' in item else '',
                            'Ready': item['READY'] if 'READY' in item else 0,
                            'ServName': servName,
                            'ManualCheckOut': ManualCheckIn,
                            'TimeZone': TimeZone,
                            'ProvName': provName
                        }
                        record.append(recordset)

        record.sort(key=getKey)
        statusCode = 200
        body = json.dumps({'Appointments': record, 'LastItem': lastItem, 'Code': 200})

    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message': str(e), 'Code': 500})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : "*"
        },
        'body' : body
    }
    return response