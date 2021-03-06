import sys
import logging
import json

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
        status = event['pathParameters']['status']

        initDate = dateAppo[0:10]+'-00-00'
        n = {'#t': 'TYPE'}
        f = '#t = :type'

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
        for serv in json_dynamodb.loads(servs['Items']):
            recordset = {
                'ServiceId': serv['SKID'].replace('SER#',''),
                'BufferTime': serv['BUFFER_TIME']
            }
            services.append(recordset)

        if providerId != '0':
            response = dynamodb.query(
                TableName="TuCita247",
                IndexName="TuCita247_Index",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='GSI1PK = :gsi1pk AND GSI1SK BETWEEN :initDate AND :gsi1sk',
                ExpressionAttributeNames=n,
                FilterExpression=f,
                ExpressionAttributeValues={
                    ':gsi1pk': {'S': 'BUS#' + businessId + '#LOC#' + locationId + '#PRO#' + providerId},
                    ':gsi1sk': {'S': str(status) +'#DT#' + dateAppo},
                    ':initDate': {'S': str(status) +'#DT#' + initDate},
                    ':type': {'N': str(1)}
                }
            )
        else:
            response = dynamodb.query(
                TableName="TuCita247",
                IndexName="TuCita247_Index09",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='GSI9PK = :gsi9pk AND GSI9SK BETWEEN :initDate AND :gsi9sk',
                ExpressionAttributeNames=n,
                FilterExpression=f,
                ExpressionAttributeValues={
                    ':gsi9pk': {'S': 'BUS#' + businessId + '#LOC#' + locationId},
                    ':gsi9sk': {'S': str(status) +'#DT#' + dateAppo},
                    ':initDate': {'S': str(status) +'#DT#' + initDate},
                    ':type': {'N': str(1)}
                }
            )

        record = []
        recordset = {}
        for row in json_dynamodb.loads(response['Items']):
            recordset = {
                'BusinessId': businessId,
                'LocationId': locationId,
                'ProviderId': row['GSI1PK'].replace('BUS#'+businessId+'#LOC#'+locationId+'#PRO#',''),
                'AppointmentId': row['PKID'].replace('APPO#',''),
                'ClientId': row['GSI2PK'].replace('CUS#',''),
                'BufferTime': findService(row['SERVICEID'], services),
                'Name': row['NAME'],
                'Phone': row['PHONE'],
                'OnBehalf': row['ON_BEHALF'],
                'Guests': row['PEOPLE_QTY'] if 'PEOPLE_QTY' in row else 0,
                'Door': row['DOOR'] if 'DOOR' in row else '',
                'Disability': row['DISABILITY'] if 'DISABILITY' in row else 0,
                'Type': row['TYPE'] if 'TYPE' in row else 0,
                'DateAppo': row['DATE_APPO'],
                'Unread': row['UNREAD'] if 'UNREAD' in row else 0,
                'CheckInTime': row['TIMECHEK'] if 'TIMECHEK' in row else '',
                'Purpose': row['PURPOSE'] if 'PURPOSE' in row else '',
                'Status': row['STATUS']
            }
            record.append(recordset)
        
        appoId = ''
        lastItem = ''
        while 'LastEvaluatedKey' in response:
            lastItem = json_dynamodb.loads(response['LastEvaluatedKey'])
            # if lastItem:
            #     appoId = lastItem['PKID'].replace('APPO#','')
            #     lastItem = lastItem['GSI1SK']
            #     lastItem = {'GSI1PK': {'S': 'BUS#' + businessId + '#LOC#' + locationId + '#PRO#' + providerId},'GSI1SK': {'S': lastItem }, 'SKID': {'S': 'APPO#' + appoId}, 'PKID': {'S': 'APPO#' + appoId}}
            if providerId != '0':
                response = dynamodb.query(
                    TableName="TuCita247",
                    IndexName="TuCita247_Index",
                    ReturnConsumedCapacity='TOTAL',
                    ExclusiveStartKey= lastItem,
                    KeyConditionExpression='GSI1PK = :gsi1pk AND GSI1SK BETWEEN :initDate AND :gsi1sk',
                    FilterExpression=f,
                    ExpressionAttributeNames=n,
                    ExpressionAttributeValues={
                        ':gsi1pk': {'S': 'BUS#' + businessId + '#LOC#' + locationId + '#PRO#' + providerId},
                        ':gsi1sk': {'S': str(status) +'#DT#' + dateAppo},
                        ':initDate': {'S': str(status) +'#DT#' + initDate},
                        ':type': {'N': str(1)}
                    },
                )
            else:
                response = dynamodb.query(
                    TableName="TuCita247",
                    IndexName="TuCita247_Index09",
                    ReturnConsumedCapacity='TOTAL',
                    ExclusiveStartKey= lastItem,
                    KeyConditionExpression='GSI9PK = :gsi9pk AND GSI9SK BETWEEN :initDate AND :gsi9sk',
                    FilterExpression=f,
                    ExpressionAttributeNames=n,
                    ExpressionAttributeValues={
                        ':gsi9pk': {'S': 'BUS#' + businessId + '#LOC#' + locationId},
                        ':gsi9sk': {'S': str(status) +'#DT#' + dateAppo},
                        ':initDate': {'S': str(status) +'#DT#' + initDate},
                        ':type': {'N': str(1)}
                    },
                )

            recordset = {}
            for row in json_dynamodb.loads(response['Items']):
                recordset = {
                    'BusinessId': businessId,
                    'LocationId': locationId,
                    'ProviderId': row['GSI1PK'].replace('BUS#'+businessId+'#LOC#'+locationId+'#PRO#',''),
                    'AppointmentId': row['PKID'].replace('APPO#',''),
                    'ClientId': row['GSI2PK'].replace('CUS#',''),
                    'BufferTime': findService(row['SERVICEID'], services),
                    'Name': row['NAME'],
                    'Phone': row['PHONE'],
                    'OnBehalf': row['ON_BEHALF'],
                    'Guests': row['PEOPLE_QTY'] if 'PEOPLE_QTY' in row else 0,
                    'Door': row['DOOR'] if 'DOOR' in row else '',
                    'Disability': row['DISABILITY'] if 'DISABILITY' in row else 0,
                    'Type': row['TYPE'] if 'TYPE' in row else 0,
                    'DateAppo': row['DATE_APPO'],
                    'Unread': row['UNREAD'] if 'UNREAD' in row else 0,
                    'CheckInTime': row['TIMECHEK'] if 'TIMECHEK' in row else '',
                    'Purpose': row['PURPOSE'] if 'PURPOSE' in row else '',
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