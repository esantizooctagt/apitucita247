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
        dateAppoIni = event['pathParameters']['dateAppoIni']
        dateAppoFin = event['pathParameters']['dateAppoFin']
        if status == '1' and typeAppo == '1':
            dateAppoIni = today.strftime("%Y-%m-%d-%H-%M")
            dateAppoFin = today.strftime("%Y-%m-%d-23-59")
        
        lastItem = event['pathParameters']['lastItem']
        appoId = event['pathParameters']['appoId']

        if lastItem == '_':
            lastItem = ''
            if typeAppo != '_':
                n = {'#t': 'TYPE'}
                f = '#t = :type'
                if providerId != '0':
                    response = dynamodb.query(
                        TableName="TuCita247",
                        IndexName="TuCita247_Index",
                        ReturnConsumedCapacity='TOTAL',
                        KeyConditionExpression='GSI1PK = :gsi1pk AND GSI1SK BETWEEN :gsi1sk_ini AND :gsi1sk_fin',
                        ExpressionAttributeNames=n,
                        FilterExpression=f,
                        ExpressionAttributeValues={
                            ':gsi1pk': {'S': 'BUS#' + businessId + '#LOC#' + locationId + '#PRO#' + providerId},
                            ':gsi1sk_ini': {'S': str(status) +'#DT#' + dateAppoIni},
                            ':gsi1sk_fin': {'S': str(status) +'#DT#' + dateAppoFin},
                            ':type': {'N': str(typeAppo)}
                        },
                        Limit = 30
                    )
                else:
                    response = dynamodb.query(
                        TableName="TuCita247",
                        IndexName="TuCita247_Index09",
                        ReturnConsumedCapacity='TOTAL',
                        KeyConditionExpression='GSI9PK = :gsi9pk AND GSI9SK BETWEEN :gsi9sk_ini AND :gsi9sk_fin',
                        ExpressionAttributeNames=n,
                        FilterExpression=f,
                        ExpressionAttributeValues={
                            ':gsi9pk': {'S': 'BUS#' + businessId + '#LOC#' + locationId },
                            ':gsi9sk_ini': {'S': str(status) +'#DT#' + dateAppoIni},
                            ':gsi9sk_fin': {'S': str(status) +'#DT#' + dateAppoFin},
                            ':type': {'N': str(typeAppo)}
                        },
                        Limit = 30
                    )
            else:
                if providerId != '0':
                    response = dynamodb.query(
                        TableName="TuCita247",
                        IndexName="TuCita247_Index",
                        ReturnConsumedCapacity='TOTAL',
                        KeyConditionExpression='GSI1PK = :gsi1pk AND GSI1SK BETWEEN :gsi1sk_ini AND :gsi1sk_fin',
                        ExpressionAttributeValues={
                            ':gsi1pk': {'S': 'BUS#' + businessId + '#LOC#' + locationId + '#PRO#' + providerId},
                            ':gsi1sk_ini': {'S': str(status) +'#DT#' + dateAppoIni},
                            ':gsi1sk_fin': {'S': str(status) +'#DT#' + dateAppoFin}
                        },
                        Limit = 30
                    )
                else:
                    response = dynamodb.query(
                        TableName="TuCita247",
                        IndexName="TuCita247_Index09",
                        ReturnConsumedCapacity='TOTAL',
                        KeyConditionExpression='GSI9PK = :gsi9pk AND GSI9SK BETWEEN :gsi9sk_ini AND :gsi9sk_fin',
                        ExpressionAttributeValues={
                            ':gsi9pk': {'S': 'BUS#' + businessId + '#LOC#' + locationId },
                            ':gsi9sk_ini': {'S': str(status) +'#DT#' + dateAppoIni},
                            ':gsi9sk_fin': {'S': str(status) +'#DT#' + dateAppoFin}
                        },
                        Limit = 30
                    )
        else:
            if typeAppo != '_':
                n = {'#t': 'TYPE'}
                f = '#t = :type'
                if providerId != '0':
                    lastItem = {'GSI1PK': {'S': 'BUS#' + businessId + '#LOC#' + locationId + '#PRO#' + providerId },'GSI1SK': {'S': str(status) + '#DT#' + lastItem }, 'SKID': {'S': 'APPO#' + appoId}, 'PKID': {'S': 'APPO#' + appoId}}
                    response = dynamodb.query(
                        TableName="TuCita247",
                        IndexName="TuCita247_Index",
                        ReturnConsumedCapacity='TOTAL',
                        ExclusiveStartKey= lastItem,
                        KeyConditionExpression='GSI1PK = :gsi1pk AND GSI1SK BETWEEN :gsi1sk_ini AND :gsi1sk_fin',
                        FilterExpression=f,
                        ExpressionAttributeNames=n,
                        ExpressionAttributeValues={
                            ':gsi1pk': {'S': 'BUS#' + businessId + '#LOC#' + locationId + '#PRO#' + providerId},
                            ':gsi1sk_ini': {'S': str(status) +'#DT#' + dateAppoIni},
                            ':gsi1sk_fin': {'S': str(status) +'#DT#' + dateAppoFin},
                            ':type': {'N': str(typeAppo)}
                        },
                        Limit = 30
                    )
                else:
                    lastItem = {'GSI9PK': {'S': 'BUS#' + businessId + '#LOC#' + locationId },'GSI9SK': {'S': str(status) + '#DT#' + lastItem }, 'SKID': {'S': 'APPO#' + appoId}, 'PKID': {'S': 'APPO#' + appoId}}
                    response = dynamodb.query(
                        TableName="TuCita247",
                        IndexName="TuCita247_Index09",
                        ReturnConsumedCapacity='TOTAL',
                        ExclusiveStartKey= lastItem,
                        KeyConditionExpression='GSI9PK = :gsi9pk AND GSI9SK BETWEEN :gsi9sk_ini AND :gsi9sk_fin',
                        FilterExpression=f,
                        ExpressionAttributeNames=n,
                        ExpressionAttributeValues={
                            ':gsi9pk': {'S': 'BUS#' + businessId + '#LOC#' + locationId},
                            ':gsi9sk_ini': {'S': str(status) +'#DT#' + dateAppoIni},
                            ':gsi9sk_fin': {'S': str(status) +'#DT#' + dateAppoFin},
                            ':type': {'N': str(typeAppo)}
                        },
                        Limit = 30
                    )
            else:
                if providerId != '0':
                    lastItem = {'GSI1PK': {'S': 'BUS#' + businessId + '#LOC#' + locationId + '#PRO#' + providerId },'GSI1SK': {'S': str(status) + '#DT#' + lastItem }, 'SKID': {'S': 'APPO#' + appoId}, 'PKID': {'S': 'APPO#' + appoId}}
                    response = dynamodb.query(
                        TableName="TuCita247",
                        IndexName="TuCita247_Index",
                        ReturnConsumedCapacity='TOTAL',
                        ExclusiveStartKey= lastItem,
                        KeyConditionExpression='GSI1PK = :gsi1pk AND GSI1SK BETWEEN :gsi1sk_ini AND :gsi1sk_fin',
                        ExpressionAttributeValues={
                            ':gsi1pk': {'S': 'BUS#' + businessId + '#LOC#' + locationId + '#PRO#' + providerId},
                            ':gsi1sk_ini': {'S': str(status) +'#DT#' + dateAppoIni},
                            ':gsi1sk_fin': {'S': str(status) +'#DT#' + dateAppoFin}
                        },
                        Limit = 30
                    )
                else:
                    lastItem = {'GSI9PK': {'S': 'BUS#' + businessId + '#LOC#' + locationId },'GSI9SK': {'S': str(status) + '#DT#' + lastItem }, 'SKID': {'S': 'APPO#' + appoId}, 'PKID': {'S': 'APPO#' + appoId}}
                    response = dynamodb.query(
                        TableName="TuCita247",
                        IndexName="TuCita247_Index09",
                        ReturnConsumedCapacity='TOTAL',
                        ExclusiveStartKey= lastItem,
                        KeyConditionExpression='GSI9PK = :gsi9pk AND GSI9SK BETWEEN :gsi9sk_ini AND :gsi9sk_fin',
                        ExpressionAttributeValues={
                            ':gsi9pk': {'S': 'BUS#' + businessId + '#LOC#' + locationId},
                            ':gsi9sk_ini': {'S': str(status) +'#DT#' + dateAppoIni},
                            ':gsi9sk_fin': {'S': str(status) +'#DT#' + dateAppoFin}
                        },
                        Limit = 30
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
        
        lastItem = ''
        appoId = '_'
        if 'LastEvaluatedKey' in response:
            lastItem = json_dynamodb.loads(response['LastEvaluatedKey'])
            appoId = lastItem['PKID'].replace('APPO#','')
            if providerId != '0':
                lastItem = lastItem['GSI1SK']
            else:
                lastItem = lastItem['GSI9SK']

        resultSet = { 
            'Code': 200,
            'lastItem': lastItem,
            'AppId': appoId,
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