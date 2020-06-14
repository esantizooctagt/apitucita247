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

dynamodb = boto3.client('dynamodb', region_name='us-east-1')
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
        dateAppoIni = event['pathParameters']['dateAppoIni']
        dateAppoFin = event['pathParameters']['dateAppoFin']
        status = event['pathParameters']['status']
        lastItem = event['pathParameters']['lastItem']
        typeAppo = event['pathParameters']['type']
        appoId = event['pathParameters']['appoId']

        if lastItem == '_':
            lastItem = ''
            if typeAppo != '_':
                n = {'#t': 'TYPE'}
                f = '#t = :type'
                response = dynamodb.query(
                    TableName="TuCita247",
                    IndexName="TuCita247_Index",
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='GSI1PK = :gsi1pk AND GSI1SK BETWEEN :gsi1sk_ini AND :gsi1sk_fin',
                    ExpressionAttributeNames=n,
                    FilterExpression=f,
                    ExpressionAttributeValues={
                        ':gsi1pk': {'S': 'BUS#' + businessId + '#LOC#' + locationId},
                        ':gsi1sk_ini': {'S': str(status) +'#DT#' + dateAppoIni},
                        ':gsi1sk_fin': {'S': str(status) +'#DT#' + dateAppoFin},
                        ':type': {'N': str(typeAppo)}
                    },
                    Limit = 30
                )
            else:
                response = dynamodb.query(
                    TableName="TuCita247",
                    IndexName="TuCita247_Index",
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='GSI1PK = :gsi1pk AND GSI1SK BETWEEN :gsi1sk_ini AND :gsi1sk_fin',
                    ExpressionAttributeValues={
                        ':gsi1pk': {'S': 'BUS#' + businessId + '#LOC#' + locationId},
                        ':gsi1sk_ini': {'S': str(status) +'#DT#' + dateAppoIni},
                        ':gsi1sk_fin': {'S': str(status) +'#DT#' + dateAppoFin}
                    },
                    Limit = 30
                )
        else:
            lastItem = {'GSI1PK': {'S': 'BUS#' + businessId + '#LOC#' + locationId },'GSI1SK': {'S': str(status) + '#DT#' + lastItem }, 'SKID': {'S': 'APPO#' + appoId}, 'PKID': {'S': 'APPO#' + appoId}}
            if typeAppo != '_':
                n = {'#t': 'TYPE'}
                f = '#t = :type'
                response = dynamodb.query(
                    TableName="TuCita247",
                    IndexName="TuCita247_Index",
                    ReturnConsumedCapacity='TOTAL',
                    ExclusiveStartKey= lastItem,
                    KeyConditionExpression='GSI1PK = :gsi1pk AND GSI1SK BETWEEN :gsi1sk_ini AND :gsi1sk_fin',
                    FilterExpression=f,
                    ExpressionAttributeNames=n,
                    ExpressionAttributeValues={
                        ':gsi1pk': {'S': 'BUS#' + businessId + '#LOC#' + locationId},
                        ':gsi1sk_ini': {'S': str(status) +'#DT#' + dateAppoIni},
                        ':gsi1sk_fin': {'S': str(status) +'#DT#' + dateAppoFin},
                        ':type': {'N': str(typeAppo)}
                    },
                    Limit = 30
                )
            else:
                response = dynamodb.query(
                    TableName="TuCita247",
                    IndexName="TuCita247_Index",
                    ReturnConsumedCapacity='TOTAL',
                    ExclusiveStartKey= lastItem,
                    KeyConditionExpression='GSI1PK = :gsi1pk AND GSI1SK BETWEEN :gsi1sk_ini AND :gsi1sk_fin',
                    ExpressionAttributeValues={
                        ':gsi1pk': {'S': 'BUS#' + businessId + '#LOC#' + locationId},
                        ':gsi1sk_ini': {'S': str(status) +'#DT#' + dateAppoIni},
                        ':gsi1sk_fin': {'S': str(status) +'#DT#' + dateAppoFin}
                    },
                    Limit = 30
                )

        record = []
        recordset = {}
        for row in json_dynamodb.loads(response['Items']):
            recordset = {
                'BusinessId': businessId,
                'LocationId': locationId,
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
            lastItem = lastItem['GSI1SK']

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