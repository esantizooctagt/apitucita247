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
        dateAppoIni = event['pathParameters']['dateAppoIni']

        if providerId != '0':
            response = dynamodb.query(
                TableName="TuCita247",
                IndexName="TuCita247_Index",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='GSI1PK = :gsi1pk AND GSI1SK = :gsi1sk_ini',
                ExpressionAttributeValues={
                    ':gsi1pk': {'S': 'BUS#' + businessId + '#LOC#' + locationId + '#PRO#' + providerId},
                    ':gsi1sk_ini': {'S': '1#DT#' + dateAppoIni}
                }
            )
        else:
            response = dynamodb.query(
                TableName="TuCita247",
                IndexName="TuCita247_Index09",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='GSI9PK = :gsi9pk AND GSI9SK = :gsi9sk_ini',
                ExpressionAttributeValues={
                    ':gsi9pk': {'S': 'BUS#' + businessId + '#LOC#' + locationId},
                    ':gsi9sk_ini': {'S': '1#DT#' + dateAppoIni}
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
                'Status': row['STATUS'],
                'Comments': row['COMMENTS'] if 'COMMENTS' in row else ''
            }
            record.append(recordset)
        
        lastItem = ''
        while 'LastEvaluatedKey' in response:
            lastItem = json_dynamodb.loads(response['LastEvaluatedKey'])
            if providerId != '0':
                response = dynamodb.query(
                    TableName="TuCita247",
                    IndexName="TuCita247_Index",
                    ExclusiveStartKey= lastItem,
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='GSI1PK = :gsi1pk AND GSI1SK = :gsi1sk_ini',
                    ExpressionAttributeValues={
                        ':gsi1pk': {'S': 'BUS#' + businessId + '#LOC#' + locationId + '#PRO#' + providerId},
                        ':gsi1sk_ini': {'S': '1#DT#' + dateAppoIni}
                    }
                )
            else:
                response = dynamodb.query(
                    TableName="TuCita247",
                    IndexName="TuCita247_Index09",
                    ExclusiveStartKey= lastItem,
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='GSI9PK = :gsi9pk AND GSI9SK = :gsi9sk_ini',
                    ExpressionAttributeValues={
                        ':gsi9pk': {'S': 'BUS#' + businessId + '#LOC#' + locationId},
                        ':gsi9sk_ini': {'S': '1#DT#' + dateAppoIni}
                    }
                )
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
                    'Status': row['STATUS'],
                    'Comments': row['COMMENTS'] if 'COMMENTS' in row else ''
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