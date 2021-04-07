import sys
import logging
import json

import boto3
import botocore.exceptions
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
lambdaInv = boto3.client('lambda')
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def findTimeZone(businessId, locationId):
    timeZone='America/Puerto_Rico'
    locZone = dynamodbQuery.query(
        TableName="TuCita247",
        ReturnConsumedCapacity='TOTAL',
        KeyConditionExpression='PKID = :key AND SKID = :skey',
        ExpressionAttributeValues={
            ':key': {'S': 'BUS#'+businessId},
            ':skey': {'S': 'LOC#'+locationId}
        }
    )
    for timeLoc in json_dynamodb.loads(locZone['Items']):
        timeZone = timeLoc['TIME_ZONE'] if 'TIME_ZONE' in timeLoc else 'America/Puerto_Rico'
    return timeZone

def lambda_handler(event, context):
    try:
        statusCode = ''
        dataId = ''
        appoData = ''
        businessId = ''
        locationId = ''
        providerId = ''
        locId = ''
        busId = ''
        guests = 0

        appointmentId = event['pathParameters']['appointmentId']
        dateAppo = event['pathParameters']['dateAppo']
        status = 5
        response = dynamodbQuery.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :appointmentId AND SKID = :appointmentId',
            ExpressionAttributeValues={
                ':appointmentId': {'S': 'APPO#' + appointmentId}
            }
        )
        for row in json_dynamodb.loads(response['Items']):
            dataId = row['GSI1PK']
            guests = int(row['PEOPLE_QTY'])
            appoData = str(row['DATE_APPO'])[0:10]+'#APPO#'+appointmentId
            if dataId != '':
                busId = dataId.split('#')[1]
                locId = dataId.split('#')[3]
                businessId = 'BUS#'+dataId.split('#')[1]+'#5'
                locationId = 'BUS#'+dataId.split('#')[1]+'#LOC#'+dataId.split('#')[3]+'#5'
                providerId = 'BUS#'+dataId.split('#')[1]+'#LOC#'+dataId.split('#')[3]+'#PRO#'+dataId.split('#')[5]+'#5'
                keyUpd = 'LOC#'+dataId.split('#')[3]+'#PRO#'+dataId.split('#')[5]+'#DT#'+dateAppo[0:10]
        
        country_date = dateutil.tz.gettz(findTimeZone(busId, locId))
        today = datetime.datetime.now(tz=country_date)
        dateOpe = today.strftime("%Y-%m-%d-%H-%M-%S")

        items = []
        recordset = {
            "Update": {
                "TableName": "TuCita247",
                "Key": {
                    "PKID": {"S": 'APPO#' + appointmentId}, 
                    "SKID": {"S": 'APPO#' + appointmentId}, 
                },
                "UpdateExpression": "SET #s = :status, MODIFIED_DATE = :mod_date, GSI1SK = :key01, GSI9SK = :key01, GSI2SK = :key01, TIMECANCEL = :dateope, GSI5PK = :pkey05, GSI5SK = :skey05, GSI6PK = :pkey06, GSI6SK = :skey06, GSI7PK = :pkey07, GSI7SK = :skey07, STATUS_CANCEL = :statCancel REMOVE GSI8PK, GSI8SK",
                "ExpressionAttributeValues": { 
                    ":status": {"N": str(status)}, 
                    ":key01": {"S": str(status) + '#DT#' + str(dateAppo)}, 
                    ":pkey05": {"S": businessId}, 
                    ":skey05": {"S": appoData}, 
                    ":pkey06": {"S": locationId}, 
                    ":skey06": {"S": appoData}, 
                    ":pkey07": {"S": providerId}, 
                    ":skey07": {"S": appoData},
                    ":dateope": {"S": dateOpe},
                    ":statCancel": {"N": str(2)},
                    ":mod_date": {"S": str(dateOpe)}
                },
                "ExpressionAttributeNames": {'#s': 'STATUS'},
                "ConditionExpression": "attribute_exists(PKID) AND attribute_exists(SKID)",
                "ReturnValuesOnConditionCheckFailure": "ALL_OLD" 
            }
        }
        items.append(recordset)

        recordset = {
            "Put": {
                "TableName": "TuCita247",
                "Item": {
                    "PKID": {"S": 'LOG#' + str(dateOpe)[0:10]},
                    "SKID": {"S": 'APPO#' + appointmentId + '#' + str(dateOpe)},
                    "DATE_APPO": {"S": str(dateOpe)},
                    "STATUS": {"N": str(status)}
                },
                "ConditionExpression": "attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
                }
            }
        items.append(recordset)

        logger.info(items)
        response = dynamodbQuery.transact_write_items(
            TransactItems = items
        )
        data = {
            'BusinessId': busId,
            'LocationId': locId,
            'AppId': appointmentId,
            'CustomerId': '',
            'Tipo': 'CANCEL'
        }
        if dateOpe[0:10] == dateAppo[0:10]:
            lambdaInv.invoke(
                FunctionName='PostMessages',
                InvocationType='Event',
                Payload=json.dumps(data)
            )

        statusCode = 200
        body = json.dumps({'Message': 'Appointment updated successfully', 'Code': 200})

        logger.info(response)
        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Message': 'Error on update appointment', 'Code': 500})

    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message': 'Error on request try again ' + str(e), 'Code': 500})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : "*"
        },
        'body' : body
    }
    return response