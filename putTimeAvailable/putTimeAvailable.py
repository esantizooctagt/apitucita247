import sys
import logging
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

        country_date = dateutil.tz.gettz(findTimeZone(businessId, locationId))
        today = datetime.datetime.now(tz=country_date)
        dateOpe = today.strftime("%Y-%m-%d-%H-%M-%S")

        response = dynamodbQuery.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :key01 AND SKID = :key02',
            ExpressionAttributeValues={
                ':key01': {'S': 'LOC#' + locationId + '#PRO#' + providerId + '#DT#' + dateAppo[0:10]},
                ':key02': {'S': 'HR#' + dateAppo[-5:]}
            }
        )
        entro = 0
        for row in json_dynamodb.loads(response['Items']):
            entro = 1
            if row['PKID'] != '':
                table = dynamodb.Table('TuCita247')
                response = table.update_item(
                    Key={
                        'PKID': 'LOC#' + locationId + '#PRO#' + providerId + '#DT#' + dateAppo[0:10],
                        'SKID': 'HR#' + dateAppo[-5:]
                    },
                    UpdateExpression='SET CANCEL = :cancel, AVAILABLE = :available',
                    ExpressionAttributeValues= {
                        ':cancel': 0,
                        ':available': 1
                    }
                )
        if entro == 0:
            table = dynamodb.Table('TuCita247')
            response = table.put_item(
                TableName='TuCita247',
                Item={
                    'PKID': 'LOC#' + locationId + '#PRO#' + providerId + '#DT#' + dateAppo[0:10],
                    'SKID': 'HR#' + dateAppo[-5:],
                    'AVAILABLE': 1,
                    'CANCEL': 0
                }
            )
        
        #REMOVE FROM QEUE
        data = {
            'BusinessId': businessId,
            'LocationId': locationId,
            'AppId': '',
            'CustomerId': '',
            'DateAppo': dateAppo[0:10],
            'Hour': dateAppo[-5:],
            'Available': 1,
            'Tipo': 'AVAILABLE'
        }
        lambdaInv.invoke(
            FunctionName='PostMessages',
            InvocationType='Event',
            Payload=json.dumps(data)
        )

        statusCode = 200
        body = json.dumps({'Message': 'Citas bucket open successfully', 'Code': 200})

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