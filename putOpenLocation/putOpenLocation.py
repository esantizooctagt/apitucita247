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
dynamodbQry = boto3.client('dynamodb', region_name=REGION)
lambdaInv = boto3.client('lambda')
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def findTimeZone(businessId, locationId):
    timeZone='America/Puerto_Rico'
    locZone = dynamodbQry.query(
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
        statusCode = ''
        locationId = event['pathParameters']['id']
        businessId = event['pathParameters']['businessId']
        
        country_date = dateutil.tz.gettz(findTimeZone(businessId, locationId))
        today = datetime.datetime.now(tz=country_date)
        dateOpe = today.strftime("%Y-%m-%d-%H-%M-%S")

        table = dynamodb.Table('TuCita247')
        response = table.update_item(
            Key={
                'PKID': 'BUS#' + businessId,
                'SKID': 'LOC#' + locationId
            },
            UpdateExpression="SET OPEN_DATE = :actualDate, PEOPLE_CHECK_IN = :qty, #o = :open",
            ExpressionAttributeValues= {':actualDate': dateOpe, ':qty': 0, ':open': 1, ':initVal': 0},
            ExpressionAttributeNames={'#o': 'OPEN'},
            ConditionExpression='attribute_not_exists(#o) OR #o = :initVal',
            ReturnValues="UPDATED_NEW"
        )

        data = {
            'BusinessId': businessId,
            'LocationId': locationId,
            'Tipo': 'OPEN'
        }
        lambdaInv.invoke(
            FunctionName='PostMessages',
            InvocationType='Event',
            Payload=json.dumps(data)
        )

        statusCode = 200
        body = json.dumps({'Message': 'Service opened successfully', 'Code': 200, 'Business': json_dynamodb.loads(response['Attributes'])})

        logger.info(response)
        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Message': 'Error on opened location', 'Code': 500})
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