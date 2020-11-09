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
        dateAppo = event['pathParameters']['dateAppo']

        country_date = dateutil.tz.gettz('America/Puerto_Rico')
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
        
        existe = 0
        for row in json_dynamodb.loads(response['Items']):
            if row['PKID'] != '':
                existe = 1
                table = dynamodb.Table('TuCita247')
                response = table.delete_item(
                    Key={
                        'PKID': 'LOC#' + locationId + '#PRO#' + providerId + '#DT#' + dateAppo[0:10],
                        'SKID': 'HR#' + dateAppo[-5:]
                    }
                )
        
        if existe == 0:
            table = dynamodb.Table('TuCita247')
            response = table.put_item(
                TableName='TuCita247',
                Item={
                    'PKID': 'LOC#' + locationId + '#PRO#' + providerId + '#DT#' + dateAppo[0:10],
                    'SKID': 'HR#' + dateAppo[-5:],
                    # 'SERVICEID': '',
                    'AVAILABLE': 1,
                    # 'CUSTOMER_PER_TIME': 0,
                    # 'TIME_SERVICE': 1,
                    'CANCEL': 0
                }
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