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
        dateAppo = event['pathParameters']['dateAppo']
        dateAppoFin = event['pathParameters']['dateAppoFin']
        status = event['pathParameters']['status']
        statusFin = event['pathParameters']['statusFin']

        response = dynamodb.query(
            TableName="TuCita247",
            IndexName="TuCita247_Index",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='GSI1PK = :gsi1pk AND GSI1SK BETWEEN :gsi1sk_ini AND :gsi1sk_fin',
            ExpressionAttributeValues={
                ':gsi1pk': {'S': 'BUS#' + businessId + '#LOC#' + locationId},
                ':gsi1sk_ini': {'S': str(status) +'#DT#' + dateAppo},
                ':gsi1sk_fin': {'S': str(statusFin) +'#DT#' + dateAppoFin}
            }
        )
        
        record = []
        locations = json_dynamodb.loads(response['Items'])
        for row in locations:
            recordset = {
                'BusinessId': businessId,
                'LocationId': locationId,
                'AppointmentId': row['PKID'].replace('APPO#',''),
                'ClientId': row['GSI2PK'].replace('CUS#','')[0:2],
                'Name': row['NAME'],
                'Phone': row['PHONE'],
                'OnBehalf': row['ON_BEHALF'],
                'PeopleQty': row['PEOPLE_QTY'],
                'Type': row['TYPE'],
                'DateAppo': row['DATE_APPO'],
                'Status': row['STATUS']
            }
            record.append(recordset)

        statusCode = 200
        body = json.dumps({'Code': 200, 'Appos': record})
    
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