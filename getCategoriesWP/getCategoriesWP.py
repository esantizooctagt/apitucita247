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
    records =[]
    stage = event['headers']
    if stage['origin'] != "http://tucita247.local":
        if stage['origin'] == "https://tucita247.com":
            cors = os.environ['prodCors']
        else:
            cors = os.environ['prodWCors']
    else:
        cors = os.environ['devCors']
    # cors = "*"
        
    try:
        language = event['pathParameters']['language']
        e = {'#s': 'STATUS'}
        f = '#s = :stat'
        response = dynamodb.query(
            TableName="TuCita247",
            IndexName="TuCita247_Index",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='GSI1PK = :key AND GSI1SK = :key',
            ExpressionAttributeNames=e,
            FilterExpression=f,
            ExpressionAttributeValues={
                ':key': {'S': 'CAT#'},
                ':stat' : {'N': '1'}
            },
        )
        recordset = {
            'id': '',
            'text': 'Business category' if language.upper() == 'EN' else 'Categoria del Negocio',
            'class': 'l1',
            'icono': ''
        }
        records.append(recordset)
        cat = ''
        for row in json_dynamodb.loads(response['Items']):
            recordset = {
                'id': row['PKID'],
                'text': row['NAME_ENG'] if language.upper() == 'EN' else row['NAME_ESP'],
                'class': 'l1',
                'icono': ''
            }
            cat = row['NAME_ENG'] if language.upper() == 'EN' else row['NAME_ESP']
            records.append(recordset)
            subs = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :key AND begins_with(SKID , :sub)',
                ExpressionAttributeNames=e,
                FilterExpression=f,
                ExpressionAttributeValues={
                    ':key': {'S': row['PKID']},
                    ':stat' : {'N': '1'},
                    ':sub' :{'S': 'SUB#'}
                },
            )
            for sub in json_dynamodb.loads(subs['Items']):
                recordset = {
                    'id': sub['PKID']+'#'+sub['SKID'],
                    'text': cat + ' - ' + sub['NAME_ENG'] if language.upper() == 'EN' else sub['NAME_ESP'],
                    'class': 'l2',
                    'icono': ''
                }
                records.append(recordset)
        
        statusCode = 200
        body = json.dumps(records)
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