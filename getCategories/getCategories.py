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

def getKey(obj):
  return obj['Name']

def lambda_handler(event, context):
    records =[]
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']
        
    try:
        language = event['pathParameters']['language']
        e = {'#s': 'STATUS'}
        f = '#s = :stat'
        response = dynamodb.query(
            TableName="TuCita247",
            IndexName="TuCita247_Index",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='GSI1PK = :key AND GSI1SK = :categories',
            ExpressionAttributeNames=e,
            FilterExpression=f,
            ExpressionAttributeValues={
                ':key': {'S': 'CAT#'},
                ':categories': {'S': 'CAT#'},
                ':stat' : {'N': '1'}
            },
        )
        for row in json_dynamodb.loads(response['Items']):
            recordset = {
                'CategoryId': row['PKID'],
                'Name': row['NAME_ENG'] if language.upper() == 'EN' else row['NAME_ESP']
            }
            records.append(recordset)
        records.sort(key=getKey)

        rows = []
        idCat = ''
        for item in records:
            idCat = item['CategoryId']
            dat = {
                'CategoryId': idCat,
                'Name': item['Name']
            }
            rows.append(dat)

            resp = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :key AND begins_with(SKID , :categories)',
                ExpressionAttributeNames=e,
                FilterExpression=f,
                ExpressionAttributeValues={
                    ':key': {'S': idCat},
                    ':categories': {'S': 'SUB#'},
                    ':stat' : {'N': '1'}
                },
            )
            recSubset = []
            for sub in json_dynamodb.loads(resp['Items']):
                subSet = {
                    'CategoryId': sub['PKID']+'#'+sub['SKID'] if sub['PKID'] != sub['SKID'] else sub['PKID'],
                    'Name': sub['NAME_ENG'] if language.upper() == 'EN' else sub['NAME_ESP']
                }
                recSubset.append(subSet)
            
            if recSubset != []:
                recSubset.sort(key=getKey)
                for subItem in recSubset:
                    dat = {
                        'CategoryId': subItem['CategoryId'],
                        'Name': subItem['Name']
                    }
                    rows.append(dat)

        statusCode = 200
        body = json.dumps(rows)
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