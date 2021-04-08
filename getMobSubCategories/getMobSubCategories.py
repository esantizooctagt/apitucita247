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
    specific_order = ['A','Á','B','C','D','E','É','F','G','H','I','Í','J','K','L','M','N','Ñ','O','Ó','P','Q','R','S','T','U','Ú','Ü','V','W','X','Y','Z']
    try:
        language = event['pathParameters']['language']
        categoryId = event['pathParameters']['categoryId']

        e = {'#s': 'STATUS'}
        f = '#s = :stat'
        response = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :categories',
            ExpressionAttributeNames=e,
            FilterExpression=f,
            ExpressionAttributeValues={
                ':categories': {'S': 'CAT#' + categoryId},
                # ':subcat': {'S': 'SUB#'},
                ':stat' : {'N': '1'}
            },
        )
        for row in json_dynamodb.loads(response['Items']):
            recordset = {
                'SubCategoryId': row['SKID'].replace('SUB#',''),
                'Name': row['NAME_ENG'] if language.upper() == 'EN' else row['NAME_ESP'],
                'Icon': row['ICON'],
                'Imagen': row['IMG_CAT']
            }
            records.append(recordset)

        # records.sort(key=getKey)
        records.sort(key=lambda v: specific_order.index(v['Name'][0:1]))
        statusCode = 200
        body = json.dumps(records)
    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message': 'Error on request try again ' + str(e)})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : "*"
        },
        'body' : body
    }
    return response