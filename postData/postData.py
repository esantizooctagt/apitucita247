import sys
import logging
import json
import csv

import base64

import datetime
import dateutil.tz

import boto3
import botocore.exceptions
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
from dynamodb_json import json_util as json_dynamodb

import os

REGION = 'us-east-1'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')

dynamodb = boto3.resource('dynamodb', region_name=REGION)
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def put_object(bucket_name, object_name, file):
    # Insert the object
    try:
        s3.put_object(Bucket=bucket_name, Key=object_name, Body=file) 
    except ClientError as e:
        logger.info('Error Inserted')
        logging.error(e)
        return False
    return True

def lambda_handler(event, context):
    bucket = os.environ['bucket']

    try:
        today = datetime.datetime.now()
        year = today.strftime("%Y")
        month = today.strftime("%m").zfill(2)
        day = today.strftime("%d").zfill(2)

        business = dynamodb.query(
            TableName="TuCita247",
            IndexName="TuCita247_Index11",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='GSI11PK = :key01 and begins_with(GSI11SK , :key02)',
            ExpressionAttributeValues={
                ':key01': {'S': 'DT#'+year+'-'+month+'-'+day},
                ':key02': {'S': 'BUS#'}
            }
        )
        data = json_dynamodb.loads(business['Items'])
        tempCsv = open('data.csv')
        csv_file = csv.writer(tempCsv)
        for item in data:
            csv_file.writerow(item)

        tempCsv.close()
        put_object(bucket, '/business/year='+year+'/month='+month+'/day='+day+'/data.csv', csv_file)

        customers = dynamodb.query(
            TableName="TuCita247",
            IndexName="TuCita247_Index11",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='GSI11PK = :key01 and begins_with(GSI11SK , :key02)',
            ExpressionAttributeValues={
                ':key01': {'S': 'DT#'+year+'-'+month+'-'+day},
                ':key02': {'S': 'CUS#'}
            }
        )
        data01 = json_dynamodb.loads(business['Items'])
        tempCsv01 = open('data01.csv')
        csv_file01 = csv.writer(tempCsv01)
        for item in data01:
            csv_file01.writerow(item)

        tempCsv01.close()
        put_object(bucket, '/customers/year='+year+'/month='+month+'/day='+day+'/data01.csv', csv_file01)

        appos = dynamodb.query(
            TableName="TuCita247",
            IndexName="TuCita247_Index11",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='GSI11PK = :key01 and begins_with(GSI11SK , :key02)',
            ExpressionAttributeValues={
                ':key01': {'S': 'DT#'+year+'-'+month+'-'+day},
                ':key02': {'S': 'APPO#'}
            }
        )
        data02 = json_dynamodb.loads(business['Items'])
        tempCsv02 = open('data02.csv')
        csv_file02 = csv.writer(tempCsv02)
        for item in data02:
            csv_file02.writerow(item)

        tempCsv02.close()
        put_object(bucket, '/citas/year='+year+'/month='+month+'/day='+day+'/data02.csv', csv_file02)
    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message': 'Error on request try again'})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : "*"
        },
        'body' : body
    }
    return response  