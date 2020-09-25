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

dynamodb = boto3.client('dynamodb', region_name=REGION)
dynamoUpd = boto3.resource('dynamodb', region_name=REGION)
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):

    try:
        country_date = dateutil.tz.gettz('America/Puerto_Rico')
        today = datetime.datetime.now(tz=country_date)
        dueDate = (today + datetime.timedelta(days=31)).strftime("%Y-%m-%d")
        date = today.strftime("%Y-%m-%d")

        response = dynamodb.query(
            TableName="TuCita247",
            IndexName="TuCita247_Index",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='GSI1PK = :date',
            ExpressionAttributeValues={
                ':date': {'S': date}
            }
        )
        for row in json_dynamodb.loads(response['Items']):
            if row['NAME'] == 'FREE':
                table = dynamoUpd.Table('TuCita247')
                response = table.update_item(
                    Key={
                        'PKID': row['PKID'],
                        'SKID': 'PLAN'
                    },
                    UpdateExpression="SET AVAILABLE = APPOINTMENTS, DUE_DATE = :dueDate, GSI1PK = :dueDate",
                    ExpressionAttributeValues={
                        ':dueDate': dueDate 
                    }
                )
        
        while 'LastEvaluatedKey' in response:
            lastItem = json_dynamodb.loads(response['LastEvaluatedKey'])
            response = dynamodb.query(
                TableName="TuCita247",
                IndexName="TuCita247_Index",
                ReturnConsumedCapacity='TOTAL',
                ExclusiveStartKey= lastItem,
                KeyConditionExpression='GSI1PK = :date',
                ExpressionAttributeValues={
                    ':date': {'S': date}
                }
            )

            for row in json_dynamodb.loads(response['Items']):
                if row['NAME'] == 'FREE':
                    table = dynamoUpd.Table('TuCita247')
                    response = table.update_item(
                        Key={
                            'PKID': row['PKID'],
                            'SKID': 'PLAN'
                        },
                        UpdateExpression="SET AVAILABLE = APPOINTMENTS, DUE_DATE = :dueDate, GSI1PK = :dueDate",
                        ExpressionAttributeValues={
                            ':dueDate': dueDate 
                        }
                    )

        statusCode = 200
        body = json.dumps({'Message':'OK','Code':200})
    except:
        statusCode = 500
        body = json.dumps({'Message':'Error on request try again'})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : "*"
        },
        'body' : body
    }
    return response