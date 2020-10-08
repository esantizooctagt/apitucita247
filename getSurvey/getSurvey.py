import sys
import logging
import json

import os

import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr
from dynamodb_json import json_util as json_dynamodb

REGION = 'us-east-1'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.client('dynamodb', region_name=REGION)
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']

    try:
        surveyId = event['pathParameters']['surveyId']

        items=[]
        lines={}
        details = dynamodb.query(
            TableName="TuCita247",
            IndexName="TuCita247_Index",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='GSI1PK = :surveys AND begins_with (GSI1SK , :item)',
            ExpressionAttributeValues={
                ':surveys': {'S': 'SUR#' + surveyId},
                ':item': {'S': 'ITEM#'}
            }
        )
        for item in json_dynamodb.loads(details['Items']):
            lines = {
                'QuestionId': item['GSI1SK'].replace('ITEM#',''),
                'Description': item['DESCRIPTION'],
                'Status': item['STATUS'],
                'Happy': item['HAPPY'],
                'Neutral': item['NEUTRAL'],
                'Angry': item['ANGRY']
            }
            items.append(lines)

        master = dynamodb.query(
            TableName="TuCita247",
            IndexName="TuCita247_Index",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='GSI1PK = :surveys AND GSI1SK = :surveys',
            ExpressionAttributeValues={
                ':surveys': {'S': 'SUR#' + surveyId}
            },
            Limit =1
        )
        for item in json_dynamodb.loads(master['Items']):
            recordset = {
                'SurveyId': item['GSI1SK'].replace('SUR#',''),
                'Name': item['NAME'],
                'LocationId': item['LOCATIONID'],
                'DateSurvey': item['DATE_SURVEY'],
                'Status': int(item['STATUS']),
                'Questions': items
            }
    
        statusCode = 200
        body = json.dumps(recordset)

        if statusCode == '':
            statusCode = 404
            body = json.dumps({"Message": "No more rows", "Code": 404})
    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message': 'Error on request try again ' +str(e)})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : cors
        },
        'body' : body
    }
    return response