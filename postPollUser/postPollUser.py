import sys
import logging
import json

import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr
from dynamodb_json import json_util as json_dynamodb

import base64

import uuid
import os

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
        statusCode = ''
        data = json.loads(event['body'])

        items = []
        recordset = {}
        response = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :pollId AND SKID = :customerId',
            ExpressionAttributeValues={
                ':pollId': {'S': 'POLL#' + data['PollId']},
                ':customerId': {'S': 'CUS#' + data['CustomerId']}
            }
        )

        if response['Count'] == 0:
            businessId = ''
            response = dynamodb.query(
                TableName="TuCita247",
                IndexName="TuCita247_Index",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='GSI1PK = :pollId AND GSI1SK = :pollId',
                ExpressionAttributeValues={
                    ':pollId': {'S': 'POLL#' + data['PollId']}
                }
            )
            for row in json_dynamodb.loads(response['Items']):
                businessId = row['PKID'].replace('BUS#','')
            if businessId != '':
                recordset = {
                    "Put": {
                        "TableName": "TuCita247",
                        "Item": {
                            "PKID": {"S": 'POLL#' + data['PollId']},
                            "SKID": {"S": 'CUS#' + data['CustomerId']},
                            "GSI1PK": {"S": 'POLL#' + data['PollId']},
                            "GSI1SK": {"S": 'CUS#' + data['CustomerId']},
                            "HAPPY": {"N": str(data['Happy'])},
                            "NEUTRAL": {"N": str(data['Neutral'])},
                            "ANGRY": {"N": str(data['Angry'])}
                        },
                        "ConditionExpression": "attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                        "ReturnValuesOnConditionCheckFailure": "NONE"
                    },
                }
                items.append(recordset)

                recordset = {
                    "Update": {
                        "TableName": "TuCita247",
                        "Key": {
                            "PKID": {"S": 'BUS#' + businessId },
                            "SKID": {"S": 'POLL#' + data['PollId'] }
                        },
                        "UpdateExpression": "SET HAPPY = HAPPY + :happy, NEUTRAL = NEUTRAL + :neutral, ANGRY = ANGRY + :angry",
                        "ExpressionAttributeValues": {
                            ':happy': {'N': str(data['Happy'])},
                            ':neutral': {'N': str(data['Neutral'])},
                            ':angry': {'N': str(data['Angry'])}
                        },
                        "ConditionExpression": "attribute_exists(PKID) AND attribute_exists(SKID)",
                        "ReturnValuesOnConditionCheckFailure": "NONE"
                    }
                }
                items.append(recordset)
                    
                logger.info(items)
                response = dynamodb.transact_write_items(
                    TransactItems = items
                )
                statusCode = 200
                body = json.dumps({'Message': 'Poll saved successfully', 'Code': 200})
            else:
                statusCode = 404
                body = json.dumps({'Message': 'Something goes wrong', 'Code': 404})    
        else:
            statusCode = 404
            body = json.dumps({'Message': 'Poll already filled', 'Code': 404})

        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Message': 'Error on save poll', 'Code': 404})
    except dynamodb.exceptions.TransactionCanceledException as e:
            statusCode = 404
            body = json.dumps({"Code":400,"error": False, 
                    "success": True, 
                    "message": str(e), 
                    "data": None})
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