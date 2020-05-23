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
        statusCode = ''
        data = json.loads(event['body'])
        businessId = event['pathParameters']['id']
        # BUSINESS CATEGORIES
        response = dynamodb.query(
            TableName = "TuCita247",
            ReturnConsumedCapacity = 'TOTAL',
            KeyConditionExpression = 'PKID = :businessId AND begins_with( SKID , :cats )',
            ExpressionAttributeValues = {
                ':businessId': {'S': 'BUS#' + businessId},
                ':cats': {'S': 'CAT#'}
            }
        )

        items = []
        for cats in response['Items']:
            row = json_dynamodb.loads(cats)
            deletes = {}
            deletes = {
                "Delete":{
                    "TableName":"TuCita247",
                    "Key": {
                        "PKID": {"S": 'BUS#'+businessId},
                        "SKID": {"S":row['SKID']}
                    },
                    "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
                },
            }
            items.append(deletes)

        for items in data['Categories']:
            row = json_dynamodb.loads(items)
            recordset = {
                "Put": {
                    "TableName": "TuCita247",
                    "Item": {
                        "PKID": {"S": 'BUS#'+businessId},
                        "SKID": {"S": row['CategoryId']},
                        "NAME": {"N": str(row['Name'])}
                    },
                    "ConditionExpression": "attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                    "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
                }
            }
            items.append(recordset)
        
        rows = {}
        rows = {
            "Update": {
                "TableName": "TuCita247",
                "Key": {
                    "PKID": {"S": 'BUS#' + businessId },
                    "SKID": {"S": 'METADATA' }
                },
                "UpdateExpression":"set ADDRESS = :address, CITY = :city, COUNTRY = :country, EMAIL = :email, FACEBOOK = :facebook, GEOLOCATION = :geolocation, INSTAGRAM = :instagram, #n = :name, OPERATIONHOURS = :operationsHours, PHONE = :phone, TWITTER = :twitter, WEBSITE = :website, ZIPCODE = :zipcode",
                "ExpressionAttributeNames": { '#n': 'NAME' },
                "ExpressionAttributeValues": { 
                    ":address": {"S": data['Address']},
                    ":city": {"S": data['City']},
                    ":country": {"S": data['Country']},
                    ":email": {"S": data['Email']},
                    ":facebook": {"S": data['Facebook']},
                    ":geolocation": {"S": data['Geolocation']},
                    ":instagram": {"S": data['Instagram']},
                    ":name": {"S": data['Name']},
                    ":operationHours": {"S": data['OperationHours']},
                    ":phone": {"S": data['Phone']},
                    ":twitter": {"S": data['Twitter']},
                    ":website": {"S": data['Website']},
                    ":zipcode": {"S": data['ZipCode']}
                },
                "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
            },
        }
        items.append(rows)

        logger.info(items)
        response = dynamodb.transact_write_items(
            TransactItems = items
        )
        logger.info(response)

        statusCode = 200
        body = json.dumps({'Message': 'User updated successfully'})

        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Message': 'Error on update user'})
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