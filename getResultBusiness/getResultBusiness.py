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
    try:
        categoryId = event['pathParameters']['categoryId']
        subcategoryId = event['pathParameters']['subcategoryId']
        businessId = event['pathParameters']['businessId']
        lastItem = event['pathParameters']['lastItem']
        bussList = {}
        bussList = set()

        if lastItem == '_':
            lastItem = ''
        else:
          lastItem = {'PKID': {'S': 'BUS#'+lastItem.split('.')[0] },'SKID': {'S': 'METADATA'}, 'GSI1PK': {'S': 'BUS#CAT'}, 'GSI1SK': {'S': 'CAT#'+categoryId+'#SUB#'+lastItem.split('.')[1]+'#'+lastItem.split('.')[0]}}

        if businessId != '_' and categoryId == '_':
            response = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :businessId AND SKID = :metadata',
                ExpressionAttributeValues={
                    ':businessId': {'S': 'BUS#' + businessId},
                    ':metadata': {'S': 'METADATA'}
                }
            )
        if categoryId != '_' and subcategoryId == '_':
            if lastItem == '':
                response = dynamodb.query(
                    TableName="TuCita247",
                    IndexName="TuCita247_Index",
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='GSI1PK = :businessId AND begins_with(GSI1SK , :categoryId)',
                    ExpressionAttributeValues={
                        ':businessId': {'S': 'BUS#CAT'},
                        ':categoryId': {'S': 'CAT#' + str(categoryId)}
                    }
                )
            else:
                response = dynamodb.query(
                    TableName="TuCita247",
                    IndexName="TuCita247_Index",
                    ReturnConsumedCapacity='TOTAL',
                    ExclusiveStartKey=lastItem,
                    KeyConditionExpression='GSI1PK = :businessId AND begins_with(GSI1SK , :categoryId)',
                    ExpressionAttributeValues={
                        ':businessId': {'S': 'BUS#CAT'},
                        ':categoryId': {'S': 'CAT#' + str(categoryId)}
                    }
                )
        if subcategoryId != '_':
            if lastItem == '':
                response = dynamodb.query(
                    TableName="TuCita247",
                    IndexName="TuCita247_Index",
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='GSI1PK = :businessId AND begins_with(GSI1SK , :categoryId)',
                    ExpressionAttributeValues={
                        ':businessId': {'S': 'BUS#CAT'},
                        ':categoryId': {'S': 'CAT#' + str(categoryId) + '#SUB#' + str(subcategoryId)}
                    }
                )
            else:
                response = dynamodb.query(
                    TableName="TuCita247",
                    IndexName="TuCita247_Index",
                    ReturnConsumedCapacity='TOTAL',
                    ExclusiveStartKey=lastItem,
                    KeyConditionExpression='GSI1PK = :businessId AND begins_with(GSI1SK , :categoryId)',
                    ExpressionAttributeValues={
                        ':businessId': {'S': 'BUS#CAT'},
                        ':categoryId': {'S': 'CAT#' + str(categoryId) + '#SUB#' + str(subcategoryId)}
                    }
                )
        recordset = {}
        business = []
        for row in json_dynamodb.loads(response['Items']):
            records = []
            locsNumber = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :businessId AND begins_with(SKID , :locs)',
                ExpressionAttributeValues={
                    ':businessId': {'S': row['PKID']},
                    ':locs': {'S': 'LOC#'},
                    ':stat' : {'N': '1'}
                },
                FilterExpression='#s = :stat',
                ExpressionAttributeNames={'#s': 'STATUS'}
            )
            number = 0
            for item in json_dynamodb.loads(locsNumber['Items']):
                number = number + 1
            recordset = {
                'Business_Id': row['PKID'].replace('BUS#',''),
                'Name': row['NAME'],
                'LongDescription': row['LONGDESCRIPTION'] if 'LONGDESCRIPTION' in row else '',
                'ShortDescription': row['SHORTDESCRIPTION'] if 'SHORTDESCRIPTION' in row else '',
                'Imagen': row['IMGBUSINESS'] if 'IMGBUSINESS' in row else '',
                'Location_No': number,
                'Categories': records,
                'Status': row['STATUS']
            }
            business.append(recordset)
        
        if 'LastEvaluatedKey' in response:
            lastItem = json_dynamodb.loads(response['LastEvaluatedKey'])
            lastItem = lastItem['PKID'].replace('BUS#','')+'.'+lastItem['GSI1SK'].split('#')[3]
        else:
            lastItem = ''

        statusCode = 200
        body = json.dumps({'Code': 200, 'LastItem': lastItem, 'Business': business})
    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message':'Error on request try again'+ str(e)})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : "*"
        },
        'body' : body
    }
    return response