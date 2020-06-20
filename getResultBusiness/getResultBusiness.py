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

        if lastItem == '_':
            lastItem = ''
        else:
           lastItem = {'PKID': {'S': 'BUS#' + businessId },'SKID': {'S': 'METADATA'}}

        if businessId != '_':
            if lastItem == '':
                response = dynamodb.query(
                    TableName="TuCita247",
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='PKID = :businessId AND begins_with( SKID , :metadata )',
                    ExpressionAttributeValues={
                        ':businessId': {'S': 'BUS#' + businessId},
                        ':metadata': {'S': 'METADATA'}
                    }
                )
            else:
                response = dynamodb.query(
                    TableName="TuCita247",
                    ReturnConsumedCapacity='TOTAL',
                    ExclusiveStartKey= lastItem,
                    KeyConditionExpression='PKID = :businessId AND begins_with( SKID , :metadata )',
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
                    KeyConditionExpression='GSI1PK = :categoryId',
                    ExpressionAttributeValues={
                        ':categoryId': {'S': 'CAT#' + categoryId}
                    }
                )
            else:
                response = dynamodb.query(
                    TableName="TuCita247",
                    IndexName="TuCita247_Index",
                    ReturnConsumedCapacity='TOTAL',
                    ExclusiveStartKey= lastItem,
                    KeyConditionExpression='GSI1PK = :categoryId',
                    ExpressionAttributeValues={
                        ':categoryId': {'S': 'CAT#' + categoryId}
                    }
                )
        if subcategoryId != '_':
            if lastItem == '':
                response = dynamodb.query(
                    TableName="TuCita247",
                    IndexName="TuCita247_Index",
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='GSI1PK = :categoryId AND GSI1SK = :subcategoryId',
                    ExpressionAttributeValues={
                        ':categoryId': {'S': 'CAT#' + categoryId},
                        ':subcategoryId': {'S': 'SUB#' + subcategoryId}
                    }
                )
            else:
                response = dynamodb.query(
                    TableName="TuCita247",
                    IndexName="TuCita247_Index",
                    ReturnConsumedCapacity='TOTAL',
                    ExclusiveStartKey= lastItem,
                    KeyConditionExpression='GSI1PK = :categoryId AND GSI1SK = :subcategoryId',
                    ExpressionAttributeValues={
                        ':categoryId': {'S': 'CAT#' + categoryId},
                        ':subcategoryId': {'S': 'SUB#' + subcategoryId}
                    }
                )

        recordset = {}
        business = []
        for row in json_dynamodb.loads(response['Items']):
            records = []
            cats = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :businessId AND begins_with ( SKID , :cat )',
                ExpressionAttributeValues={
                    ':businessId': {'S': 'BUS#' + row['PKID'].replace('BUS#','')},
                    ':cat': {'S': 'CAT#'}
                }
            )
            for cat in json_dynamodb.loads(cats['Items']):
                item = {
                    'CategoryId': cat['SKID'].split('#')[1],
                    'SubCategoryId': cat['SKID'].split('#')[2]
                }
                records.append(item)
            if businessId == '_':
                buss = dynamodb.query(
                    TableName="TuCita247",
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='PKID = :businessId AND SKID = :metadata',
                    ExpressionAttributeValues={
                        ':businessId': {'S': 'BUS#' + row['PKID'].replace('BUS#','')},
                        ':metadata': {'S': 'METADATA'}
                    },
                    Limit = 1
                )
                for item in json_dynamodb.loads(buss):
                    recordset = {
                        'Business_Id': item['PKID'].replace('BUS#',''),
                        'Name': item['NAME'],
                        'LongDescription': item['LONGDESCRIPTION'] if 'LONGDESCRIPTION' in item else '',
                        'ShortDescription': item['SHORTDESCRIPTION'] if 'SHORTDESCRIPTION' in item else '',
                        'Imagen': item['IMGBUSINESS'] if 'IMGBUSINESS' in item else '',
                        'Categories': records,
                        'Location_No': '1', #GUARDAR Y TRAER EL NUMERO DE LOCALIDADES DEL NEGOCIO BUSCAR EL PUT PARA EDITARLO
                        'Status': item['STATUS']
                    }
            else:
                recordset = {
                    'Business_Id': row['PKID'].replace('BUS#',''),
                    'Name': row['NAME'],
                    'LongDescription': row['LONGDESCRIPTION'] if 'LONGDESCRIPTION' in row else '',
                    'ShortDescription': row['SHORTDESCRIPTION'] if 'SHORTDESCRIPTION' in row else '',
                    'Imagen': row['IMGBUSINESS'] if 'IMGBUSINESS' in row else '',
                    'Location_No': '1', #GUARDAR Y TRAER EL NUMERO DE LOCALIDADES DEL NEGOCIO BUSCAR EL PUT PARA EDITARLO
                    'Categories': records,
                    'Status': row['STATUS']
                }
            business.append(recordset)
        
        if 'LastEvaluatedKey' in response:
            lastItem = json_dynamodb.loads(response['LastEvaluatedKey'])
            lastItem = lastItem['PKID'].replace('BUS#','')

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