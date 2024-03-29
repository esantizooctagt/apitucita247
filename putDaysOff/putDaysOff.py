import sys
import logging
import json

import boto3
import botocore.exceptions
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
from dynamodb_json import json_util as json_dynamodb

import datetime
import dateutil.tz
from datetime import timezone

import os

REGION = 'us-east-1'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodbData = boto3.resource('dynamodb', region_name=REGION)
dynamodb = boto3.client('dynamodb', region_name=REGION)
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']
    try:
        businessId = event['pathParameters']['businessId']
        locationId = event['pathParameters']['locationId']
        providerId = event['pathParameters']['providerId']
        dateSpec = event['pathParameters']['dateOpe']
        tipo = event['pathParameters']['tipo']

        e = {'#s': 'STATUS'}
        table = dynamodbData.Table('TuCita247')
        if businessId != '_' and locationId == '_' and tipo == 'add':
            response = table.update_item(
                Key={
                    'PKID': 'BUS#' + businessId,
                    'SKID': 'METADATA'
                },
                UpdateExpression="SET DAYS_OFF = list_append(DAYS_OFF,:dateope)",
                ExpressionAttributeValues={
                    ':dateope': [dateSpec]
                },
                ReturnValues="UPDATED_NEW"
            )
            locs = dynamodb.query(
                TableName='TuCita247',
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :businessId AND begins_with(SKID , :locs)',
                FilterExpression='PARENTDAYSOFF = :parentDays AND #s = :stat',
                ExpressionAttributeNames=e,
                ExpressionAttributeValues={
                    ':businessId': {'S': 'BUS#' + businessId},
                    ':locs': {'S': 'LOC#'},
                    ':parentDays': {'N': str(1)},
                    ':stat' : {'N': '1'}
                },
            )
            for loc in json_dynamodb.loads(locs['Items']):
                locId = loc['SKID'].replace('LOC#', '')
                servs = dynamodb.query(
                    TableName='TuCita247',
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='PKID = :businessId AND begins_with(SKID , :providerId)',
                    FilterExpression='PARENTDAYSOFF = :parentDays AND #s = :stat',
                    ExpressionAttributeNames=e,
                    ExpressionAttributeValues={
                        ':businessId': {'S': 'BUS#' + businessId + '#LOC#' + locId},
                        ':providerId': {'S': 'PRO#'},
                        ':parentDays': {'N': str(1)},
                        ':stat' : {'N': '1'}
                    },
                )
                for serv in json_dynamodb.loads(servs['Items']):
                    response = table.update_item(
                        Key={
                            'PKID': 'BUS#' + businessId + '#LOC#' + locId,
                            'SKID': serv['SKID']
                        },
                        UpdateExpression="SET DAYS_OFF = list_append(DAYS_OFF,:dateope)",
                        ExpressionAttributeValues={':dateope': [dateSpec]},
                        ReturnValues="UPDATED_NEW"
                    )
                response = table.update_item(
                    Key={
                        'PKID': 'BUS#' + businessId,
                        'SKID': 'LOC#' + locId
                    },
                    UpdateExpression="SET DAYS_OFF = list_append(DAYS_OFF,:dateope)",
                    ExpressionAttributeValues={':dateope': [dateSpec]},
                    ReturnValues="UPDATED_NEW"
                )

        if locationId != '_' and providerId == '_' and tipo == 'add':
            response = table.update_item(
                Key={
                    'PKID': 'BUS#' + businessId,
                    'SKID': 'LOC#' + locationId
                },
                UpdateExpression="SET DAYS_OFF = list_append(DAYS_OFF,:dateope)",
                ExpressionAttributeValues={':dateope': [dateSpec]},
                ReturnValues="UPDATED_NEW"
            )
            servs = dynamodb.query(
                TableName='TuCita247',
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :businessId AND begins_with(SKID , :providerId)',
                FilterExpression='PARENTDAYSOFF = :parentDays AND #s = :stat',
                ExpressionAttributeNames=e,
                ExpressionAttributeValues={
                    ':businessId': {'S': 'BUS#' + businessId + '#LOC#' + locationId},
                    ':providerId': {'S': 'PRO#'},
                    ':parentDays': {'N': str(1)},
                    ':stat' : {'N': '1'}
                },
            )
            for serv in json_dynamodb.loads(servs['Items']):
                response = table.update_item(
                    Key={
                        'PKID': 'BUS#' + businessId + '#LOC#' + locationId,
                        'SKID': serv['SKID']
                    },
                    UpdateExpression="SET DAYS_OFF = list_append(DAYS_OFF,:dateope)",
                    ExpressionAttributeValues={':dateope': [dateSpec]},
                    ReturnValues="UPDATED_NEW"
                )
        
        if providerId != '_' and tipo == 'add':
            response = table.update_item(
                Key={
                    'PKID': 'BUS#' + businessId + '#LOC#' + locationId,
                    'SKID': 'PRO#' + providerId
                },
                UpdateExpression="SET DAYS_OFF = list_append(DAYS_OFF,:dateope)",
                ExpressionAttributeValues={':dateope': [dateSpec]},
                ReturnValues="UPDATED_NEW"
            )

        if businessId != '_' and locationId == '_' and tipo == 'rem':
            response = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :businessId AND SKID = :metadata',
                ExpressionAttributeValues={
                    ':businessId': {'S': 'BUS#'+businessId},
                    ':metadata': {'S': 'METADATA'}
                }
            )
            getIndex = ''
            index = 0
            for row in json_dynamodb.loads(response['Items']):
                getIndex = row['DAYS_OFF'] if 'DAYS_OFF' in row else []

            index = getIndex.index(dateSpec)
            response = table.update_item(
                Key={
                    'PKID': 'BUS#' + businessId,
                    'SKID': 'METADATA'
                },
                UpdateExpression="REMOVE DAYS_OFF[" + str(index) + "]",
                ReturnValues="NONE"
            )

            locs = dynamodb.query(
                TableName='TuCita247',
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :businessId AND begins_with(SKID , :locs)',
                FilterExpression='PARENTDAYSOFF = :parentDays AND #s = :stat',
                ExpressionAttributeNames=e,
                ExpressionAttributeValues={
                    ':businessId': {'S': 'BUS#' + businessId},
                    ':locs': {'S': 'LOC#'},
                    ':parentDays': {'N': str(1)},
                    ':stat' : {'N': '1'}
                },
            )
            getIndexLoc = ''
            indexLoc = 0
            for loc in json_dynamodb.loads(locs['Items']):
                getIndexLoc = loc['DAYS_OFF'] if 'DAYS_OFF' in loc else []

                indexLoc = getIndexLoc.index(dateSpec)
                resp = table.update_item(
                    Key={
                        'PKID': 'BUS#' + businessId,
                        'SKID': 'LOC#' + loc['SKID'].replace('LOC#','')
                    },
                    UpdateExpression="REMOVE DAYS_OFF[" + str(indexLoc) + "]",
                    ReturnValues="NONE"
                )

                servs = dynamodb.query(
                    TableName='TuCita247',
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='PKID = :businessId AND begins_with(SKID , :providerId)',
                    FilterExpression='PARENTDAYSOFF = :parentDays AND #s = :stat',
                    ExpressionAttributeNames=e,
                    ExpressionAttributeValues={
                        ':businessId': {'S': 'BUS#' + businessId + '#' + loc['SKID']},
                        ':providerId': {'S': 'PRO#'},
                        ':parentDays': {'N': str(1)},
                        ':stat' : {'N': '1'}
                    },
                )
                getIndexServ =''
                indexServ = 0
                for serv in json_dynamodb.loads(servs['Items']):
                    getIndexServ = serv['DAYS_OFF'] if 'DAYS_OFF' in serv else []

                    indexServ = getIndexServ.index(dateSpec)
                    response = table.update_item(
                        Key={
                            'PKID': 'BUS#' + businessId + '#' + loc['SKID'],
                            'SKID': serv['SKID']
                        },
                        UpdateExpression="REMOVE DAYS_OFF[" + str(indexServ) + "]",
                        ReturnValues="NONE"
                    )        
        
        if locationId != '_' and providerId == '_' and tipo == 'rem':
            response = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :businessId AND begins_with ( SKID , :locationId )',
                ExpressionAttributeValues={
                    ':businessId': {'S': 'BUS#'+businessId},
                    ':locationId': {'S': 'LOC#'+locationId}
                }
            )
            getIndex = ''
            index = 0
            for row in json_dynamodb.loads(response['Items']):
                getIndex = row['DAYS_OFF'] if 'DAYS_OFF' in row else []

            index = getIndex.index(dateSpec)
            response = table.update_item(
                Key={
                    'PKID': 'BUS#' + businessId,
                    'SKID': 'LOC#' + locationId
                },
                UpdateExpression="REMOVE DAYS_OFF[" + str(index) + "]",
                ReturnValues="NONE"
            )

            servs = dynamodb.query(
                TableName='TuCita247',
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :businessId AND begins_with(SKID , :providerId)',
                FilterExpression='PARENTDAYSOFF = :parentDays AND #s = :stat',
                ExpressionAttributeNames=e,
                ExpressionAttributeValues={
                    ':businessId': {'S': 'BUS#' + businessId + '#LOC#' + locationId},
                    ':providerId': {'S': 'PRO#'},
                    ':parentDays': {'N': str(1)},
                    ':stat' : {'N': '1'}
                },
            )
            getIndexServ =''
            indexServ = 0
            for serv in json_dynamodb.loads(servs['Items']):
                getIndexServ = serv['DAYS_OFF'] if 'DAYS_OFF' in serv else []

                indexServ = getIndexServ.index(dateSpec)
                response = table.update_item(
                    Key={
                        'PKID': 'BUS#' + businessId + '#LOC#' + locationId,
                        'SKID': serv['SKID']
                    },
                    UpdateExpression="REMOVE DAYS_OFF[" + str(indexServ) + "]",
                    ReturnValues="NONE"
                )
        
        if providerId != '_' and tipo == 'rem':
            response = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :businessId AND SKID = :providerId',
                ExpressionAttributeValues={
                    ':businessId': {'S': 'BUS#'+businessId+'#LOC#'+locationId},
                    ':providerId': {'S': 'PRO#'+providerId}
                }
            )
            getIndex = ''
            index = 0
            for row in json_dynamodb.loads(response['Items']):
                getIndex = row['DAYS_OFF'] if 'DAYS_OFF' in row else []

            index = getIndex.index(dateSpec)
            response = table.update_item(
                Key={
                    'PKID': 'BUS#' + businessId + '#LOC#' + locationId,
                    'SKID': 'PRO#' + providerId
                },
                UpdateExpression="REMOVE DAYS_OFF[" + str(index) + "]",
                ReturnValues="NONE"
            )
            
        statusCode = 200
        body = json.dumps({'Message': 'Special day updated successfully', 'Code': 200})

        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Message': 'Error on update special day', 'Code': 500})
    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message': 'Error on request try again ' + str(e), 'Code': 500})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : cors
        },
        'body' : body
    }
    return response