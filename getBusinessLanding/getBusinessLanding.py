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

def lambda_handler(event, context):
    stage = ''
    businessId = ''
    body = ''
    cors = ''
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']

    try:
        statusCode = ''
        link = event['pathParameters']['link']
        response = dynamodb.query(
            TableName="TuCita247",
            IndexName="TuCita247_Appos",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='GSI3PK = :link',
            ExpressionAttributeValues={
                ':link': {'S': 'LINK#' + link}
            },
            Limit =1
        )
        for row in json_dynamodb.loads(response['Items']):            
            businessId = row['PKID'].replace('BUS#','')
            locs = []
            if businessId != '':
                locations = dynamodb.query(
                    TableName="TuCita247",
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='PKID = :businessId and begins_with (SKID, :locs) ',
                    ExpressionAttributeValues={
                        ':businessId': {'S': 'BUS#' + businessId},
                        ':locs': {'S': 'LOC#'}
                    }
                )
                for det in json_dynamodb.loads(locations['Items']):
                    record = {
                        'LocationId': det['SKID'].replace('LOC#',''),
                        'Name': det['NAME'],
                        'Address': det['ADDRESS']
                    }
                    locs.append(record)
            
            recordset = {
                'BusinessId': row['PKID'].replace('BUS#',''),
                'Name': row['NAME'],
                'Imagen': row['IMGLINK'] if 'IMGLINK' in row else '',
                'LongDescrip': row['LONGDESCRIPTION'] if 'LONGDESCRIPTION' in row else '',
                'Phone': row['PHONE'] if 'PHONE' in row else '',
                'ShortDescript': row['SHORTDESCRIPTION'] if 'SHORTDESCRIPTION' in row else '',
                'WebSite': row['WEBSITE'] if 'WEBSITE' in row else '',
                'Instagram': row['INSTAGRAM'] if 'INSTAGRAM' in row else '',
                'Twitter': row['TWITTER'] if 'TWITTER' in row else '',
                'Facebook': row['FACEBOOK'] if 'FACEBOOK' in row else '',
                'Locs': locs
            }
            
            statusCode = 200
            body = json.dumps(recordset)
        
        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Message':'Something goes wrong, try again', 'Code': 500})
    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message':'Error on request try again'+ str(e)})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : cors
        },
        'body' : body
    }
    return response