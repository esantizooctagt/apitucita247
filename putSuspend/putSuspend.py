import sys
import logging
import json
import requests
import boto3
import botocore.exceptions
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
from dynamodb_json import json_util as json_dynamodb
# from woocommerce import API
import os

REGION = 'us-east-1'
KEY = os.environ['wpKey']
SECRET = os.environ['wpSecret']

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.client('dynamodb', region_name=REGION)
ses = boto3.client('ses', region_name=REGION)
search = boto3.client('cloudsearchdomain', endpoint_url="https://search-tucita247-djl3mvkaapbmo5zjxat7pcnepu.us-east-1.cloudsearch.amazonaws.com")
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']
        
    try:
        businessId = event['pathParameters']['businessId']
        sns = int(event['pathParameters']['sns'])
        value = int(event['pathParameters']['type'])

        response = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :businessId AND SKID = :metadata',
            ExpressionAttributeValues={
                ':businessId': {'S': 'BUS#' + businessId},
                ':metadata': {'S': 'PLAN'}
            }
        )
        subId = ''
        for item in json_dynamodb.loads(response['Items']):
            subId = str(item['SUBID'])

        items = []
        rows = {}
        rows = {
            "Update": {
                "TableName": "TuCita247",
                "Key": {
                    "PKID": {"S": 'BUS#' + businessId },
                    "SKID": {"S": 'PLAN' }
                },
                "UpdateExpression":"SET #s = :status",
                "ExpressionAttributeNames":{'#s': 'STATUS'},
                "ExpressionAttributeValues": {
                    ":status": {"N": '0' if value == 1 else '2'}
                },
                "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
            },
        }
        items.append(rows)

        rows = {
            "Update": {
                "TableName": "TuCita247",
                "Key": {
                    "PKID": {"S": 'BUS#' + businessId },
                    "SKID": {"S": 'METADATA' }
                },
                "UpdateExpression":"set #s = :status",
                "ExpressionAttributeNames":{'#s': 'STATUS'},
                "ExpressionAttributeValues": { 
                    ":status": {"N": '0' if value == 1 else '2'}
                },
                "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
            },
        }
        items.append(rows)

        logger.info(items)
        response = dynamodb.transact_write_items(
            TransactItems = items
        )

        if value != 1:
            record = [{
                "type" : "delete",
                "id" : 'BUS#' + businessId
            }]
            response = search.upload_documents(
                documents=json.dumps(record),
                contentType='application/json'
            )

        if subId != '':
            url = 'https://tucita247.com/wp-json/wc/v1/subscriptions/'+subId
            params = dict(
                status='cancelled', 
                consumer_key=KEY,
                consumer_secret=SECRET
            )
            subscriptions = requests.put(url=url, params=params)

        if sns == 1:
            #EMAIL
            msg = "Cuenta cancelada"
            SENDER = "Tu Cita 24/7 <no-reply@tucita247.com>"
            # RECIPIENT = 'rita@lutorio.com', 'carlos@lutorio.com'
            SUBJECT = "Cuenta Cancelada - Tu Cita 24/7"
            BODY_TEXT = (msg)
                        
            # The HTML body of the email.
            BODY_HTML = """<html>
            <head></head>
            <body>
            <h1>Tu Cita 24/7</h1>
            <p>""" + msg + """</p>
            </body>
            </html>"""

            CHARSET = "UTF-8"

            response = ses.send_email(
                Destination={
                    'ToAddresses': [
                        'rita@lutorio.com', 'carlos@lutorio.com',
                    ],
                },
                Message={
                    'Body': {
                        'Html': {
                            'Charset': CHARSET,
                            'Data': BODY_HTML,
                        },
                        'Text': {
                            'Charset': CHARSET,
                            'Data': BODY_TEXT,
                        },
                    },
                    'Subject': {
                        'Charset': CHARSET,
                        'Data': SUBJECT,
                    },
                },
                Source=SENDER
            )

        statusCode = 200
        body = json.dumps({'Message': 'Business updated successfully', 'Code': 200})

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