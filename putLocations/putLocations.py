import sys
import logging
import json

import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr
from dynamodb_json import json_util as json_dynamodb

import os
import uuid

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
        businessId = event['pathParameters']['businessId']
        
        # LOCATIONS
        items = []
        for locs in data['Items']:
            locations = {}
            if locs['LocationId'] == '':
                locId = str(uuid.uuid4()).replace("-","")
                locations = {
                    "Put":{
                        "TableName":"TuCita247",
                        "Item": {
                            "PKID": {"S": 'BUS#'+businessId},
                            "SKID": {"S": 'LOC#'+locId},
                            "NAME": {"S": str(locs['Name'])},
                            "ADDRESS": {"S", str(locs['Address'])},
                            "GEOLOCATION": {"S", str(locs['Geolocation'])},
                            "PARENT_LOCATION": {"S", str(locs['ParentLocation'])},
                            "TOTAL_TRANSITABLE_AREA": {"S", str(locs['TotalPiesTransArea'])},
                            "LOCATION_DENSITY": {"S", str(locs['LocationDensity'])},
                            "MAX_NUMBER_EMPLOYEES_LOC": {"S", str(locs['MaxNumberEmployeesLocation'])},
                            "MAX_CUSTOMER_LOC": {"S", str(locs['MaxConcurrentCustomerLocation'])},
                            "OPEN": {"S", str(locs['Open'])},
                            "BUCKET_INTERVAL": {"S", str(locs['BucketInterval'])},
                            "CUSTOMER_PER_BUCKET": {"S", str(locs['TotalCustPerBucketInter'])},
                            "OPERATIONHOURS": {"S", str(locs['OperationHours'])},
                            "DOORS": {"S", str(locs['Doors'])},
                            "STATUS": {"S", str(locs['Status'])}
                        },
                        "ConditionExpression": "attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                        "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
                    },
                }
                items.append(locations)
            else:
                locations = {
                    "Put":{
                        "TableName":"TuCita247",
                        "Item": {
                            "PKID": {"S": 'BUS#'+businessId},
                            "SKID": {"S": 'LOC#'+locs['LocationId']},
                            "NAME": {"S": str(locs['Name'])}
                        },
                        "ConditionExpression": "attribute_exists(PKID) AND attribute_exists(SKID)",
                        "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
                    },
                }

        logger.info(items)
        response = dynamodb.transact_write_items(
            TransactItems = items
        )
        logger.info(response)

        statusCode = 200
        body = json.dumps({'Message': 'Locations updated successfully'})

        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Message': 'Error on update locations'})
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