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
        for locs in data['Locs']:
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
                            "CITY": {"S": str(locs['City'])},
                            "ADDRESS": {"S": str(locs['Address'])},
                            "SECTOR": {"S": str(locs['Sector'])},
                            "GEOLOCATION": {"S": str(locs['Geolocation'])},
                            "PARENT_LOCATION": {"S": str(locs['ParentLocation'])},
                            "MAX_CUSTOMER": {"N": str(locs['MaxConcurrentCustomer'])},
                            "BUCKET_INTERVAL": {"N": str(locs['BucketInterval'])},
                            "CUSTOMER_PER_BUCKET": {"N": str(locs['TotalCustPerBucketInter'])},
                            "MANUAL_CHECK_OUT": {"N": str(locs['ManualCheckOut'])},
                            "DOORS": {"S": str(locs['Doors'])},
                            "STATUS": {"N": str(locs['Status'])}
                        },
                        "ConditionExpression": "attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                        "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
                    },
                }
            else:
                locations = {
                    "Update":{
                        "TableName":"TuCita247",
                        "Key": {
                            "PKID": {"S": 'BUS#'+businessId},
                            "SKID": {"S": 'LOC#'+locs['LocationId']}
                        },
                        "UpdateExpression": "SET #n = :name, CITY = :city, SECTOR = :sector, ADDRESS = :address, GEOLOCATION = :geolocation, PARENT_LOCATION = :parentLocation, MAX_CUSTOMER = :maxCustomer, BUCKET_INTERVAL = :bucketInterval, CUSTOMER_PER_BUCKET = :customerPerBucket, DOORS = :doors, #s = :status, MANUAL_CHECK_OUT = :manualCheckOut",
                        "ExpressionAttributeNames": {"#n":"NAME", "#s":"STATUS"},
                        "ExpressionAttributeValues": {
                            ":name": {"S": str(locs['Name'])},
                            ":city": {"S": str(locs['City'])},
                            ":sector": {"S": str(locs['Sector'])},
                            ":address": {"S": str(locs['Address'])},
                            ":geolocation": {"S": str(locs['Geolocation'])},
                            ":parentLocation": {"S": str(locs['ParentLocation'])},
                            ":maxCustomer": {"N": str(locs['MaxConcurrentCustomer'])},
                            ":bucketInterval": {"N": str(locs['BucketInterval'])},
                            ":customerPerBucket": {"N": str(locs['TotalCustPerBucketInter'])},
                            ":manualCheckOut": {"N": str(locs['ManualCheckOut'])},
                            ":doors": {"S": str(locs['Doors'])},
                            ":status": {"N": str(locs['Status'])}
                        },
                        "ConditionExpression": "attribute_exists(PKID) AND attribute_exists(SKID)",
                        "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
                    },
                }
            items.append(locations)
            
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