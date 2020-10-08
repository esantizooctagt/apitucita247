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

def cleanNullTerms(d):
   clean = {}
   for k, v in d.items():
      if isinstance(v, dict):
         nested = cleanNullTerms(v)
         if len(nested.keys()) > 0:
            clean[k] = nested
      elif v is not None:
         clean[k] = v
   return clean
   
def lambda_handler(event, context):
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']
        
    try:
        statusCode = ''
        locationId = str(uuid.uuid4()).replace("-","")
        data = json.loads(event['body'])

        items = []
        recordset = {}
        if data['LocationId'] == '':
            response = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :businessId AND SKID = :metadata',
                ExpressionAttributeValues={
                    ':businessId': {'S': 'BUS#'+data['BusinessId']},
                    ':metadata': {'S': 'METADATA'}
                }
            )
            daysOff = []
            operationHours = ''
            index = 0
            for row in json_dynamodb.loads(response['Items']):
                daysOff = row['DAYS_OFF'] if 'DAYS_OFF' in row else []
                operationHours = row['OPERATIONHOURS'] if 'OPERATIONHOURS' in row else ''
            
            resDays = []
            for day in daysOff:
                resDays.append(json.loads('{"S": "' + day + '"}'))

            recordset = {
                "Put": {
                    "TableName": "TuCita247",
                    "Item": {
                        "PKID": {"S": 'BUS#' + data['BusinessId']},
                        "SKID": {"S": 'LOC#' + locationId},
                        "NAME": {"S": str(data['Name'])},
                        "CITY": {"S": str(data['City'])},
                        "ADDRESS": {"S": str(data['Address'])},
                        "ZIPCODE": {"S": data['ZipCode']},
                        "SECTOR": {"S": str(data['Sector'])},
                        "GEOLOCATION": {"S": str(data['Geolocation'])},
                        "PARENT_LOCATION": {"S": str(data['ParentLocation'])},
                        "MAX_CUSTOMER": {"N": str(data['MaxConcurrentCustomer'])},
                        "MANUAL_CHECK_OUT": {"N": str(data['ManualCheckOut'])},
                        "PARENTDAYSOFF": {"N": str(1)},
                        "PARENTHOURS": {"N": str(1)},
                        "DAYS_OFF": {"L": resDays if resDays != [] else None},
                        "OPERATIONHOURS": {"S": operationHours},
                        "OPEN": {"N": str(0)},
                        "DOORS": {"S": str(data['Doors'])},
                        "STATUS": {"N": str(data['Status'])}
                    },
                    "ConditionExpression": "attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                    "ReturnValuesOnConditionCheckFailure": "NONE"
                },
            }
        else:
            locationId = data['LocationId']
            recordset = {
                "Update": {
                    "TableName": "TuCita247",
                    "Key": {
                        "PKID": {"S": 'BUS#' + data['BusinessId']},
                        "SKID": {"S": 'LOC#' + locationId}
                    },
                    "UpdateExpression": "SET #n = :name, CITY = :city, SECTOR = :sector, ADDRESS = :address, ZIPCODE = :zipcode, GEOLOCATION = :geolocation, PARENT_LOCATION = :parentLocation, MAX_CUSTOMER = :maxCustomer, DOORS = :doors, #s = :status, MANUAL_CHECK_OUT = :manualCheckOut",
                    "ExpressionAttributeNames": {"#n":"NAME", "#s":"STATUS"},
                    "ExpressionAttributeValues": {
                        ":name": {"S": str(data['Name'])},
                        ":city": {"S": str(data['City'])},
                        ":sector": {"S": str(data['Sector'])},
                        ":address": {"S": str(data['Address'])},
                        ":zipcode": {"S": str(data['ZipCode'])},
                        ":geolocation": {"S": str(data['Geolocation'])},
                        ":parentLocation": {"S": str(data['ParentLocation'])},
                        ":maxCustomer": {"N": str(data['MaxConcurrentCustomer'])},
                        ":manualCheckOut": {"N": str(data['ManualCheckOut'])},
                        ":doors": {"S": str(data['Doors'])},
                        ":status": {"N": str(data['Status'])}
                    },
                    "ConditionExpression": "attribute_exists(PKID) AND attribute_exists(SKID)",
                    "ReturnValuesOnConditionCheckFailure": "NONE"
                },
            }
        items.append(cleanNullTerms(recordset))
        
        business = {
            "Update":{
                "TableName":"TuCita247",
                "Key":{
                    "PKID": {"S": 'BUS#'+data['BusinessId']},
                    "SKID": {"S": 'METADATA'}
                },
                "UpdateExpression": "SET GSI4PK = :search, GSI4SK = :search",
                "ExpressionAttributeValues": {
                    ":search": {"S": "SEARCH"}
                },
                "ConditionExpression": "attribute_exists(PKID) AND attribute_exists(SKID)",
                "ReturnValuesOnConditionCheckFailure": "NONE"
            },
        }
        items.append(cleanNullTerms(business))

        logger.info(items)
        response = dynamodb.transact_write_items(
            TransactItems = items
        )
        statusCode = 200
        body = json.dumps({'Message': 'Location added successfully', 'LocationId': locationId, 'Code': 200})

        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Message': 'Error on added location'})
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