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
        data = json.loads(event['body'])
        businessId = event['pathParameters']['id']
        parentBusiness = data['ParentBusiness']
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
            encontro = 0
            for cat in data['Categories']:
                if 'CAT#'+cat['CategoryId'] == row['SKID']:
                    encontro =1
                    break
            if encontro == 0:
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

        for row in data['Categories']:
            encontro = 0
            for cats in response['Items']:
                catego = json_dynamodb.loads(cats)
                if (catego['SKID'] == 'CAT#'+row['CategoryId']):
                    encontro = 1
                    break
            if encontro == 0:
                recordset = {}
                recordset = {
                    "Put": {
                        "TableName": "TuCita247",
                        "Item": {
                            "PKID": {"S": 'BUS#'+businessId},
                            "SKID": {"S": 'CAT#'+row['CategoryId']},
                            "GSI1PK": {"S": 'CAT#'+row['CategoryId'].split('#')[0]},
                            "GSI1SK": {"S": 'SUB#'+row['CategoryId'].split('#')[1]},
                            "NAME": {"S": str(row['Name'])}
                        },
                        "ConditionExpression": "attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                        "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
                    }
                }
                items.append(recordset)
        
        rows = {}
        if data['TuCitaLink'] != '':
            rows = {
                "Put": {
                    "TableName": "TuCita247",
                    "Item": {
                        "PKID": {"S": 'LINK#'+data['TuCitaLink']},
                        "SKID": {"S": 'LINK#'+data['TuCitaLink']}
                    },
                    "ConditionExpression": "attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                    "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
                }
            }
            items.append(rows)
    
        rows = {
            "Update": {
                "TableName": "TuCita247",
                "Key": {
                    "PKID": {"S": 'BUS#' + businessId },
                    "SKID": {"S": 'METADATA' }
                },
                "UpdateExpression":"set ADDRESS = :address, CITY = :city, COUNTRY = :country, EMAIL = :email, FACEBOOK = :facebook, GEOLOCATION = :geolocation, INSTAGRAM = :instagram, #n = :name, OPERATIONHOURS = :operationHours, PHONE = :phone, TWITTER = :twitter, WEBSITE = :website, ZIPCODE = :zipcode, LONGDESCRIPTION = :longDescrip, SHORTDESCRIPTION = :shortDescrip, PARENTBUSINESS = :parentBus, TAGS = :tags, APPOINTMENTS_PURPOSE = :appospurpose" + (", GSI1PK = :key1, GSI1SK = :skey1" if parentBusiness == 1 else "") + (", TU_CITA_LINK = :tucitalink" if data['TuCitaLink'] != "" else ""),
                "ExpressionAttributeNames": { '#n': 'NAME' },
                "ExpressionAttributeValues": { 
                    ":longDescrip": {"S": data['LongDescription']},
                    ":shortDescrip": {"S": data['ShortDescription']},
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
                    ":parentBus": {"N": str(data['ParentBusiness'])},
                    ":tags": {"S": data['Tags']},
                    ":appospurpose": {"S": data['ApposPurpose']},
                    ":tucitalink": {"S": data['TuCitaLink'] if data['TuCitaLink'] != '' else None},
                    ":key1": {"S": "PARENT#BUS" if parentBusiness == 1 else None},
                    ":skey1": {"S": data['Name'] + "#" + businessId if parentBusiness == 1 else None},
                    ":zipcode": {"S": data['ZipCode']}
                },
                "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
            },
        }
        items.append(cleanNullTerms(rows))

        logger.info(items)
        response = dynamodb.transact_write_items(
            TransactItems = items
        )
        logger.info(response)

        statusCode = 200
        body = json.dumps({'Message': 'Business updated successfully'})

        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Message': 'Error on update business'})
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