import sys
import logging
import json

import datetime
import dateutil.tz
from datetime import timezone

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

        country_date = dateutil.tz.gettz('America/Puerto_Rico')
        today = datetime.datetime.now(tz=country_date)
        dateOpe = today.strftime("%Y-%m-%d-%H-%M-%S")

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
        for row in json_dynamodb.loads(response['Items']):
            encontro = 0
            for cat in  json.loads(data['Categories']):
                if cat['CategoryId'] == row['SKID']:
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

        for row in json.loads(data['Categories']):
            encontro = 0
            for catego in json_dynamodb.loads(response['Items']):
                if (catego['SKID'] == row['CategoryId']):
                    encontro = 1
                    break
            if encontro == 0:
                recordset = {}
                recordset = {
                    "Put": {
                        "TableName": "TuCita247",
                        "Item": {
                            "PKID": {"S": 'BUS#'+businessId},
                            "SKID": {"S": row['CategoryId']},
                            "GSI1PK": {"S": 'BUS#CAT'},
                            "GSI1SK": {"S": row['CategoryId']+'#'+businessId},
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
                "UpdateExpression":"set MODIFIED_DATE = :mod_date, ADDRESS = :address, CITY = :city, COUNTRY = :country, EMAIL = :email, FACEBOOK = :facebook, GEOLOCATION = :geolocation, INSTAGRAM = :instagram, #n = :name, #l = :language, PHONE = :phone, COUNTRYCODE = :countryCode, TWITTER = :twitter, WEBSITE = :website, ZIPCODE = :zipcode, LONGDESCRIPTION = :longDescrip, SHORTDESCRIPTION = :shortDescrip, PARENTBUSINESS = :parentBus, TAGS = :tags, REASONS = :reasons, GSI4PK = :search, GSI4SK = :search" + (", GSI8PK = :key2, GSI8SK = :skey2" if parentBusiness == 1 else "") + (", TU_CITA_LINK = :tucitalink" if data['TuCitaLink'] != "" else ""),
                "ExpressionAttributeNames": { '#n': 'NAME', '#l': 'LANGUAGE' },
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
                    ":phone": {"S": data['Phone']},
                    ":countryCode": {"S": data['CountryCode']},
                    ":twitter": {"S": data['Twitter']},
                    ":website": {"S": data['Website']},
                    ":parentBus": {"N": str(data['ParentBusiness'])},
                    ":tags": {"S": data['Tags']},
                    ":language": {"S": data['Language']},
                    ":reasons": {"S": data['Reasons']},
                    ":tucitalink": {"S": data['TuCitaLink'] if data['TuCitaLink'] != '' else None},
                    ":key2": {"S": "PARENT#BUS" if parentBusiness == 1 else None},
                    ":skey2": {"S": "BUS#" + businessId if parentBusiness == 1 else None},
                    ":zipcode": {"S": data['ZipCode']},
                    ":search": {"S": "SEARCH"},
                    ":mod_date": {"S": str(dateOpe)}
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