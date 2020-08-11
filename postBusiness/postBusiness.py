import sys
import logging
import json

import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr
from dynamodb_json import json_util as json_dynamodb

import datetime
import dateutil.tz
from datetime import timezone

import hmac
import hashlib
import base64

import uuid
import random
import string
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

def get_secret_hash(username):
    msg = username + '52k0o8239mueu31uu5fihccbbf'
    dig = hmac.new(str('1r2k3dm8748i5dfu632eu8ptai7vocidm01vp3la82nhq91jgqqt').encode('utf-8'), 
        msg = str(msg).encode('utf-8'), digestmod=hashlib.sha256).digest()
    d2 = base64.b64encode(dig).decode()
    return d2

def get_random_string():
    random_source = string.ascii_letters + string.digits
    password = random.choices(string.ascii_lowercase, k=2)
    password += random.choices(string.ascii_uppercase, k=2)
    password += random.choices(string.digits, k=2)

    for i in range(1):
        password += random.choices(random_source, k=2)

    password_list = list(password)
    random.SystemRandom().shuffle(password_list)
    password = ''.join(password_list)
    return password

def lambda_handler(event, context):
    stage = event['headers']
    cors = stage['origin']
    # stage = event['headers']
    # if stage['origin'] != "http://localhost:4200":
    #     cors = os.environ['prodCors']
    # else:
    #     cors = os.environ['devCors']
        
    try:
        statusCode = ''
        data = json.loads(event['body'])
        businessId = str(uuid.uuid4()).replace("-","")
        userId = str(uuid.uuid4()).replace("-","")

        country_date = dateutil.tz.gettz('America/Puerto_Rico')
        today = datetime.datetime.now(tz=country_date)
        dueDate = (today + datetime.timedelta(days=30)).strftime("%Y-%m-%d")

        items = []
        rows = {
            "Put": {
                "TableName": "TuCita247",
                "Item": {
                    "PKID": {"S": 'BUS#'+businessId},
                    "SKID": {"S": 'USER#' + userId},
                    "GSI1PK": {"S": 'EMAIL#' + data['Email']},
                    "GSI1SK": {"S": 'USER#' + userId},
                    "GSI2PK": {"S": 'USER#' + userId},
                    "GSI2SK": {"S": 'USER#' + userId},
                    "FIRST_NAME": {"S": data['First_Name']},
                    "LAST_NAME": {"S": data['Last_Name']},
                    "PHONE": {"S": data['User_Phone'] if data['User_Phone'] != '' else None},
                    "IS_ADMIN": {"N": str(1)},
                    "USERID": {"S": userId },
                    "STATUS": {"N": "1"}
                },
                "ConditionExpression": "attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
            }
        }
        items.append(cleanNullTerms(rows))

        rows = {
            "Put": {
                "TableName": "TuCita247",
                "Item": {
                    "PKID": {"S": 'EMAIL#'+data['Email']},
                    "SKID": {"S": 'EMAIL#'+data['Email']}
                },
                "ConditionExpression": "attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
            }
        }
        items.append(cleanNullTerms(rows))

        rows = {
            "Put": {
                "TableName": "TuCita247",
                "Item": {
                    "PKID": {"S": 'BUS#' + businessId },
                    "SKID": {"S": 'METADATA'},
                    "ADDRESS": {"S": data['Address']},
                    "CITY": {"S": data['City']},
                    "COUNTRY": {"S": data['Country']},
                    "CATEGORYID": {"S": data['CategoryId']},
                    "EMAIL": {"S": data['Email']},
                    # "FACEBOOK": {"S": data['Facebook']},
                    "GEOLOCATION": {"S": data['Geolocation']},
                    # "INSTAGRAM": {"S": data['Instagram']},
                    "NAME": {"S": data['Name']},
                    "PHONE": {"S": data['Phone']},
                    # "TWITTER": {"S": data['Twitter']},
                    # "WEBSITE": {"S": data['Website']},
                    # "TAGS": {"S": data['Tags'] if data['Tags'] != '' else None},
                    "OPERATIONHOURS": {"S": '{\"MON\":[{\"I\":\"8\",\"F\":\"17\"}],\"TUE\":[{\"I\":\"8\",\"F\":\"17\"}],\"WED\":[{\"I\":\"8\",\"F\":\"17\"}],\"THU\":[{\"I\":\"8\",\"F\":\"17\"}],\"FRI\":[{\"I\":\"8\",\"F\":\"17\"}]}'},
                    "TU_CITA_LINK": {"S": data['TuCitaLink'] if data['TuCitaLink'] != '' else None},
                    "ZIPCODE": {"S": data['ZipCode']},
                    "STATUS": {"N": str(1)},
                    "PARENTBUSINESS": {"N": str(0)},
                    "GSI1PK": {"S": 'PARENT#BUS'},
                    "GSI1SK": {"S": data['Name']+'#'+businessId},
                    "GSI2PK": {"S": 'PLAN#' + data['Plan']},
                    "GSI2SK": {"S": 'BUS#' + businessId},
                    "GSI3PK": {"S": 'LINK#' + data['TuCitaLink']},
                    "GSI3SK": {"S": 'BUS#' + businessId},
                    "GSI4PK": {"S": 'SEARCH'},
                    "GSI4SK": {"S": 'SEARCH'}
                },
                "ConditionExpression": "attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
            },
        }
        items.append(cleanNullTerms(rows))

        rows = {
            "Put": {
                "TableName": "TuCita247",
                "Item": {
                    "PKID": {"S": 'LINK#' + data['TuCitaLink']},
                    "SKID": {"S": 'LINK#' + data['TuCitaLink']}
                },
                "ConditionExpression": "attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
            },
        }
        items.append(cleanNullTerms(rows))

        for item in data['Locations']:
            locationId = str(uuid.uuid4()).replace("-","")
            locations = {
                "Put":{
                    "TableName":"TuCita247",
                    "Item": {
                        "PKID": {"S": 'BUS#'+businessId},
                        "SKID": {"S": 'LOC#'+locationId},
                        "NAME": {"S": str(item['Name'])},
                        "CITY": {"S": str(item['City'])},
                        # "SECTOR": {"S": str(item['Sector'])},
                        "ADDRESS": {"S": str(item['Address'])},
                        "DOORS": {"S": 'MAIN DOOR'},
                        "GEOLOCATION": {"S": str(item['Geolocation'])},
                        "OPERATIONHOURS": {"S": '{\"MON\":[{\"I\":\"8\",\"F\":\"17\"}],\"TUE\":[{\"I\":\"8\",\"F\":\"17\"}],\"WED\":[{\"I\":\"8\",\"F\":\"17\"}],\"THU\":[{\"I\":\"8\",\"F\":\"17\"}],\"FRI\":[{\"I\":\"8\",\"F\":\"17\"}]}'},
                        "PARENT_LOCATION": {"N": str(0)},
                        "MANUAL_CHECK_OUT": {"N": str(0)},
                        "PARENTDAYSOFF": {"N": str(1)},
                        "PARENTHOURS": {"N": str(1)},
                        "MAX_CUSTOMER": {"N": str(1)},
                        "PEOPLE_CHECK_IN": {"N": str(0)},
                        "STATUS": {"N": str(1)}
                    },
                    "ConditionExpression": "attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                    "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
                },
            }
            items.append(cleanNullTerms(locations))

            providerId = str(uuid.uuid4()).replace("-","")
            provider = {
                "Put":{
                    "TableName":"TuCita247",
                    "Item": {
                        "PKID": {"S": 'BUS#'+businessId+'#LOC#'+locationId},
                        "SKID": {"S": 'PRO#'+providerId},
                        "NAME": {"S": str(item['Name'])},
                        "OPERATIONHOURS": {"S": '{\"MON\":[{\"I\":\"8\",\"F\":\"17\"}],\"TUE\":[{\"I\":\"8\",\"F\":\"17\"}],\"WED\":[{\"I\":\"8\",\"F\":\"17\"}],\"THU\":[{\"I\":\"8\",\"F\":\"17\"}],\"FRI\":[{\"I\":\"8\",\"F\":\"17\"}]}'},
                        # "DAYS_OFF": {"L": []},
                        "OPEN": {"N": str(0)},
                        "PEOPLE_CHECK_IN": {"N": str(0)},
                        "PARENTDAYSOFF": {"N": str(1)},
                        "PARENTHOURS": {"N": str(1)},
                        "STATUS": {"N": str(1)},
                        "GSI1PK": {"S": 'BUS#'+businessId},
                        "GSI1SK": {"S": 'PRO#'+providerId}
                    },
                    "ConditionExpression": "attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                    "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
                },
            }
            items.append(cleanNullTerms(provider))

            serviceId = str(uuid.uuid4()).replace("-","")
            service = {
                "Put":{
                    "TableName":"TuCita247",
                    "Item": {
                        "PKID": {"S": 'BUS#'+businessId},
                        "SKID": {"S": 'SER#'+serviceId},
                        "NAME": {"S": data['CategoryName']},
                        "TIME_SERVICE": {"N": str(1)},
                        "CUSTOMER_PER_TIME": {"N": str(1)},
                        "COLOR": {"S": str('#D9E1F2')},
                        "STATUS": {"N": str(1)}
                    },
                    "ConditionExpression": "attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                    "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
                },
            }
            items.append(cleanNullTerms(service))

            serviceProv = {
                "Put":{
                    "TableName":"TuCita247",
                    "Item": {
                        "PKID": {"S": 'BUS#'+businessId+'#SER#'+serviceId},
                        "SKID": {"S": 'PRO#'+providerId},
                        "GSI1PK": {"S": 'BUS#'+businessId+'#PRO#'+providerId},
                        "GSI1SK": {"S": 'SER#'+serviceId}
                    },
                    "ConditionExpression": "attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                    "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
                },
            }
            items.append(cleanNullTerms(serviceProv))

            plan = {
                "Put":{
                    "TableName":"TuCita247",
                    "Item": {
                        "PKID": {"S": 'BUS#'+businessId},
                        "SKID": {"S": 'PLAN'},
                        "NAME": {"S": 'FREE'},
                        "ORDER": {"S": str(0)},
                        "PREMIUM": {"N": str(0)},
                        "DUE_DATE": {"S": dueDate},
                        "AVAILABLE": {"N": str(20)},
                        "APPOINTMENTS": {"N": str(20)},
                        "STATUS": {"N": str(1)},
                        "GSI1PK": {"S": dueDate},
                        "GSI1SK": {"S": 'BUS#'+businessId}
                    },
                    "ConditionExpression": "attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                    "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
                },
            }
            items.append(cleanNullTerms(plan))


        logger.info(items)
        response = dynamodb.transact_write_items(
            TransactItems = items
        )
        logger.info(response)

        try:
            passDecrypt = get_random_string()
            logger.info(passDecrypt)
            
            client = boto3.client('cognito-idp')
            response = client.admin_create_user(
                UserPoolId='us-east-1_gXhBD4bsG',
                Username=data['Email'],
                TemporaryPassword=passDecrypt,
                # MessageAction='SUPPRESS',
                UserAttributes=[
                    {
                        'Name': 'email',
                        'Value': data['Email']
                    },
                    {
                        'Name': 'custom:userId',
                        'Value': userId
                    },
                    {
                        'Name': 'custom:wpUserId',
                        'Value': ''
                    },
                    {   
                        'Name': 'custom:isAdmin',
                        'Value': '1'
                    }
                ]
            )
            logger.info(response)

            statusCode = 200
            body = json.dumps({'Message': 'Business created successfully', 'BusinessId': businessId, 'Code': 200})

        except client.exceptions.UsernameExistsException as e:
            statusCode = 404
            body = json.dumps({"Code":400, "error": False, 
                    "success": True, 
                    "message": "This email already exists", 
                    "data": None})
        except client.exceptions.InvalidPasswordException as e:
            statusCode = 404
            body = json.dumps({"Code":400,"error": False, 
                    "success": True, 
                    "message": "Password should have Caps,\
                                Special chars, Numbers", 
                    "data": None})
        except client.exceptions.UserLambdaValidationException as e:
            statusCode = 404
            body = json.dumps({"Code":400,"error": False, 
                    "success": True, 
                    "message": "Email already exists " + str(e), 
                    "data": None})
        
        except Exception as e:
            statusCode = 404
            body = json.dumps({"Code":400,"error": False, 
                    "success": True, 
                    "message": str(e), 
                    "data": None})

        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Message': 'Error on update business', 'BusinessId': businessId, 'Code': 500})
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