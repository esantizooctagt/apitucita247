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

cognitoId = os.environ['cognitoId']
cognitoClientId = os.environ['cognitoClientId']
cognitoSecret = os.environ['cognitoSecret']

dynamodb = boto3.client('dynamodb', region_name=REGION)
ses = boto3.client('ses',region_name=REGION)
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
    msg = username + cognitoClientId
    dig = hmac.new(str(cognitoSecret).encode('utf-8'), 
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
        
    try:
        statusCode = ''
        language = ''
        data = json.loads(event['body'])
        businessId = str(uuid.uuid4()).replace("-","")
        userId = str(uuid.uuid4()).replace("-","")
        language = data['Language']

        country_date = dateutil.tz.gettz('America/Puerto_Rico')
        today = datetime.datetime.now(tz=country_date)
        dueDate = (today + datetime.timedelta(days=31)).strftime("%Y-%m-%d")
        dateOpe = today.strftime("%Y-%m-%d-%H-%M-%S")
        sectorCod = ''
        for item in data['Locations']:
            sectorCod = str(item['Sector'])
            
        cityName = data['City']

        items = []
        locationId = str(uuid.uuid4()).replace("-","")
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
                    "COUNTRY": {"S": data['CountryCode'] if data['CountryCode'] != '' else None},
                    "IS_ADMIN": {"N": str(1)},
                    "USERID": {"S": userId },
                    "DOOR": {"S": 'MAIN DOOR'},
                    "SUPER_ADMIN": {"N": str(0)},
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
                    "PKID": {"S": 'BUS#'+businessId},
                    "SKID": {"S": data['CategoryId']},
                    "GSI1PK": {"S": 'BUS#CAT'},
                    "GSI1SK": {"S": data['CategoryId']+'#'+businessId},
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
                    "SECTOR": {"S": sectorCod},
                    "CITY": {"S": cityName},
                    "COUNTRY": {"S": data['Country']},
                    "EMAIL": {"S": data['Email']},
                    "SHORTDESCRIPTION": {"S": str(data['Description'])},
                    "LONGDESCRIPTION": {"S": str(data['Description'])},
                    "GEOLOCATION": {"S": data['Geolocation']},
                    "NAME": {"S": data['Name']},
                    "PHONE": {"S": data['Phone']},
                    "COUNTRYCODE": {"S": data['CountryCode']},
                    # "TAGS": {"S": data['Tags'] if data['Tags'] != '' else None},
                    "OPERATIONHOURS": {"S": '{\"MON\":[{\"I\":\"8\",\"F\":\"17\"}],\"TUE\":[{\"I\":\"8\",\"F\":\"17\"}],\"WED\":[{\"I\":\"8\",\"F\":\"17\"}],\"THU\":[{\"I\":\"8\",\"F\":\"17\"}],\"FRI\":[{\"I\":\"8\",\"F\":\"17\"}]}'},
                    "DAYS_OFF": {"L": []},
                    "REASONS": {"S": 'OTHER'},
                    "TU_CITA_LINK": {"S": data['TuCitaLink'] if data['TuCitaLink'] != '' else None},
                    "ZIPCODE": {"S": data['ZipCode']},
                    "STATUS": {"N": str(1)},
                    "LANGUAGE": {"S": data['Language']},
                    "PARENTBUSINESS": {"N": str(0)},
                    "CREATED_DATE": {"S": str(dateOpe)},
                    "GSI2PK": {"S": 'PLAN#' + data['Plan']},
                    "GSI2SK": {"S": 'BUS#' + businessId},
                    "GSI3PK": {"S": 'LINK#' + data['TuCitaLink']},
                    "GSI3SK": {"S": 'BUS#' + businessId},
                    "GSI4PK": {"S": 'SEARCH'},
                    "GSI4SK": {"S": 'SEARCH'},
                    "GSI5PK": {"S": 'METADATA'},
                    "GSI5SK": {"S": 'BUS#'+businessId},
                    "GSI11PK": {"S": "DT#" + str(dateOpe)[0:10]},
                    "GSI11SK": {"S": "BUS#" + businessId}
                },
                "ConditionExpression": "attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
            },
        }
        items.append(cleanNullTerms(rows))

        cats = data['CategoryId'].split('#')
        rows = {
            "Update": {
                "TableName": "TuCita247",
                "Key": {
                    "PKID": {"S": 'CAT#'+cats[1]},
                    "SKID": {"S": 'CAT#'+cats[1]}
                },
                "UpdateExpression": "SET QTY = QTY + :increment",
                "ExpressionAttributeValues": { 
                    ":increment": {"N": str(1)}
                },
                "ConditionExpression": "attribute_exists(PKID) AND attribute_exists(SKID)",
                "ReturnValuesOnConditionCheckFailure": "ALL_OLD" 
            }
        }
        items.append(rows)

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
            locations = {
                "Put":{
                    "TableName":"TuCita247",
                    "Item": {
                        "PKID": {"S": 'BUS#'+businessId},
                        "SKID": {"S": 'LOC#'+locationId},
                        "NAME": {"S": str(item['Name'])},
                        "CITY": {"S": str(item['City'])},
                        "SECTOR": {"S": str(item['Sector'])},
                        "ADDRESS": {"S": str(item['Address'])},
                        "ZIPCODE": {"S": data['ZipCode']},
                        "DOORS": {"S": 'MAIN DOOR'},
                        "GEOLOCATION": {"S": str(item['Geolocation'])},
                        "OPERATIONHOURS": {"S": '{\"MON\":[{\"I\":\"8\",\"F\":\"17\"}],\"TUE\":[{\"I\":\"8\",\"F\":\"17\"}],\"WED\":[{\"I\":\"8\",\"F\":\"17\"}],\"THU\":[{\"I\":\"8\",\"F\":\"17\"}],\"FRI\":[{\"I\":\"8\",\"F\":\"17\"}]}'},
                        "DAYS_OFF": {"L": []},
                        "OPEN": {"N": str(0)},
                        "PARENT_LOCATION": {"N": str(0)},
                        "MANUAL_CHECK_OUT": {"N": str(1)},
                        "PARENTDAYSOFF": {"N": str(1)},
                        "PARENTHOURS": {"N": str(1)},
                        "MAX_CUSTOMER": {"N": str(item['MaxCustomer'])},
                        "PEOPLE_CHECK_IN": {"N": str(0)},
                        "TIME_ZONE": {"S": 'America/Puerto_Rico'},
                        "STATUS": {"N": str(1)},
                        "CREATED_DATE": {"S": str(dateOpe)}
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
                        "NAME": {"S": str(data['Provider'])},
                        "OPERATIONHOURS": {"S": '{\"MON\":[{\"I\":\"8\",\"F\":\"17\"}],\"TUE\":[{\"I\":\"8\",\"F\":\"17\"}],\"WED\":[{\"I\":\"8\",\"F\":\"17\"}],\"THU\":[{\"I\":\"8\",\"F\":\"17\"}],\"FRI\":[{\"I\":\"8\",\"F\":\"17\"}]}'},
                        "DAYS_OFF": {"L": []},
                        "PARENTDAYSOFF": {"N": str(1)},
                        "PARENTHOURS": {"N": str(1)},
                        "STATUS": {"N": str(1)},
                        "GSI1PK": {"S": 'BUS#'+businessId},
                        "GSI1SK": {"S": 'PRO#'+providerId},
                        "CREATED_DATE": {"S": str(dateOpe)}
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
                        "TIME_SERVICE": {"N": str(100)},
                        "BUFFER_TIME": {"N": str(10)},
                        "CUSTOMER_PER_TIME": {"N": str(1)},
                        "CUSTOMER_PER_BOOKING": {"N": str(1)},
                        "COLOR": {"S": str('#D9E1F2')},
                        "STATUS": {"N": str(1)},
                        "CREATED_DATE": {"S": str(dateOpe)}
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
                        "EXPIRE": {"N": str(1)},
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
                UserPoolId=cognitoId,
                Username=data['Email'],
                TemporaryPassword=passDecrypt,
                MessageAction='SUPPRESS',
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
                    },
                    {
                        'Name': 'email_verified',
                        'Value': 'True'
                    }
                ]
            )
            logger.info(response)

            #EMAIL
            SENDER = "Tu Cita 24/7 <no-reply@tucita247.com>"
            RECIPIENT = data['Email']

            if language == 'en':
                LAN = 'EN'
            else:
                LAN = 'ES'

            logger.info("prev send email")
            response = ses.send_templated_email(
                Source=SENDER,
                Destination={
                    'ToAddresses': [
                        RECIPIENT,
                    ],
                },
                Template ='WELCOME_' + LAN, 
                TemplateData='{ "userId": "'+ userId +'", "passDecrypt": "' + passDecrypt + '" }'
            )
            logger.info("Success BUsinessId --> " + businessId)
            statusCode = 200
            body = json.dumps({'Message': 'Business created successfully', 'BusinessId': businessId, 'Code': 200})

        except client.exceptions.UsernameExistsException as e:
            statusCode = 404
            body = json.dumps({"Code":400, "error": False, 
                    "success": True, 
                    'BusinessId': '',
                    "message": "This email already exists", 
                    "data": None})
        except client.exceptions.InvalidPasswordException as e:
            statusCode = 404
            body = json.dumps({"Code":400,"error": False, 
                    "success": True, 
                    'BusinessId': '',
                    "message": "Password should have Caps,\
                                Special chars, Numbers", 
                    "data": None})
        except client.exceptions.UserLambdaValidationException as e:
            statusCode = 404
            body = json.dumps({"Code":400,"error": False, 
                    "success": True,
                    'BusinessId': '',
                    "message": "Email already exists " + str(e), 
                    "data": None})
        
        except Exception as e:
            statusCode = 404
            body = json.dumps({"Code":400,"error": False, 
                    "success": True, 
                    'BusinessId': '',
                    "message": str(e), 
                    "data": None})

        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Message': 'Error on update business', 'BusinessId': '', 'Code': 500})
    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message': 'Error on request try again ' + str(e)})
        logger.error("catch line no. 492")
        logger.error(str(e))
        
    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : cors
        },
        'body' : body
    }
    return response