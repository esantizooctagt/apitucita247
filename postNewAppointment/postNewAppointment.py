import sys
import logging
import json

import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr
from dynamodb_json import json_util as json_dynamodb

import uuid
import os

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
        data = json.loads(event['body'])
        businessId = data['BusinessId']
        locationId = data['LocationId']
        door = data['Door']
        phone = data['Phone']
        name = data['Name']
        email = data['Email']
        dob = data['DOB']
        gender = data['Gender']
        preference = data['Preference']
        disability = data['Disability']
        companions = data['Companions']
        customerId = str(uuid.uuid4()).replace("-","")
        existe = 0
        dateAppo = '2020-05-25-10-00' # OBTENER LA FECHA Y HORA ACTUAL

        if phone != '0000000000':
            # SEARCH FOR PHONE NUMBER
            getPhone = dynamodb.query(
                TableName = "TuCita247",
                ReturnConsumedCapacity = 'TOTAL',
                KeyConditionExpression = 'PKID = :phone',
                ExpressionAttributeValues = {
                    'phone': {'S', 'MOB#' + phone}
                },
                Limit = 1
            )
            for phoneNumber in json_dynamodb.loads(getPhone['Items']):
                existe = 1
                customerId = phoneNumber['SKID'].replace('CUS#','')
                name = (phoneNumber['NAME'] if name == '' else name)
                email = (phoneNumber['EMAIL'] if email == '' else email)
                dob = (phoneNumber['DOB'] if dob == '' else dob)
                gender = (phoneNumber['GENDER'] if gender == '' else gender)
                preference = (phoneNumber['PREFERENCES'] if dob == '' else preference)
        
        recordset = {}
        items = []
        if existe == 0:
            recordset = {
                "Put": {
                    "TableName": "TuCita247",
                    "Item": {
                        "PKID": {"S": 'MOB#'+phone},
                        "SKID": {"S": 'CUS#'+customerId},
                        "STATUS": {"N": "1"},
                        "NAME": {"S": name},
                        "GSI1PK": {"S": "CUS#TOT"},
                        "GSI1SK": {"S": name+'#'+customerId},
                        "EMAIL": {"S": email},
                        "DOB": {"S": dob},
                        "GENDER": {"S": gender},
                        "PREFERENCES": {"N": preference}
                    },
                    "ConditionExpression": "attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                    "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
                }
            }
            items.append(recordset)
        
        appoId = str(uuid.uuid4()).replace("-","")
        recordset = {}
        recordset = {
            "Put": {
                "TableName": "TuCita247",
                "Item": {
                    "PKID": {"S": 'APPO#'+appoId},
                    "SKID": {"S": 'APPO#'+appoId},
                    "STATUS": {"N": "1"},
                    "NAME": {"S": name},
                    "GSI1PK": {"S": "BUS#"+businessId+'#LOC#'+locationId},
                    "GSI1SK": {"S": '1#DT#'+dateAppo},
                    "PHONE": {"S": phone},
                    "DATE_APPO": {"S": dateAppo},
                    "GSI2PK": {"S": 'CUS#'+customerId},
                    "GSI2SK": {"S": '1#DT#'+dateAppo},
                    "PEOPLE_QTY": {"N": companions},
                    "TYPE": {"S": "2"},
                    "DISABILITY": {"S", disability}
                },
                "ConditionExpression": "attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
            }
        }
        items.append(recordset)
        
        logger.info(items)
        response = dynamodb.transact_write_items(
            TransactItems = items
        )

        # appoInfo = {}
        # getAppointmentInfo = dynamodb.query(
        #     TableName = "TuCita247",
        #     ReturnConsumedCapacity = 'TOTAL',
        #     KeyConditionExpression = 'APPO#' + appoId,
        #     Limit = 1
        # )
        # for row in json_dynamodb.loads(getAppointmentInfo):
        appoInfo = {
            'AppId': appoId,
            'ClientId': customerId,
            'Name': name,
            'Phone': phone,
            'OnBehalf': "0",
            'PeoQty': companions,
            'DateAppo': dateAppo[-5:],
            'DateFull': dateAppo
        }

        logger.info(response)
        statusCode = 200
        body = json.dumps({'Message': 'Role added successfully', 'Code': 200, 'Appointment': appoInfo})
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