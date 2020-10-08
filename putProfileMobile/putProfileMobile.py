import sys
import logging
import json

import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr

import os

REGION = 'us-east-1'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb', region_name=REGION)
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
    try:
        statusCode = ''
        customerId = event['pathParameters']['customerId']
        mobile = event['pathParameters']['mobile']
        data = json.loads(event['body'])

        e = {'#n': 'NAME'}
        table = dynamodb.Table('TuCita247')
        expUpdate = {
                ':name': data['Name'],
                ':email': data['Email'] if data['Email'] != '' else None,
                ':dob': data['DOB'] if data['DOB'] != '' else None,
                ':gender': data['Gender'] if data['Gender'] != '' else None,
                ':preferences': data['Preferences'] if data['Preferences'] != '' else None,
                ':disability': data['Disability'] if data['Disability'] != '' else None
        }
        rem = (" REMOVE EMAIL" if data['Email'] == '' else '') 
        rem = rem + ((", DOB" if rem != '' else " REMOVE DOB") if data['DOB'] == '' else '')
        rem = rem + ((", GENDER" if rem != '' else " REMOVE GENDER") if data['Gender'] == '' else '')
        rem = rem + ((", PREFERENCES" if rem != '' else " REMOVE PREFERENCES") if data['Preferences'] == '' else '')
        rem = rem + ((", DISABILITY" if rem != '' else " REMOVE DISABILITY" ) if data['Disability'] == '' else '')
        response = table.update_item(
            Key={
                'PKID': 'MOB#' + mobile,
                'SKID': 'CUS#' + customerId
            },
            UpdateExpression="SET #n = :name" + (", EMAIL = :email" if data['Email'] != '' else '') + (", DOB = :dob" if data['DOB'] != '' else '') + (", GENDER = :gender" if data['Gender'] != '' else '') + (", PREFERENCES = :preferences" if data['Preferences'] != '' else '') + (", DISABILITY = :disability" if data['Disability'] != '' else '') + rem,
            ExpressionAttributeNames=e,
            ExpressionAttributeValues=cleanNullTerms(expUpdate),
            ConditionExpression="attribute_exists(PKID) AND attribute_exists(SKID)"
            # ReturnValues="UPDATED_NEW"
        )

        statusCode = 200
        body = json.dumps({'Message': 'User edited successfully', 'Code': 200})

        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Message': 'Error on edit user', 'Code': 400})
    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message': 'Error on request try again ' + str(e), 'Code': 400})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : "*"
        },
        'body' : body
    }
    return response