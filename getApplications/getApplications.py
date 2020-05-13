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

def lambda_handler(event, context):
    stage = ''
    roleId = ''
    body = ''
    cors = ''
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']

    records =[]
    try:
        roleId = event['pathParameters']['id']
        # if roleId == '0':
        #     cur.execute("SELECT REPLACE(BIN_TO_UUID(APPLICATIONID, true),'-','') AS APPLICATIONID, NAME, 0 AS ACTIVE, ICON, ROUTE FROM APPLICATIONS WHERE STATUS IN (0,1) ORDER BY ORDERAPP")
        # elif roleId == '1':
        #     cur.execute("SELECT REPLACE(BIN_TO_UUID(APPLICATIONID, true),'-','') AS APPLICATIONID, NAME, 1 AS ACTIVE, ICON, ROUTE FROM APPLICATIONS WHERE STATUS IN (0,1) ORDER BY ORDERAPP")
        # else:
        #     cur.execute("SELECT REPLACE(BIN_TO_UUID(APPLICATIONS.APPLICATIONID, true),'-','') AS APPLICATIONID, APPLICATIONS.NAME, CASE WHEN ROLEID IS NULL THEN 0 ELSE 1 END AS ACTIVE, ICON, ROUTE FROM APPLICATIONS LEFT JOIN ACCESS ON APPLICATIONS.APPLICATIONID = ACCESS.APPLICATIONID AND REPLACE(BIN_TO_UUID(ACCESS.ROLEID, true),'-','') = %s WHERE APPLICATIONS.STATUS IN (0,1) ORDER BY ORDERAPP", (roleId))
        response = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            # IndexName="TuCita247_Index",
            # KeyConditionExpression='GS1PK = :email',
            # ExpressionAttributeValues={
            #     ':email': {'S': 'EMAIL#' + data['Email']}
            # },
            Limit=1
        )
        for row in response['Items']:
            recordset = {
                'Application_Id': row['APPLICATIONID'],
                'Name': row['NAME'],
                'Active': row['ACTIVE'],
                'Icon': row['ICON'],
                'Route': row['ROUTE']
            }
            records.append(recordset)
            
            statusCode = 200
            body = json.dumps(records)
    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message': 'Error on request try again ' + str(e)})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "Access-Control-Allow-Origin" : cors
        },
        'body' : body
    }
    return response