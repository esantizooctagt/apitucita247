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

def lambda_handler(event, context):
    stage = event['headers']
    cors = "http://localhost:4200"
    # if stage['origin'] != "http://localhost:4200":
    #     cors = os.environ['prodCors']
    # else:
    #     cors = os.environ['devCors']
        
    try:
        userId = event['pathParameters']['id']
        with conn.cursor() as cur:
            cur.execute("SELECT REPLACE(BIN_TO_UUID(USERID, true),'-','') AS USERID, EMAIL, USERNAME, FIRST_NAME, LAST_NAME, AVATAR, REPLACE(BIN_TO_UUID(STOREID, true),'-','') AS STOREID, PASSWORD, REPLACE(BIN_TO_UUID(COMPANYID, true),'-','') AS COMPANYID, IS_ADMIN, STATUS, REPLACE(BIN_TO_UUID(ROLEID, true),'-','') AS ROLEID, MFACT_AUTH, LANGUAGEID FROM USERS WHERE STATUS IN (0,1,3) AND REPLACE(BIN_TO_UUID(USERID, true),'-','') = %s", userId)
            record = cur.fetchone()
            if record == None:
                conn.commit()
                statusCode = 404
                body = json.dumps({'Message':'No valid entry found for user ID'})
            else:
                cur.execute("SELECT NAME FROM COMPANIES WHERE REPLACE(BIN_TO_UUID(COMPANYID, true),'-','') = %s",  record[8])
                comp = cur.fetchone()
                
                if comp == None:
                    conn.commit()
                    statusCode = 404
                    body = json.dumps({'Message':'No valid entry found for user ID'})
                else:
                    conn.commit()
                    recordset = {
                        'User_Id': record[0],
                        'Email': record[1],
                        'User_Name': record[2],
                        'First_Name': record[3],
                        'Last_Name': record[4],
                        'Avatar': record[5],
                        'Company_Name': comp[0],
                        'Store_Id': record[6],
                        'Password': record[7],
                        'Is_Admin': record[9],
                        'Company_Id': record[8],
                        'Status': record[10],
                        'Role_Id': record[11],
                        'MFact_Auth': record[12],
                        'Language_Id': record[13]
                    }
                    statusCode = 200
                    body = json.dumps(recordset)
                    
    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message': 'Error on request try again'})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : cors
        },
        'body' : body
    }
    return response