import sys
import logging
import json


import os

REGION = 'us-east-1'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']
        
    try:
        roleId = event['pathParameters']['id']
        businessId = event['pathParameters']['businessId']

        cur.execute("SELECT REPLACE(BIN_TO_UUID(ROLEID, true),'-','') AS ROLEID, REPLACE(BIN_TO_UUID(COMPANYID, true),'-','') AS COMPANYID, NAME, STATUS FROM ROLES WHERE STATUS IN (0,1) AND REPLACE(BIN_TO_UUID(ROLEID, true),'-','') = %s", (roleId))
        
        if result == None:
            statusCode = 404
            body = json.dumps({'Message':'No valid entry found for tax ID'})
        else:
            recordset = {
                'Role_Id': result[0],
                'Company_Id': result[1],
                'Name': result[2],
                'Status': result[3]
            }
            statusCode = 200
            body = json.dumps(recordset)
    except: #Exception as e:
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