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
        businessId = event['pathParameters']['id']
        # cur.execute("SELECT REPLACE(BIN_TO_UUID(COMPANYID, true),'-','') AS COMPANYID, NAME, ADDRESS, HOUSE_NO, COUNTRY, STATE, PHONE, POSTAL_CODE, TAX_NUMBER, EMAIL, STORE_NO, STATUS, IFNULL(CURRENCY,'') AS CURRENCY, CASHIER_NO FROM COMPANIES WHERE STATUS IN (0,1) AND REPLACE(BIN_TO_UUID(COMPANYID, true),'-','') = %s", companyId)
        recordset = {
            'Company_Id': record[0],
            'Name': record[1],
            'Address': record[2],
            'House_No': record[3],
            'Country': record[4],
            'State': record[5],
            'Phone': record[6],
            'Postal_Code': record[7],
            'Tax_Number': record[8],
            'Email': record[9],
            'Store_No': record[10],
            'Status': record[11],
            'Currency': record[12],
            'Cashier_No': record[13]
        }
        statusCode = 200
        body = json.dumps(recordset)
    except:
        statusCode = 500
        body = json.dumps({'Message':'Error on request try again'})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : cors
        },
        'body' : body
    }
    return response