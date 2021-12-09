import sys
import logging
import json
import requests
import os

REGION = 'us-east-1'
SITEID = os.environ['SiteId']

logger = logging.getLogger()
logger.setLevel(logging.INFO)
    
def lambda_handler(event, context):
    stage = event['headers']
    res = ''

    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']

    try:
        data = json.loads(event['body'])
        sessionId = data['sessionId']

        header = {"Content-Type": "application/json","SiteId": SITEID}
        req = requests.get("https://www.agilpay.net/WebApi5Test/APaymentTokenApi/GetHash?contentHash=" + SITEID + sessionId + data['AccountNumber'] + data['CustomerName'] + data['CustomerId'] + '1', headers=header)
        hashAcct = req.json()
        
        header = {"Content-Type": "application/json","SiteId": SITEID, "SessionId": sessionId, "MessageHash": hashAcct}
        payload ={
            "MerchantKey": data['MerchantKey'], 
            "AccountNumber": data['AccountNumber'], 
            "ExpirationMonth": data['ExpirationMonth'], 
            "ExpirationYear": data['ExpirationYear'],
            "CustomerName": data['CustomerName'],
            "IsDefault": data['IsDefault'],
            "CustomerId": data['CustomerId'],
            "AccountType": data['AccountType'],
            "CustomerEmail": data['CustomerEmail'],
            "ZipCode": data['ZipCode']   
        }

        req = requests.post("https://www.agilpay.net/WebApi/APaymentTokenApi/RegisterToken", headers=header, data=json.dumps(payload))
        res = req.json()

        statusCode = 200
        body = json.dumps({'Data': res, 'Code': 200})
    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message': 'Error on request try again ' + str(e), 'Code': 500})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : cors
        },
        'body' : body
    }
    return response