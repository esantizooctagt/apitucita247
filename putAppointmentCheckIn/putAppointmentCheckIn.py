import sys
import logging
import json

import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr

from dynamodb_json import json_util as json_dynamodb

import datetime
import dateutil.tz
from datetime import datetime, timezone, timedelta

import os

REGION = 'us-east-1'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.client('dynamodb', region_name=REGION)
lambdaInv = boto3.client('lambda')
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

def findTimeZone(businessId, locationId):
    timeZone='America/Puerto_Rico'
    locZone = dynamodb.query(
        TableName="TuCita247",
        ReturnConsumedCapacity='TOTAL',
        KeyConditionExpression='PKID = :key AND SKID = :skey',
        ExpressionAttributeValues={
            ':key': {'S': 'BUS#'+businessId},
            ':skey': {'S': 'LOC#'+locationId}
        }
    )
    for timeLoc in json_dynamodb.loads(locZone['Items']):
        timeZone = timeLoc['TIME_ZONE'] if 'TIME_ZONE' in timeLoc else 'America/Puerto_Rico'
    return timeZone

def lambda_handler(event, context):
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']
        
    try:
        statusCode = ''
        typeAppo = ''
        businessId = ''
        serviceId = ''
        customerId = ''
        servName = ''
        provName = ''
        manualCheckIn = 0
        timeService = 0
        bufferTime = 0
        realTimeService = 0
        data = json.loads(event['body'])
        appointmentId = event['pathParameters']['id']
        status = data['Status']
        dateAppo = data['DateAppo']
        qty = data['Guests']
        qrCode = data['qrCode'].upper() if 'qrCode' in data else ''
        businessId = data['BusinessId'] if 'BusinessId' in data else ''
        locationId = data['LocationId'] if 'LocationId' in data else ''
        providerId = data['ProviderId'] if 'ProviderId' in data else ''

        country_date = dateutil.tz.gettz(findTimeZone(businessId, locationId))
        today = datetime.now(tz=country_date)
        dateOpe = today.strftime("%Y-%m-%d-%H-%M-%S")
        initDate = ''
        businessName = ''
        Address = ''
        TimeZone = ''
        operation = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :pkid AND SKID = :skid',
            ExpressionAttributeValues={
                ':pkid': {'S': 'BUS#' + businessId},
                ':skid': {'S': 'LOC#' + locationId}
            }
        )
        for ope in json_dynamodb.loads(operation['Items']):
            manualCheckIn = int(ope['MANUAL_CHECK_OUT'])
            businessName = ope['NAME'],
            Address = ope['ADDRESS'],
            TimeZone = ope['TIME_ZONE'] if 'TIME_ZONE' in ope else 'America/Puerto_Rico'

        response = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :pkid AND SKID = :skid',
            ExpressionAttributeValues={
                ':pkid': {'S': 'APPO#' + appointmentId},
                ':skid': {'S': 'APPO#' + appointmentId}
            }
        )
        for row in json_dynamodb.loads(response['Items']):
            typeAppo = row['TYPE']
            serviceId = row['SERVICEID']
            customerId = row['GSI2PK'].replace('CUS#','')
        
        service = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :pkid AND SKID = :skid',
            ExpressionAttributeValues={
                ':pkid': {'S': 'BUS#' + businessId},
                ':skid': {'S': 'SER#' + serviceId}
            }
        )
        for serv in json_dynamodb.loads(service['Items']):
            timeService = int(serv['TIME_SERVICE'])*60
            bufferTime = int(serv['BUFFER_TIME'])

        servs = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :businessId AND begins_with(SKID , :serv)',
            ExpressionAttributeValues={
                ':businessId': {'S': 'BUS#' + businessId},
                ':serv': {'S': 'SER#' }
            }
        )
        count = 0
        for serv in json_dynamodb.loads(servs['Items']):
            count = count + 1
            if serv['SKID'].replace('SER#','') == serviceId:
                servName = serv['NAME']
        if count == 1:
            servName = ''
        
        provs =  dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :key AND begins_with(SKID , :provs)',
            ExpressionAttributeValues={
                ':key': {'S': 'BUS#' + businessId + '#LOC#'+locationId},
                ':provs': {'S': 'PRO#' }
            }
        )
        countp = 0
        for prov in json_dynamodb.loads(provs['Items']):
            countp = countp + 1
            if prov['SKID'].replace('PRO#','') == providerId:
                provName = prov['NAME']
        if countp == 1:
            provName = ''

        initDate = ((datetime.strptime(today.strftime("%Y-%m-%d-%H-00"),"%Y-%m-%d-%H-%M"))+timedelta(minutes=bufferTime*2)).strftime("%Y-%m-%d-%H-%M")
        if today.strftime("%Y-%m-%d-%H-%M") > initDate:
            dateAppo = (today + timedelta(hours=1)).strftime("%Y-%m-%d-%H-00")
        else:
            if dateAppo > today.strftime("%Y-%m-%d-%H-%M"):
                dateAppo = today.strftime("%Y-%m-%d-%H-00")

        items = []
        recordset = {
            "Update": {
                "TableName": "TuCita247",
                "Key": {
                    "PKID": {"S": 'APPO#' + appointmentId}, 
                    "SKID": {"S": 'APPO#' + appointmentId}
                },
                "UpdateExpression": "REMOVE GSI8PK, GSI8SK SET DATE_APPO = :dateAppo, MODIFIED_DATE = :mod_date, #s = :status, GSI1SK = :key, GSI2SK = :key2, TIMECHECKIN = :dateOpe, PEOPLE_QTY = :qty, GSI5PK = :key05, GSI5SK = :skey05, GSI6PK = :key06, GSI6SK = :skey06, GSI7PK = :key07, GSI7SK = :skey07, GSI9SK = :key, GSI10SK = :dateAppo" + ("" if typeAppo != 2 else ", GSI4PK = :key4, GSI4SK = :skey4"), 
                "ExpressionAttributeValues": {
                    ":status": {"N": "3"}, 
                    ":key": {"S": str(status) + '#DT#' + str(dateAppo)}, 
                    ":key2": {"S": str(status) + '#DT#' + str(dateAppo)},   #'#5' if str(status) == '5' else 
                    ":dateOpe": {"S": str(dateOpe)},
                    ":qrCode": {"S": qrCode},
                    ":key4": {"S": None if typeAppo != 2 else "BUS#" + businessId + "#LOC#" + locationId},
                    ":qty": {"N": str(qty)},
                    ":skey4": {"S": None if typeAppo != 2 else str(status) + "#DT#" + str(dateAppo) + "#" + appointmentId},
                    ":key05": {"S" : 'BUS#' + businessId},
                    ":skey05": {"S" : str(dateAppo)[0:10]+'#APPO#' + appointmentId},
                    ":key06": {"S" : 'BUS#' + businessId + '#LOC#' + locationId},
                    ":skey06": {"S" : str(dateAppo)[0:10]+'#APPO#' + appointmentId},
                    ":key07": {"S" : 'BUS#' + businessId + '#LOC#' + locationId + '#PRO#' + providerId},
                    ":skey07": {"S" : str(dateAppo)[0:10]+'#APPO#' + appointmentId},
                    ":dateAppo": {"S": dateAppo},
                    ":mod_date": {"S": str(dateOpe)}
                },
                "ExpressionAttributeNames": {'#s': 'STATUS'},
                "ConditionExpression": "attribute_exists(PKID) AND attribute_exists(SKID) AND QRCODE = :qrCode",
                "ReturnValuesOnConditionCheckFailure": "ALL_OLD" 
            }
        }
        items.append(cleanNullTerms(recordset))

        recordset = {
            "Put": {
                "TableName": "TuCita247",
                "Item": {
                    "PKID": {"S": 'LOG#' + str(dateOpe)[0:10]},
                    "SKID": {"S": 'APPO#' + appointmentId + '#' + str(dateOpe)},
                    "DATE_APPO": {"S": str(dateOpe)},
                    "STATUS": {"N": str(status)}
                },
                "ConditionExpression": "attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
                "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
                }
            }
        items.append(cleanNullTerms(recordset))

        recordset = {
            "Update": {
                "TableName": "TuCita247",
                "Key": {
                    "PKID": {"S": 'BUS#' + businessId}, 
                    "SKID": {"S": 'LOC#' + locationId}, 
                },
                "UpdateExpression": "SET PEOPLE_CHECK_IN = PEOPLE_CHECK_IN + :increment",
                "ExpressionAttributeValues": { 
                    ":increment": {"N": str(qty)}
                },
                "ConditionExpression": "attribute_exists(PKID) AND attribute_exists(SKID)",
                "ReturnValuesOnConditionCheckFailure": "ALL_OLD" 
            }
        }
        items.append(recordset)

        tranAppo = dynamodb.transact_write_items(
            TransactItems = items
        )
        data = {
            'BusinessId': businessId,
            'LocationId': locationId,
            'AppId': appointmentId,
            'CustomerId': customerId,
            'Guests': qty,
            'Tipo': 'MOVE',
            'ManualCheckOut': manualCheckIn,
            'To': 'CHECKIN',
            'AppointmentId': row['PKID'].replace('APPO#',''),
            'Status': row['STATUS'],
            'Address': Address,
            'NameBusiness': businessName,
            'PeopleQty': row['PEOPLE_QTY'],
            'QrCode': row['QRCODE'],
            'UnRead': 0,
            'Ready': 0,
            'DateAppo': row['DATE_APPO'],
            'DateFull': row['DATE_APPO'],
            'Disability': row['DISABILITY'] if 'DISABILITY' in row else '',
            'Door': row['DOOR'],
            'Name': row['NAME'],
            'OnBehalf': row['ON_BEHALF'],
            'Phone': row['PHONE'],
            'TimeZone': TimeZone,
            'Service': servName,
            'Provider': provName
        }
        lambdaInv.invoke(
            FunctionName='PostMessages',
            InvocationType='Event',
            Payload=json.dumps(data)
        )
        
        logger.info(tranAppo)
        statusCode = 200
        body = json.dumps({'Message': 'Appointment updated successfully', 'Code': 200})

        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Message': 'Error on update appointment', 'Code': 500})
    except dynamodb.exceptions.TransactionCanceledException as e:
        statusCode = 404
        body = json.dumps({'Message': 'QR Code invalid ' + str(e), 'Code': 404})
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