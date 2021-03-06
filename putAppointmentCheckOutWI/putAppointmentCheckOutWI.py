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

import os

REGION = 'us-east-1'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.client('dynamodb', region_name=REGION)
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']
        
    try:
        # statusCode = ''
        # appointmentId = ''
        # dateAppo = ''
        # timeCheckIn = ''
        # existe = 0
        # qty = 0
        # data = json.loads(event['body'])
        # status = 4
        # businessId = data['BusinessId']
        # locationId = data['LocationId']

        # country_date = dateutil.tz.gettz('America/Puerto_Rico')
        # today = datetime.datetime.now(tz=country_date)
        # dateOpe = today.strftime("%Y-%m-%d-%H-%M-%S")
        
        # country_date = dateutil.tz.gettz('America/Puerto_Rico')
        # outTime = datetime.datetime.now(tz=country_date)
        # outTime = outTime.strftime('%Y-%m-%d %H:%M:%S.%f')
        # outTime = datetime.datetime.strptime(outTime,'%Y-%m-%d %H:%M:%S.%f')

        # for appo in data['Appos']:
        #     timeCheckIn = ''
        #     response = dynamodb.query(
        #         TableName="TuCita247",
        #         ReturnConsumedCapacity='TOTAL',
        #         KeyConditionExpression='PKID = :key AND SKID = :key',
        #         ExpressionAttributeValues={
        #             ':key': {'S': 'APPO#'+appo['AppointmentId']}
        #         }
        #     )
        #     for row in json_dynamodb.loads(response['Items']):
        #         timeCheckIn = row['TIMECHECKIN'] + '-000000' if 'TIMECHECKIN' in row else ''

        #         if timeCheckIn != '':
        #             inTime = datetime.datetime.strptime(timeCheckIn, '%Y-%m-%d-%H-%M-%S-%f')
        #             citaTime = outTime - inTime
        #             citaTime = int(str(citaTime).split(':')[0]) + int(str(citaTime).split(':')[1])/60
        #             dateAvg = str(appo['DateAppo'])[0:10]

        #             response = dynamodb.query(
        #                 TableName="TuCita247",
        #                 ReturnConsumedCapacity='TOTAL',
        #                 KeyConditionExpression='PKID = :key01 AND SKID = :key02',
        #                 ExpressionAttributeValues={
        #                     ':key01': {'S': 'LOC#'+locationId+'#PRO#'+providerId+'#DT#'+dateAvg[0:7]},
        #                     ':key02': {'S': 'DT#'+dateAvg}
        #                 }
        #             )
        #             for row in json_dynamodb.loads(response['Items']):
        #                 timeAct = row['TIME_APPO']
        #                 existe = 1

        #     items = []
        #     recordset = {
        #         "Update": {
        #             "TableName": "TuCita247",
        #             "Key": {
        #                 "PKID": {"S": 'APPO#' + appo['AppointmentId']}, 
        #                 "SKID": {"S": 'APPO#' + appo['AppointmentId']}
        #             },
        #             "UpdateExpression": "SET #s = :status, GSI1SK = :key, GSI2SK = :key2, TIMECHECKOUT = :dateOpe REMOVE GSI4PK, GSI4SK", 
        #             "ExpressionAttributeValues": {
        #                 ":status": {"N": str(status)}, 
        #                 ":key": {"S": str(status) + '#DT#' + str(appo['DateAppo'])}, 
        #                 ":key2": {"S": '#5' if str(status) == '5' else str(appo['DateAppo'])[0:10]}, 
        #                 ":dateOpe": {"S": str(dateOpe)}
        #             },
        #             "ExpressionAttributeNames": {'#s': 'STATUS'},
        #             "ConditionExpression": "attribute_exists(PKID) AND attribute_exists(SKID)",
        #             "ReturnValuesOnConditionCheckFailure": "ALL_OLD" 
        #         }
        #     }
        #     items.append(recordset)

        #     recordset = {
        #         "Update": {
        #             "TableName": "TuCita247",
        #             "Key": {
        #                 "PKID": {"S": 'BUS#' + businessId}, 
        #                 "SKID": {"S": 'LOC#' + locationId}, 
        #             },
        #             "UpdateExpression": "SET PEOPLE_CHECK_IN = PEOPLE_CHECK_IN - :increment",
        #             "ExpressionAttributeValues": { 
        #                 ":increment": {"N": str(appo['Qty'])}
        #             },
        #             "ConditionExpression": "attribute_exists(PKID) AND attribute_exists(SKID)",
        #             "ReturnValuesOnConditionCheckFailure": "ALL_OLD" 
        #         }
        #     }
        #     items.append(recordset)

        #     if citaTime != '':
        #         if existe == 1:
        #             recordset = {
        #                 "Update":{
        #                     "TableName": "TuCita247",
        #                     "Key":{
        #                         "PKID": {"S": 'LOC#' + locationId + '#PRO#' + providerId + '#DT#' + dateAvg[0:7]},
        #                         "SKID": {"S": 'DT#' + dateAvg}
        #                     },
        #                     "UpdateExpression": "SET TIME_APPO = TIME_APPO + :citaTime, QTY_APPOS = QTY_APPOS + :qty",
        #                     "ExpressionAttributeValues": { 
        #                         ":citaTime": {"N": str(citaTime)},
        #                         ":qty": {"N": str(1)}
        #                     },
        #                     "ConditionExpression": "attribute_exists(PKID) AND attribute_exists(SKID)",
        #                     "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
        #                 }
        #             }
        #         else:
        #             recordset = {
        #                 "Put": {
        #                     "TableName": "TuCita247",
        #                     "Item":{
        #                         "PKID": {"S": 'LOC#' + locationId + '#PRO#' + providerId + '#DT#'+ dateAvg[0:7]},
        #                         "SKID": {"S": 'DT#'+ dateAvg},
        #                         "GSI1PK": {"S": 'BUS#' + businessId},
        #                         "GSI1SK": {"S": dateAvg + '#LOC#' + locationId + '#PRO#' + providerId},
        #                         "GSI2PK": {"S": 'BUS#' + businessId + '#LOC#' + locationId},
        #                         "GSI2SK": {"S": dateAvg + '#PRO#' + providerId},
        #                         "GSI3PK": {"S": 'BUS#' + businessId + '#LOC#' + locationId + '#PRO#' + providerId},
        #                         "GSI3SK": {"S": dateAvg},
        #                         "TIME_APPO": {"N": str(citaTime)},
        #                         "QTY_APPOS": {"N": str(1)}
        #                     },
        #                     "ConditionExpression": "attribute_not_exists(PKID) AND attribute_not_exists(SKID)",
        #                     "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
        #                 }
        #             }

        #         items.append(recordset)

        #     tranAppo = dynamodb.transact_write_items(
        #         TransactItems = items
        #     )

        logger.info(tranAppo)
        statusCode = 200
        body = json.dumps({'Message': 'Appointment updated successfully', 'Code': 200})

        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Message': 'Error on update appointment', 'Code': 500})
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