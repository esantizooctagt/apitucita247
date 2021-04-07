import sys
import logging
import json
import csv

import base64

import datetime
import dateutil.tz

import boto3
import botocore.exceptions
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
from dynamodb_json import json_util as json_dynamodb

import os

REGION = 'us-east-1'
BUCKET = os.environ['bucket']

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')

dynamodb = boto3.client('dynamodb', region_name=REGION)
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def findCategory(categoryId, cats):
    for item in cats:
        if item['CategoryId'] == categoryId:
            return item['NameEsp'], item['NameEng']
    return '',''

def put_object(bucket_name, object_name, file):
    # Insert the object
    try:
        s3.put_object(Bucket=bucket_name, Key=object_name, Body=file) 
    except ClientError as e:
        logger.info('Error Inserted')
        logging.error(e)
        return False
    return True

def lambda_handler(event, context):
    try:
        today = datetime.datetime.now()
        year = today.strftime("%Y")
        month = today.strftime("%m").zfill(2)
        day = today.strftime("%d").zfill(2)

        categories = dynamodb.query(
            TableName="TuCita247",
            IndexName="TuCita247_Index",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='GSI1PK = :key01',
            ExpressionAttributeValues={
                ':key01': {'S': 'CAT#'}
            }
        )
        categos = []
        for category in json_dynamodb.loads(categories['Items']):
            cat = {
                'CategoryId': (category['PKID'] + '#' + category['SKID']) if category['SKID'] != category['PKID'] else category['PKID'],
                'NameEsp': category['NAME_ESP'],
                'NameEng': category['NAME_ENG']  
            }
            categos.append(cat)

        business = dynamodb.query(
            TableName="TuCita247",
            IndexName="TuCita247_Index11",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='GSI11PK = :key01 and begins_with(GSI11SK , :key02)',
            ExpressionAttributeValues={
                ':key01': {'S': 'DT#'+year+'-'+month+'-'+day},
                ':key02': {'S': 'BUS#'}
            }
        )
        busArr = []
        for bus in json_dynamodb.loads(business['Items']):
            businessId = bus['PKID']
            busData = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :key01 and SKID = :key02',
                ExpressionAttributeValues={
                    ':key01': {'S': businessId},
                    ':key02': {'S': 'METADATA'}
                }
            )
            for bMetadata in json_dynamodb.loads(busData['Items']):
                busCategories = dynamodb.query(
                    TableName="TuCita247",
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='PKID = :key01 and begins_with(SKID , :key02)',
                    ExpressionAttributeValues={
                        ':key01': {'S': businessId},
                        ':key02': {'S': 'CAT#'}
                    }
                )
                catEng01 = ''
                catEng02 = ''
                catEng03 = ''
                catEng04 = ''
                catEng05 = ''
                catEsp01 = ''
                catEsp02 = ''
                catEsp03 = ''
                catEsp04 = ''
                catEsp05 = ''
                count = 1
                for cat in json_dynamodb.loads(busCategories['Items']):
                    if count == 1:
                        catEsp01, catEng01 = findCategory(cat['SKID'], categos)
                    if count == 2:
                        catEsp02, catEng02 = findCategory(cat['SKID'], categos)
                    if count == 3:
                        catEsp03, catEng03 = findCategory(cat['SKID'], categos)
                    if count == 4:
                        catEsp04, catEng04 = findCategory(cat['SKID'], categos)
                    if count == 5:
                        catEsp05, catEng05 = findCategory(cat['SKID'], categos)
                    count = count + 1
                busRecord = {
                    'BusinessId': bMetadata['PKID'].replace('BUS#',''),
                    'Name': bMetadata['NAME'],
                    'Address': bMetadata['ADDRESS'],
                    'Plan': bMetadata['GSI2PK'].replace('PLAN#',''),
                    'Category_Esp01': catEsp01,
                    'Category_Esp02': catEsp02,
                    'Category_Esp03': catEsp03,
                    'Category_Esp04': catEsp04,
                    'Category_Esp05': catEsp05,
                    'Category_Eng01': catEng01,
                    'Category_Eng02': catEng02,
                    'Category_Eng03': catEng03,
                    'Category_Eng04': catEng04,
                    'Category_Eng05': catEng05,
                    'Created_Date': float(bMetadata['CREATED_DATE'].replace('-','')) if 'CREATED_DATE' in bMetadata else 0
                }
                busArr.append(busRecord)
                
        with open("/tmp/data.csv", "w") as file:
            csv_file = csv.writer(file)
            # csv_file.writerow(['BusinessId', 'Name', 'Address', 'Plan', 'Category_Esp01', 'Category_Esp02', 'Category_Esp03', 'Category_Esp04', 'Category_Esp05', 'Category_Eng01', 'Category_Eng02', 'Category_Eng03', 'Category_Eng04', 'Category_Eng05', 'Created_Date'])
            for item in busArr:
                csv_file.writerow([item.get('BusinessId'), item.get('Name'), item.get('Address'), item.get('Plan'), item.get('Category_Esp01'), item.get('Category_Esp02'), item.get('Category_Esp03'), item.get('Category_Esp04'), item.get('Category_Esp05'), item.get('Category_Eng01'), item.get('Category_Eng02'), item.get('Category_Eng03'), item.get('Category_Eng04'), item.get('Category_Eng05'), item.get('Created_Date')])
        csv_binary = open("/tmp/data.csv", "rb").read()
        put_object(BUCKET, 'business/year='+year+'/month='+month+'/day='+day+'/data.csv', csv_binary)

        customers = dynamodb.query(
            TableName="TuCita247",
            IndexName="TuCita247_Index11",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='GSI11PK = :key01 and begins_with(GSI11SK , :key02)',
            ExpressionAttributeValues={
                ':key01': {'S': 'DT#'+year+'-'+month+'-'+day},
                ':key02': {'S': 'CUS#'}
            }
        )
        cusArr = []
        for cus in json_dynamodb.loads(customers['Items']):
            customerId = cus['SKID'].replace('CUS#','')
            mobile = cus['PKID'].replace('MOB#','')
            cData = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :key01 and SKID = :key02',
                ExpressionAttributeValues={
                    ':key02': {'S': 'CUS#'+customerId},
                    ':key01': {'S': 'MOB#'+mobile}
                }
            )
            for cusData in json_dynamodb.loads(cData['Items']):
                cusRecord = {
                    'CustomerId': customerId,
                    'Phone': mobile,
                    'Name': cusData['NAME'],
                    'Email': cusData['EMAIL'] if 'EMAIL' in cusData else '',
                    'Disability': cusData['DISABILITY'] if 'DISABILITY' in cusData else '',
                    'Gender': cusData['GENDER'] if 'GENDER' in cusData else '',
                    'DOB': cusData['DOB'] if 'DOB' in cusData else '',
                    'Preferences': cusData['PREFERENCES'],
                    'Created_Date': float(cusData['CREATED_DATE'].replace('-','')) if 'CREATED_DATE' in cusData else 0
                }
                cusArr.append(cusRecord)

        with open("/tmp/data01.csv", "w") as file:
            csv_file = csv.writer(file)
            for item in cusArr:
                csv_file.writerow([item.get('CustomerId'), item.get('Phone'), item.get('Name'), item.get('Email'), item.get('Disability'), item.get('Gender'), item.get('DOB'), item.get('Preferences'), item.get('Created_Date')])
        csv_binary = open("/tmp/data01.csv", "rb").read()
        put_object(BUCKET, 'customers/year='+year+'/month='+month+'/day='+day+'/data01.csv', csv_binary)


        logs = dynamodb.query(
            TableName="TuCita247",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='PKID = :key01',
            ExpressionAttributeValues={
                ':key01': {'S': 'LOG#'+year+'-'+month+'-'+day}
            }
        )
        logArr = []
        for log in json_dynamodb.loads(logs['Items']):
            logRecord = {
                'CitaId': log['SKID'].split('#')[1],
                'Date_Ope': float(log['DATE_APPO'].replace('-','')),
                'Status': log['STATUS']
            }
            logArr.append(logRecord)

        with open("/tmp/data02.csv", "w") as file:
            csv_file = csv.writer(file)
            for item in logArr:
                csv_file.writerow([item.get('CitaId'), item.get('Date_Ope'), item.get('Status')])
        csv_binary = open("/tmp/data02.csv", "rb").read()
        put_object(BUCKET, 'logs/year='+year+'/month='+month+'/day='+day+'/data02.csv', csv_binary)

        appos = dynamodb.query(
            TableName="TuCita247",
            IndexName="TuCita247_Index11",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='GSI11PK = :key01 and begins_with(GSI11SK , :key02)',
            ExpressionAttributeValues={
                ':key01': {'S': 'DT#'+year+'-'+month+'-'+day},
                ':key02': {'S': 'APPO#'}
            }
        )
        appoArr = []
        for appo in json_dynamodb.loads(appos['Items']):
            appoId = appo['PKID'].replace('APPO#','')
            appData = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :key01 and SKID = :key01',
                ExpressionAttributeValues={
                    ':key01': {'S': 'APPO#'+appoId}
                }
            )
            for cita in json_dynamodb.loads(appData['Items']):
                citaRecord = {
                    'CitaId': appoId,
                    'Date_Ope': float(cita['DATE_APPO'].replace('-','')),
                    'Name': cita['NAME'],
                    'Business': cita['BUSINESS_NAME'],
                    'Location': cita['LOCATION_NAME'],
                    'Provider': cita['PROVIDER_NAME'],
                    'Service': cita['SERVICE_NAME'],
                    'CustomerId': cita['GSI2PK'].replace('CUS#',''),
                    'Type': cita['TYPE'],
                    'Phone': cita['PHONE'],
                    'People': int(cita['PEOPLE_QTY']),
                    'Created_Date': float(cita['CREATED_DATE'].replace('-','')) if 'CREATED_DATE' in cusData else 0
                }
                appoArr.append(citaRecord)

        with open("/tmp/data03.csv", "w") as file:
            csv_file = csv.writer(file)
            for item in appoArr:
                csv_file.writerow([item.get('CitaId'), item.get('Date_Ope'), item.get('Name'), item.get('Business'), item.get('Location'), item.get('Provider'), item.get('Service'), item.get('CustomerId'), item.get('Type'), item.get('Phone'), item.get('People'), item.get('Created_Date')])
        csv_binary = open("/tmp/data03.csv", "rb").read()
        put_object(BUCKET, 'citas/year='+year+'/month='+month+'/day='+day+'/data03.csv', csv_binary)

        statusCode = 200
        body = json.dumps({'Message': 'Data successfully', 'Code': 200})

        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Message': 'Error on data', 'Code': 500})
    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message': 'Error on request try again ' + str(e)})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : "*"
        },
        'body' : body
    }
    return response  