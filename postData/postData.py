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
        logger.info("categos")
        
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
        logger.info("infor")
        for bus in json_dynamodb.loads(business['Items']):
            businessId = bus['PKID']
            logger.info("entro")
            busData = dynamodb.query(
                TableName="TuCita247",
                ReturnConsumedCapacity='TOTAL',
                KeyConditionExpression='PKID = :key01 and SKID = :key02',
                ExpressionAttributeValues={
                    ':key01': {'S': 'BUS#'+businessId},
                    ':key02': {'S': 'METADATA'}
                }
            )
            for bMetadata in json_dynamodb.loads(busData['Items']):
                logger.info("ingreso detnro metadata")
                busCategories = dynamodb.query(
                    TableName="TuCita247",
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='PKID = :key01 and begins_with(SKID , :key02)',
                    ExpressionAttributeValues={
                        ':key01': {'S': 'BUS#'+businessId},
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
                    logger.info("ingreso categos")
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
                    'Created_Date': bMetadata['CREATED_DATE']
                }
                busArr.append(busRecord)

        tempCsv = open('data.csv')
        csv_file = csv.writer(tempCsv)
        for item in busArr:
            csv_file.writerow(item)

        tempCsv.close()
        put_object(BUCKET, '/business/year='+year+'/month='+month+'/day='+day+'/data.csv', csv_file)

        # customers = dynamodb.query(
        #     TableName="TuCita247",
        #     IndexName="TuCita247_Index11",
        #     ReturnConsumedCapacity='TOTAL',
        #     KeyConditionExpression='GSI11PK = :key01 and begins_with(GSI11SK , :key02)',
        #     ExpressionAttributeValues={
        #         ':key01': {'S': 'DT#'+year+'-'+month+'-'+day},
        #         ':key02': {'S': 'CUS#'}
        #     }
        # )
        # data01 = json_dynamodb.loads(business['Items'])
        # tempCsv01 = open('data01.csv')
        # csv_file01 = csv.writer(tempCsv01)
        # for item in data01:
        #     csv_file01.writerow(item)

        # tempCsv01.close()
        # put_object(bucket, '/customers/year='+year+'/month='+month+'/day='+day+'/data01.csv', csv_file01)

        # appos = dynamodb.query(
        #     TableName="TuCita247",
        #     IndexName="TuCita247_Index11",
        #     ReturnConsumedCapacity='TOTAL',
        #     KeyConditionExpression='GSI11PK = :key01 and begins_with(GSI11SK , :key02)',
        #     ExpressionAttributeValues={
        #         ':key01': {'S': 'DT#'+year+'-'+month+'-'+day},
        #         ':key02': {'S': 'APPO#'}
        #     }
        # )
        # data02 = json_dynamodb.loads(business['Items'])
        # tempCsv02 = open('data02.csv')
        # csv_file02 = csv.writer(tempCsv02)
        # for item in data02:
        #     csv_file02.writerow(item)

        # tempCsv02.close()
        # put_object(bucket, '/citas/year='+year+'/month='+month+'/day='+day+'/data02.csv', csv_file02)
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