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

dynamodb = boto3.client('dynamodb', region_name=REGION)
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def lambda_handler(event, context):
    stage = ''
    businessId = ''
    body = ''
    cors = ''
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']

    try:
        statusCode = ''
        link = event['pathParameters']['link']
        response = dynamodb.query(
            TableName="TuCita247",
            IndexName="TuCita247_Appos",
            ReturnConsumedCapacity='TOTAL',
            KeyConditionExpression='GSI3PK = :link',
            ExpressionAttributeValues={
                ':link': {'S': 'LINK#' + link}
            },
            Limit =1
        )
        for row in json_dynamodb.loads(response['Items']):            
            businessId = row['PKID'].replace('BUS#','')
            locs = []
            if businessId != '':
                e = {'#s': 'STATUS'}
                f = '#s = :stat'
                locations = dynamodb.query(
                    TableName="TuCita247",
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='PKID = :businessId and begins_with (SKID, :locs) ',
                    ExpressionAttributeNames=e,
                    FilterExpression=f,
                    ExpressionAttributeValues={
                        ':businessId': {'S': 'BUS#' + businessId},
                        ':locs': {'S': 'LOC#'},
                        ':stat' : {'N': '1'}
                    }
                )
                for det in json_dynamodb.loads(locations['Items']):
                    e = {'#s': 'STATUS'}
                    f = '#s = :stat'
                    resprovs = dynamodb.query(
                        TableName="TuCita247",
                        ReturnConsumedCapacity='TOTAL',
                        KeyConditionExpression='PKID = :businessId and begins_with (SKID, :locs) ',
                        ExpressionAttributeNames=e,
                        FilterExpression=f,
                        ExpressionAttributeValues={
                            ':businessId': {'S': 'BUS#' + businessId + '#LOC#' + det['SKID'].replace('LOC#','')},
                            ':locs': {'S': 'PRO#'},
                            ':stat' : {'N': '1'}
                        }
                    )
                    provs = []
                    for resP in json_dynamodb.loads(resprovs['Items']):
                        servicesAv = []
                        resServ = dynamodb.query(
                            TableName="TuCita247",
                            IndexName="TuCita247_Index",
                            ReturnConsumedCapacity='TOTAL',
                            KeyConditionExpression='GSI1PK = :businessId and begins_with (GSI1SK, :locs) ',
                            ExpressionAttributeValues={
                                ':businessId': {'S': 'BUS#' + businessId + '#PRO#' + resP['SKID'].replace('PRO#','')},
                                ':locs': {'S': 'SER#'}
                            }
                        )
                        for item in json_dynamodb.loads(resServ['Items']):
                            e = {'#s': 'STATUS'}
                            f = '#s = :stat'
                            getService = dynamodb.query(
                                TableName="TuCita247",
                                ReturnConsumedCapacity='TOTAL',
                                KeyConditionExpression='PKID = :businessId AND SKID = :service',
                                ExpressionAttributeNames=e,
                                FilterExpression=f,
                                ExpressionAttributeValues={
                                    ':businessId': {'S': 'BUS#'+businessId},
                                    ':service': {'S': item['GSI1SK']},
                                    ':stat' : {'N': '1'}
                                }
                            )
                            serRec = {}
                            for itemSer in json_dynamodb.loads(getService['Items']):
                                serRec = {
                                    'ServiceId': item['GSI1SK'].replace('SER#',''),
                                    'Name': itemSer['NAME'],
                                    'MaxGuests': itemSer['CUSTOMER_PER_BOOKING'] if 'CUSTOMER_PER_BOOKING' in itemSer else 1
                                }
                            if serRec != {}:
                                servicesAv.append(serRec)
                        rec = {
                            'ProviderId': resP['SKID'].replace('PRO#',''),
                            'Name': resP['NAME'],
                            'Services': servicesAv
                        }
                        provs.append(rec)

                    record = {
                        'LocationId': det['SKID'].replace('LOC#',''),
                        'Name': det['NAME'],
                        'Address': det['ADDRESS'],
                        'TimeZone': det['TIME_ZONE'] if 'TIME_ZONE' in det else 'America/Puerto_Rico',
                        'Provs': provs
                    }
                    locs.append(record)

            servs = []
            e = {'#s': 'STATUS'}
            f = '#s = :stat'
            services = dynamodb.query(
                    TableName="TuCita247",
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='PKID = :businessId and begins_with (SKID, :servs) ',
                    ExpressionAttributeNames=e,
                    FilterExpression=f,
                    ExpressionAttributeValues={
                        ':businessId': {'S': 'BUS#' + businessId},
                        ':servs': {'S': 'SER#'},
                        ':stat' : {'N': '1'}
                    }
                )
            for results in json_dynamodb.loads(services['Items']):
                record = {
                    'ServiceId': results['SKID'].replace('SER#',''),
                    'Name': results['NAME'],
                    'MaxGuests': results['CUSTOMER_PER_BOOKING'] if 'CUSTOMER_PER_BOOKING' in results else 1
                }
                servs.append(record)

            recordset = {
                'BusinessId': row['PKID'].replace('BUS#',''),
                'Name': row['NAME'],
                'Imagen': row['IMGBUSINESS'] if 'IMGBUSINESS' in row else '',
                'LongDescrip': row['LONGDESCRIPTION'] if 'LONGDESCRIPTION' in row else '',
                'Phone': row['PHONE'] if 'PHONE' in row else '',
                'ShortDescript': row['SHORTDESCRIPTION'] if 'SHORTDESCRIPTION' in row else '',
                'WebSite': row['WEBSITE'] if 'WEBSITE' in row else '',
                'Instagram': row['INSTAGRAM'] if 'INSTAGRAM' in row else '',
                'Twitter': row['TWITTER'] if 'TWITTER' in row else '',
                'Facebook': row['FACEBOOK'] if 'FACEBOOK' in row else '',
                'Locs': locs,
                'Services': servs
            }
            
            statusCode = 200
            body = json.dumps(recordset)
        
        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Message':'Something goes wrong, try again', 'Code': 500})
    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message':'Error on request try again'+ str(e)})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : cors
        },
        'body' : body
    }
    return response