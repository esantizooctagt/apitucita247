import sys
import logging
import json

import datetime
import dateutil.tz
from datetime import timezone

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

cloudsearch = boto3.client('cloudsearchdomain', endpoint_url="https://search-tucita247-djl3mvkaapbmo5zjxat7pcnepu.us-east-1.cloudsearch.amazonaws.com")
logger.info("SUCCESS: Connection to CloudSearch")

def lambda_handler(event, context):
    try:
        queryStd = event['pathParameters']['search']
        city = event['pathParameters']['city']
        sector = event['pathParameters']['sector']
        when = event['pathParameters']['dateOpe']

        if sector != '_' and city != '_':
            response = cloudsearch.search(
                query="(and (phrase field='name_esp' '" + queryStd + "') (phrase field='city' '" + city + "') (phrase field='sector' '" + sector + "'))",
                queryParser='structured',
                sort='tipo asc, _score desc, name_esp asc, name_eng asc',
                highlight='{"name_esp":{"format":"html", "max_phrases": 4,"pre_tag": "<strong>","post_tag": "</strong>"}, "name_eng":{"format":"html", "max_phrases": 4,"pre_tag": "<strong>","post_tag": "</strong>"}, "name":{"format":"html", "max_phrases": 4,"pre_tag": "<strong>","post_tag": "</strong>"} }',
                size=10
            )
        if city != '_' and sector == '_':
            response = cloudsearch.search(
                query="(and (phrase field='name_esp' '" + queryStd + "') (phrase field='city' '" + city + "'))",
                queryParser='structured',
                sort='tipo asc, _score desc, name_esp asc, name_eng asc',
                highlight='{"name_esp":{"format":"html", "max_phrases": 4,"pre_tag": "<strong>","post_tag": "</strong>"}, "name_eng":{"format":"html", "max_phrases": 4,"pre_tag": "<strong>","post_tag": "</strong>"}, "name":{"format":"html", "max_phrases": 4,"pre_tag": "<strong>","post_tag": "</strong>"} }',
                size=10
            )
        if city == '_' and sector == '_':
            words = queryStd.split(' ')
            data = ''
            for word in words:
                data = data + word + '* '
            
            response = cloudsearch.search(
                query=data,
                queryParser='simple',
                sort='tipo asc, _score desc, name_esp asc, name_eng asc',
                highlight='{"name_esp":{"format":"html", "max_phrases": 4,"pre_tag": "<strong>","post_tag": "</strong>"}, "name_eng":{"format":"html", "max_phrases": 4,"pre_tag": "<strong>","post_tag": "</strong>"}, "name":{"format":"html", "max_phrases": 4,"pre_tag": "<strong>","post_tag": "</strong>"} }',
                size=10
            )
        
        result = []
        opeHours = ''
        if when != '_':
            for item in response['hits']['hit']:
                dataWhen = datetime.datetime.strptime(when, '%Y-%m-%d')
                dayName = dataWhen.strftime("%A")[0:3].upper()
                business = dynamodb.query(
                    TableName="TuCita247",
                    ReturnConsumedCapacity='TOTAL',
                    KeyConditionExpression='PKID = :key AND SKID = :metadata',
                    ExpressionAttributeValues={
                        ':key': {'S': item['id']},
                        ':metadata': {'S': 'METADATA'}
                    }
                )
                valid = 0
                for res in json_dynamodb.loads(business['Items']):
                    opeHours = json.loads(res['OPERATIONHOURS'])
                    valid = 1 if dayName in opeHours else 0
                if valid == 1:
                    result.append(item)
        else:
            result = response['hits']['hit']

        statusCode = 200
        body = json.dumps(result)
    except botocore.exceptions.EndpointConnectionError as e:
        statusCode = 500
        body = json.dumps({'Message':'Error on request try again ' + str(e)})
    except:
        statusCode = 500
        body = json.dumps({'Message':'Error on request try again'})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : "*"
        },
        'body' : body
    }
    return response