import sys
import logging
import json

import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr

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
            response = cloudsearch.search(
                query='+'+queryStd+'*',
                queryParser='simple',
                sort='tipo asc, _score desc, name_esp asc, name_eng asc',
                highlight='{"name_esp":{"format":"html", "max_phrases": 4,"pre_tag": "<strong>","post_tag": "</strong>"}, "name_eng":{"format":"html", "max_phrases": 4,"pre_tag": "<strong>","post_tag": "</strong>"}, "name":{"format":"html", "max_phrases": 4,"pre_tag": "<strong>","post_tag": "</strong>"} }',
                size=10
            )
                
        statusCode = 200
        body = json.dumps(response)
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