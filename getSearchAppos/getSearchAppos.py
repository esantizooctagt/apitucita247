import json
import logging

import time
import boto3

import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr
from dynamodb_json import json_util as json_dynamodb

import os

REGION = 'us-east-1'
DATABASE = 'tucita247'
OUTPUT = os.environ['bucket']

logger = logging.getLogger()
logger.setLevel(logging.INFO)

client = boto3.client('athena')
dynamodb = boto3.client('dynamodb', region_name=REGION)
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def get_result(query_str):
    results = []
    query_id = client.start_query_execution(
        QueryString=query_str,
        QueryExecutionContext={
            'Database': DATABASE
        },
        ResultConfiguration={
            'OutputLocation': OUTPUT,
        }
    )['QueryExecutionId']

    query_status = None
    while query_status == 'QUEUED' or query_status == 'RUNNING' or query_status is None:
        try:
            query_status = client.get_query_execution(QueryExecutionId=query_id)['QueryExecution']['Status']['State']
            if query_status == 'FAILED' or query_status == 'CANCELLED':
                # raise Exception('Athena query with the string "{}" failed or was cancelled'.format(query_str))
                return results
            time.sleep(3)
        except Exception as e:
            return results
            # statusCode = 500
            # body = json.dumps({'Message': 'Error on request try again ' + str(e)})
    
    results_paginator = client.get_paginator('get_query_results')
    results_iter = results_paginator.paginate(
        QueryExecutionId=query_id,
        PaginationConfig={
            'PageSize': 1000
        }
    )
    column_names = None
    for results_page in results_iter:
        for row in results_page['ResultSet']['Rows']:
          column_values = [col.get('VarCharValue', None) for col in row['Data']]
          if not column_names:
              column_names = column_values
          else:
              results.append(dict(zip(column_names, column_values)))
    return results

def lambda_handler(event, context):
    fullName = event['pathParameters']['fullName']
    dateVal = event['pathParameters']['date']
    businessId = event['pathParameters']['businessId']

    query_01 = "SELECT citaid, name, phone, people, date_ope, service, location, customerid, type FROM citas WHERE LOWER(name) LIKE '%" + fullName.lower() + "%' AND date_ope >= TIMESTAMP '"+dateVal+":00.000' AND businessid = '" + businessId + "'" #2021-05-26 12:00
    
    result01 = []
    result01 = get_result(query_01)
    appos = []
    record = {}
    for app in result01:
        record = {
            'AppId': app['citaid'],
            'CustId': app['customerid'],
            'Name': app['name'],
            'Phone': app['phone'],
            'Guests': app['people'],
            'DateOpe': app['date_ope'],
            'Type': app['type'],
            'Service': app['service'],
            'Location': app['location']
        }
        appos.append(record)

    body = json.dumps({'Appos': appos, 'Code': 200})
    statusCode = 200
    
    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : "*"
        },
        'body' : body
    }
    return response