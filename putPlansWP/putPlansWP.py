import sys
import logging
import requests
import json

import boto3
import botocore.exceptions
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
from dynamodb_json import json_util as json_dynamodb

import datetime
import dateutil.tz
from datetime import timezone
from woocommerce import API

import os

REGION = 'us-east-1'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb', region_name=REGION)
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def findProduct(prodId, prods):
    books = 0
    for x in prods:
        if x['ProductoId'] == prodId:
            return x['Bookings']
    return books
    
def lambda_handler(event, context):

    try:
        country_date = dateutil.tz.gettz('America/Puerto_Rico')
        today = datetime.datetime.now(tz=country_date)
        # dateOpe = today.strftime("%Y-%m-%d")
        dateOpe ='2020-09-18'
        
        # url = 'https://tucita247.com/wp-json/wc/v1/subscriptions'
        # params = dict(
        #     after=dateOpe+'T00:00:00', 
        #     before=dateOpe+'T23:59:59',
        #     order='asc',
        #     consumer_key='ck_3fcd8bc23ab2aa9b5cb27f3ff68c798a072b9662',
        #     consumer_secret='cs_d3892fb3ca1c1f0345a71bf7fd702fedaa385fc9'
        # )
        # subscriptions = requests.get(url=url, params=params)
        
        # urlProds = 'https://tucita247.com/wp-json/wc/v2/products/'
        # params = dict(
        #     consumer_key='ck_3fcd8bc23ab2aa9b5cb27f3ff68c798a072b9662',
        #     consumer_secret='cs_d3892fb3ca1c1f0345a71bf7fd702fedaa385fc9'
        # )
        # products = requests.get(url=urlProds, params=paramsProds)
        # record = []
        # for prod in products.json():
        #     attr = prod['attributes']
        #     bookings = 0
        #     for y in attr:
        #         if y['name'] == 'bookings':
        #             bookings = y['options'][0]
                    
        #     recordset = {
        #         'ProductoId': prod['id'],
        #         'Bookings': bookings 
        #     }
        #     record.append(recordset)
            
        # table = dynamodb.Table('TuCita247')
        # for data in subscriptions.json():
        #     customerId = data['customer_id']
        #     urlCustomer = 'https://tucita247.com/wp-json/wc/v1/customers/'+str(customerId)
        #     paramsCustomer = dict (
        #         consumer_key='ck_3fcd8bc23ab2aa9b5cb27f3ff68c798a072b9662',
        #         consumer_secret='cs_d3892fb3ca1c1f0345a71bf7fd702fedaa385fc9'
        #         )
        #     customer = requests.get(url=urlCustomer, params=paramsCustomer)
        #     if customer != '':
        #         businessId = customer.json()['username']
        #         orderId = data['id'] 
        #         status = data['status'] 
        #         nextPayment = data['next_payment_date'][0:10]
        #         item = data['line_items']
        #         for prod in item:
        #             productoName = prod['name'].upper()
        #             productId = prod['product_id']
                
        #         books = findProduct(productId, record)
        #         if status == 'active':
        #             e = {'#o': 'ORDER'}
        #             response = table.update_item(
        #                 Key={
        #                     'PKID': 'BUS#' + businessId,
        #                     'SKID': 'PLAN'
        #                 },
        #                 UpdateExpression="SET GSI1PK = :nextPayment, NAME = :name, APPOINTMENTS = :appos, DUE_DATE = :nextPayment, #o = :order",
        #                 ExpressionAttributeNames=e,
        #                 ExpressionAttributeValues={
        #                     ':nextPayment': nextPayment,
        #                     ':name': productoName.upper(),
        #                     ':appos': int(books),
        #                     ':order': orderId
        #                 }
        #             )
        #         else:
        #             e = {'#o': 'ORDER', '#s': 'STATUS'}
        #             response = table.update_item(
        #                 Key={
        #                     'PKID': 'BUS#' + businessId,
        #                     'SKID': 'PLAN'
        #                 },
        #                 UpdateExpression="SET GSI1PK = :nextPayment, NAME = :name, APPOINTMENTS = :appos, DUE_DATE = :nextPayment, #o = :order, #s :status",
        #                 ExpressionAttributeNames=e,
        #                 ExpressionAttributeValues={
        #                     ':nextPayment': nextPayment,
        #                     ':name': productoName.upper(),
        #                     ':appos': int(books),
        #                     ':order': orderId,
        #                     ':status': 0
        #                 }
        #             )
                
        statusCode = 200
        body = json.dumps({'Message': 'Carga de datos exitosa', 'Code': 200})
    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message': 'Error on request try again ' + str(e), 'Code': 500})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : "*"
        },
        'body' : body
    }
    return response