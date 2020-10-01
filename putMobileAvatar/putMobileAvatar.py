import sys
import logging
import json

import base64

import boto3
import botocore.exceptions
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
from dynamodb_json import json_util as json_dynamodb

import os

REGION = 'us-east-1'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')

dynamodb = boto3.resource('dynamodb', region_name=REGION)
logger.info("SUCCESS: Connection to DynamoDB succeeded")

def put_object(bucket_name, object_name, file, tipo):
    # Insert the object
    try:
        s3.put_object(Bucket=bucket_name, Key=object_name, Body=file, ContentType=tipo) 
    except ClientError as e:
        logger.info('Error Inserted')
        logging.error(e)
        return False
    return True
    
def lambda_handler(event, context):
    try:
        statusCode = ''
        data = str(base64.b64decode(event['body']))
        data = data.replace(r"\r\n", "")
        resultado = data.split('------')
        File = resultado[1]

        if File.find(';base64,') > 0:
            tipo = File.split(';')[1].split('/')[1]
            if tipo == 'jpeg':
                ext = 'jpg'
            if tipo == 'jpg':
                ext= 'jpg'
            if tipo == 'png':
                ext = 'png'
                
            if File.find(';base64,') > 0:
                File = File[File.find(";base64,")+8:]
        else:
            File = ''
        Img_Path = resultado[2]
        Img_Path = Img_Path[Img_Path.find("Img_Path")+9:]
        customerId = event['pathParameters']['customerId']
        mobile = event['pathParameters']['mobile']
        
        table = dynamodb.Table('TuCita247')
        response = table.update_item(
            Key={
                'PKID': 'MOB#' + mobile,
                'SKID': 'CUS#' + customerId
            },
            UpdateExpression="SET AVATAR = :path",
            ExpressionAttributeValues= {':path': '/mobile/customer/'+customerId+'.'+ext },
            ReturnValues="UPDATED_NEW"
        )
        
        statusCode = 200
        body = json.dumps({'Message': 'Avatar updated successfully'})
                
        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Message': 'Error on update the avatar'})
        else: 
            if File != '':
                logger.info('SUCCESS: Add image into the bucket')
                if put_object('tucita247', 'mobile/customer/'+customerId+'.'+ext, base64.b64decode(File), 'image/'+tipo):
                    logger.info('Image Added')
                else:
                    statusCode = 500
                    body = json.dumps({'Message': 'Error on edit the avatar'})
    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message': 'Error on request try again'})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : "*"
        },
        'body' : body
    }
    return response  