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

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
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
    stage = event['headers']
    if stage['origin'] != "http://localhost:4200":
        cors = os.environ['prodCors']
    else:
        cors = os.environ['devCors']
        
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

        businessId = event['pathParameters']['businessId']
        
        table = dynamodb.Table('TuCita247')
        response = table.update_item(
            Key={
                'PKID': 'BUS#' + businessId,
                'SKID': 'METADATA'
            },
            UpdateExpression="SET IMGLINK = :path",
            ExpressionAttributeValues= {':path': businessId+'/img/link/'+businessId+'.'+ext },
            ReturnValues="UPDATED_NEW"
        )
        
        statusCode = 200
        body = json.dumps({'Message': 'Image updated successfully'})
                
        if statusCode == '':
            statusCode = 500
            body = json.dumps({'Message': 'Error on update the image'})
        else: 
            if File != '':
                logger.info('SUCCESS: Add image into the bucket')
                if put_object('tucita247', businessId+'/img/link/'+businessId+'.'+ext, base64.b64decode(File), 'image/'+tipo):
                    logger.info('Image Added')
                else:
                    statusCode = 500
                    body = json.dumps({'Message': 'Error on edit the image'})
    except Exception as e:
        statusCode = 500
        body = json.dumps({'Message': 'Error on request try again'})

    response = {
        'statusCode' : statusCode,
        'headers' : {
            "content-type" : "application/json",
            "access-control-allow-origin" : cors
        },
        'body' : body
    }
    return response  