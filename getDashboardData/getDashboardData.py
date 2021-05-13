import json
import boto3
import time
import logging
import os

# QUERY = "SELECT * FROM tucita247.citas WHERE name like '%Mark%'"
DATABASE = 'tucita247'
OUTPUT = os.environ['bucket']

logger = logging.getLogger()
logger.setLevel(logging.INFO)

client = boto3.client('athena')

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
    businessId = event['pathParameters']['businessId']
    dateIni = event['pathParameters']['dateIni']
    dateFin = event['pathParameters']['dateFin']

    query_01 = "SELECT service, location, count(*) as citas FROM citas WHERE businessid = '"+businessId+"' and date_ope BETWEEN TIMESTAMP '"+dateIni+"' and TIMESTAMP '" + dateFin + "' GROUP BY service, location"
    query_02 = "SELECT ct.name, c.location, count(c.citaid) as citas FROM citas c inner join citastype ct on c.type = ct.pk_id  WHERE businessid = '"+businessId+"' and date_ope BETWEEN TIMESTAMP '"+dateIni+"' and TIMESTAMP '" + dateFin + "' GROUP BY ct.name, c.location"
    query_03 = "SELECT SUBSTRING(CAST(date_ope AS VARCHAR(25)),1,10) as dateOpe, location, count(citaid) as citas FROM citas WHERE businessid = '"+businessId+"' and date_ope BETWEEN TIMESTAMP '"+dateIni+"' and TIMESTAMP '" + dateFin + "' GROUP BY SUBSTRING(CAST(date_ope AS VARCHAR(25)),1,10), location"
    query_04 = "SELECT SUBSTRING(CAST(date_ope AS VARCHAR(25)),1,10) as dateOpe, location, service, count(citaid) as citas FROM citas WHERE businessid = '"+businessId+"' and date_ope BETWEEN TIMESTAMP '"+dateIni+"' and TIMESTAMP '" + dateFin + "' GROUP BY SUBSTRING(CAST(date_ope AS VARCHAR(25)),1,10), location, service"

    
    result01 = []
    result02 = []
    result03 = []
    result04 = []
    result01 = get_result(query_01)
    result02 = get_result(query_02)
    result03 = get_result(query_03)
    result04 = get_result(query_04)
    body = json.dumps({'Query01': result01, 'Query02': result02, 'Query03': result03, 'Query04': result04, 'Code': 200})
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