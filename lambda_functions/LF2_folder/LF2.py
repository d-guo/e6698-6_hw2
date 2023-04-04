import base64
import boto3
import inflection
import json
import random
import string

from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth


REGION = 'us-east-1'
HOST = 'search-opensearch-f1emqziclraj-saetzu3aldcg2sxhmitzblysju.us-east-1.es.amazonaws.com'
INDEX = 'photos'


s3 = boto3.resource('s3')
lex = boto3.client('lexv2-runtime')

def get_awsauth(region, service):
    cred = boto3.Session().get_credentials()
    return AWS4Auth(
        cred.access_key,
        cred.secret_key,
        region,
        service,
        session_token=cred.token
    )

os = OpenSearch(
    hosts=[
        {
            'host': HOST,
            'port': 443
        }
    ],
    http_auth=get_awsauth(REGION, 'es'),
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)


def lambda_handler(event, context):
    try:
        print("query start")
        print(event)
        query = event['queryStringParameters']['query']
        print("query done")
        
        res = lex.recognize_text(
            botId='USQ2IBYLZY',
            botAliasId='VBGHSU8RK0',
            localeId='en_US',
            sessionId=''.join(random.choices(string.ascii_letters + string.digits, k=8)),
            text=query
        )
        print(res)
        labels = [inflection.singularize(kw['value']['interpretedValue']).lower() for kw in res['sessionState']['intent']['slots'].values() if kw is not None]
        print(labels)
        results = [s3.Bucket(photo['bucket']).Object(key=photo['objectKey']).get()['Body'].read().decode().split(',')[1] for photo in search_photos(labels)]
        
        status_code = 200
        
    except Exception as e:
        response_body = str(e)
        print(e)
        status_code = 500
        results = {}

    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Headers': '*',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': '*',
        },
        'body': json.dumps(results)
    }

def search_photos(labels):
    if len(labels) == 0:
        return []
    
    query = {
        'query': {
            'bool': {           
                'should': [
                    {'match': {'labels': inflection.singularize(label.replace(" ", "")).lower()}} for label in labels
                ]
            }
        }
    }
    
    res = os.search(index=INDEX, body=query)
    hits = res['hits']['hits']
    
    return [hit['_source'] for hit in hits]