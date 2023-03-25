import boto3
import inflection
import json

from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth


REGION = 'us-east-1'
HOST = 'search-photos-sowdncarwfcckgivwpirg5s4la.us-east-1.es.amazonaws.com'
INDEX = 'photos'


rekognition = boto3.client('rekognition')
s3 = boto3.client('s3')

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


def get_labels_and_timestamp(bucket, key):
    res_rek = rekognition.detect_labels(
        Image={'S3Object':{'Bucket':bucket,'Name':key}},
        MaxLabels=10,
    )
    labels_rek = [label['Name'] for label in res_rek['Labels']]

    res_s3 = s3.head_object(
        Bucket=bucket,
        Key=key,
    )
    # labels_s3 = res_s3['Metadata']['x-amz-meta-customLabels']
    labels_s3 = []
    
    return labels_rek + labels_s3, res_s3['LastModified']

def lambda_handler(event, context):

    # response = os.indices.create(INDEX, body={})
    # print('\nCreating index:')
    # print(response)

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    labels, timestamp = get_labels_and_timestamp(bucket, key)
    
    document = {
        'objectKey': key,
        'bucket': bucket,
        'createdTimestamp': timestamp,
        'labels': [inflection.singularize(label).lower() for label in labels]
    }
    id = key

    response = os.index(
        index = INDEX,
        body = document,
        id = id,
        refresh = True
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps('Works!')
    }
