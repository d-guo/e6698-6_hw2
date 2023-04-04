import boto3
import inflection
import json
import base64

from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth


REGION = 'us-east-1'
HOST = 'search-opensearch-f1emqziclraj-saetzu3aldcg2sxhmitzblysju.us-east-1.es.amazonaws.com/'
INDEX = 'photos'


rekognition = boto3.client('rekognition')
s3 = boto3.client('s3')
s3_res = boto3.resource('s3')

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
    encoded_image = s3_res.Bucket(bucket).Object(key=key).get()['Body'].read().decode().split(',')[1]
    image_data = base64.b64decode(encoded_image)
    
    res_rek = rekognition.detect_labels(
        Image={
            'Bytes': bytes(image_data)
        },
        MaxLabels=10,
    )
    
    labels_rek = [label['Name'] for label in res_rek['Labels']]

    res_s3 = s3.head_object(
        Bucket=bucket,
        Key=key,
    )
    
    labels_string = res_s3["Metadata"].get("customlabels", "")
    # print(labels_string)
    labels_s3 = labels_string.replace(" ", "").split(',')
    # print(labels_s3)
    
    labels_rek.extend(label for label in labels_s3 if label not in labels_rek)
    
    return labels_rek, res_s3['LastModified']

def lambda_handler(event, context):
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
