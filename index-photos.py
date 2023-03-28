import json
import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

REGION = 'us-east-1'
HOST = 'search-photos-opensearch-e6smzo4lfrm6i6f7yf55zki7xi.us-east-1.es.amazonaws.com'
INDEX = 'photos'

def lambda_handler(event, context):
    # TODO implement
    
    bucket = event['Records'][0]['s3']['bucket']['name']
    photo = event['Records'][0]['s3']['object']['key'].replace('+', ' ')
    timestamp = event['Records'][0]['eventTime']
    
    session = boto3.Session()
    client = session.client('rekognition')

    response = client.detect_labels(Image={'S3Object':{'Bucket':bucket,'Name':photo}},
    MaxLabels=5,
    # Uncomment to use image properties and filtration settings
    #Features=["GENERAL_LABELS", "IMAGE_PROPERTIES"],
    #Settings={"GeneralLabels": {"LabelInclusionFilters":["Cat"]},
    # "ImageProperties": {"MaxDominantColors":10}}
    )
    
    #print(response)
    
    obj = {
        "objectKey": photo,
        "bucket": bucket,
        "createdTimestamp": timestamp,
        "labels": []
    }
    
    for label in response['Labels']:
        obj['labels'].append(label['Name'])
        
    print(obj['labels'])
        
    print("creating client")
    client = OpenSearch(hosts=[{
        'host': HOST,
        'port': 443
        }],
        http_auth=get_awsauth(REGION, 'es'),
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection)
        
    print("client created")
    print("adding index")
    
        
    index_name = INDEX
    index_body = {
        'settings': {
            'index': {
                'number_of_shards': 1
            }
        }
    }
    
    # response = client.indices.create(index_name, body=index_body)
    client.index(index=index_name, body=obj)
    print("index created")
    
    return {
        'statusCode': 200,
        'body': json.dumps(obj)
    }
    
def get_awsauth(region, service):
    cred = boto3.Session().get_credentials()
    return AWS4Auth(cred.access_key,
                    cred.secret_key,
                    region,
                    service,
                    session_token=cred.token)
