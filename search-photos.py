import json
import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
# Define the client to interact with Lex
client = boto3.client('lexv2-runtime')

REGION = 'us-east-1'
HOST = 'search-photos-opensearch-e6smzo4lfrm6i6f7yf55zki7xi.us-east-1.es.amazonaws.com'
INDEX = 'photos'

def lambda_handler(event, context):
    

    msg_from_user = event['queryStringParameters']['q']
    
    response = client.recognize_text(
            botId='2LIPOGTRJT', # MODIFY HERE
            botAliasId='H683YADIW3', # MODIFY HERE
            localeId='en_US',
            sessionId='testuser',
            text=msg_from_user)
            
    print("Response: ", response['interpretations'])
    
    slots = {}
    for interpretation in response['interpretations']:
        
        if interpretation['intent']['name'] == 'SearchIntent':
            slots = interpretation['intent']['slots']
    
    labels = []
    if slots['label_1'] != None:
        labels.append(slots['label_1']['value']['interpretedValue'])
    if slots['label_2'] != None:
        labels.append(slots['label_2']['value']['interpretedValue'])
            
    image_uris = []
    
    for label in labels:
        results = query(label)
        
        image_uris.extend(["https://" + obj['bucket'] + ".s3.amazonaws.com/" + obj['objectKey'] for obj in results])
        
    print("image_uris: ", image_uris)
    image_uris = list(set(image_uris))
    print("image_uris: ", image_uris)
        
    # FOR TESTING
    #results = query("person")
    #print("Results: ", results)
    
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': '*',
            'Access-Control-Allow-Methods': '*'
        },
        'body': json.dumps(image_uris)
    }


def query(term):
    q = {'size': 100, 'query': {'multi_match': {'query': term}}}

    client = OpenSearch(hosts=[{
        'host': HOST,
        'port': 443
    }],
                        http_auth=get_awsauth(REGION, 'es'),
                        use_ssl=True,
                        verify_certs=True,
                        connection_class=RequestsHttpConnection)

    res = client.search(index=INDEX, body=q)
    print("res: ",res)

    hits = res['hits']['hits']
    results = []
    for hit in hits:
        results.append(hit['_source'])

    return results
    
def get_awsauth(region, service):
    cred = boto3.Session().get_credentials()
    return AWS4Auth(cred.access_key,
                    cred.secret_key,
                    region,
                    service,
                    session_token=cred.token)