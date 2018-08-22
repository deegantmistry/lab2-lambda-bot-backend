from __future__ import print_function # Python 2/3 compatibility
import boto3
import json
import decimal
from botocore.exceptions import ClientError

def parseExport(application, exportToDynamoDBmapping):
    details = {}
    for field in application['values']:
        if (field['title'] in exportToDynamoDBmapping['fields']) and ('value' in field):
            details[exportToDynamoDBmapping['fields'][field['title']]] = field['value']
    
    return details


def lambda_handler(event,context):
    dynamodb = boto3.resource('dynamodb')
    s3 = boto3.client('s3')
    table = dynamodb.Table('applications')

    with open("./exportMapping.json") as mapping_json_file:
        exportToDynamoDBmapping = json.load(mapping_json_file, parse_float = decimal.Decimal)

    try:
        response = s3.get_object(
            Bucket=event['Records'][0]['s3']['bucket']['name'],
            Key=event['Records'][0]['s3']['object']['key']
        )
        json_file = response['Body']

        application = json.load(json_file, parse_float = decimal.Decimal)
        applicationNumber = application['applicationId']
        status = 'queued'
        details = parseExport(application,exportToDynamoDBmapping)

        print("Adding application:", applicationNumber)

        table.put_item(
            Item={
                'applicationNumber': applicationNumber,
                'status': status,
                'details': details
            }
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        return ('Success')