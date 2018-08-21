from __future__ import print_function # Python 2/3 compatibility
import boto3
import json
import decimal
from botocore.exceptions import ClientError

def lambda_handler(event,context):
    dynamodb = boto3.resource('dynamodb')
    s3 = boto3.client('s3')
    table = dynamodb.Table('applications')

    try:
        response = s3.get_object(
            Bucket=event['Records'][0]['s3']['bucket']['name'],
            Key=event['Records'][0]['s3']['object']['key']
        )
        json_file = response['Body']

        application = json.load(json_file, parse_float = decimal.Decimal)
        applicationNumber = application['applicationNumber']
        status = application['status']
        details = application['details']

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