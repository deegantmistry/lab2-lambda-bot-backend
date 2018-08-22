from __future__ import print_function # Python 2/3 compatibility
import boto3
import json
import decimal
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('applications')

def getDetails(applicationNumber, queryKey):
    try:
        response = table.get_item(
            Key={
                'applicationNumber': applicationNumber,
            }
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        item = response['Item']
        print("GetItem succeeded:")
        print(json.dumps(item, indent=4, cls=DecimalEncoder))
        if queryKey == 'pullUpEverything':
            return item
        elif queryKey in item:
            return item[queryKey]
        elif ('details' in item) and (queryKey in item['details']):
            return item['details'][queryKey]
        else:
            return None