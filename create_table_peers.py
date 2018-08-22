#!/usr/bin/env python3
"""
Create a dynamoDB table
"""

from __future__ import print_function # Python 2/3 compatibility
import boto3

dynamodb = boto3.resource('dynamodb')

table = dynamodb.create_table(
    TableName='peers',
    KeySchema=[
        {
            'AttributeName': 'peerId',
            'KeyType': 'HASH'  #Partition key
        },
        {
            'AttributeName': 'lastName',
            'KeyType': 'RANGE'  #Sort key
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'peerId',
            'AttributeType': 'N'
        },
        {
            'AttributeName': 'lastName',
            'AttributeType': 'S'
        }
    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 10,
        'WriteCapacityUnits': 10
    }
)

print("Table status:", table.table_status)