from __future__ import print_function # Python 2/3 compatibility
import boto3
import json
import decimal

dynamodb = boto3.resource('dynamodb')

table = dynamodb.Table('peers')

with open("./peers.json") as json_file:
    peers = json.load(json_file, parse_float = decimal.Decimal)
    for peer in peers:
        peerId = peer['peerId']
        lastName = peer['lastName']
        details = peer['details']

        print("Adding peer:", peerId, lastName)

        table.put_item(
           Item={
               'peerId': peerId,
               'lastName': lastName,
               'details': details
            }
        )