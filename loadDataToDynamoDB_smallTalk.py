from __future__ import print_function # Python 2/3 compatibility
import boto3
import json
import decimal

dynamodb = boto3.resource('dynamodb')

table = dynamodb.Table('smallTalk')

with open("./smallTalk.json") as json_file:
    smallTalks = json.load(json_file, parse_float = decimal.Decimal)
    for smallTalk in smallTalks:
        question = smallTalk['question']
        answer = smallTalk['answer']

        print("Adding smallTalk:", question, answer)

        table.put_item(
           Item={
               'question': question,
               'answer': answer,
            }
        )