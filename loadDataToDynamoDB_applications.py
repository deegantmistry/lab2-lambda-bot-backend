from __future__ import print_function # Python 2/3 compatibility
import boto3
import json
import decimal
import random
from botocore.exceptions import ClientError

def parseExport(application, exportToDynamoDBmapping):
    details = {}
    for field in application['values']:
        if ('value' in field):
            if (field['title'] in exportToDynamoDBmapping['fields']) and ('value' in field):
                details[exportToDynamoDBmapping['fields'][field['title']]] = field['value']
            else:
                details[field['title']] = field['value']
        if not 'age' in details:
            details['age'] = random.randint(21,75)
        if not 'job' in details:
            details['job'] = "management"
        if not 'exp' in details:
            details['exp'] = details['age'] - 20
        if not 'employed' in details:
            details['employed'] = random.randint(0,1)
        if not 'education' in details:
            details['education'] = 'basic.4y'
        if not 'marital' in details:
            details['marital'] = 'married'
        if not 'no_of_dependents' in details:
            details['no_of_dependents'] = 2
        if not 'health_Insured' in details:
            details['health_Insured'] = random.randint(0,1)
        if not 'chronic_illness' in details:
            details['chronic_illness'] = 0
        if not 'household_income' in details:
            details['household_income'] = 100000
        if not 'debt_credit_ratio_perc' in details:
            details['debt_credit_ratio_perc'] = random.randint(30,130)
        if not 'loan' in details:
            details['loan'] = 'yes'
        if not 'loan_type' in details:
            details['loan_type'] = "jumbo"
        if not 'default' in details:
            details['default'] = 'no'
        if not 'recnt_home_improv' in details:
            details['recnt_home_improv'] = 1
        if not 'no_of_props' in details:
            details['no_of_props'] = 2
        if not 'missed_pmts_this_yr' in details:
            details['missed_pmts_this_yr'] = 1
        if not 'recent_crd_approval' in details:
            details['recent_crd_approval'] = 0
        if not 'no_of_accounts' in details:
            details['no_of_accounts'] = random.randint(1,4)
        if not 'no_risk_invmts' in details:
            details['no_risk_invmts'] = random.randint(0,3)
        if not 'positive_invtmts' in details:
            details['positive_invtmts'] = random.randint(0,3)
        if not 'recent_fraud_claims' in details:
            details['recent_fraud_claims'] = random.randint(0,1)
        if not 'criminal_hist' in details:
            details['criminal_hist'] = 0
        if not 'dmv_bad_rec' in details:
            details['dmv_bad_rec'] = random.randint(0,7)
        

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