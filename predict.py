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

dynamodb = boto3.client('dynamodb')
ml = boto3.client('machinelearning')

def predict(applicationNumber):
    try:
        applicationFound = dynamodb.query(
            TableName='applications',
            IndexName='applicationNumber-index',
            KeyConditionExpression='applicationNumber = :applicationNumber',
            ExpressionAttributeValues={
                ':applicationNumber' : {
                    "S":applicationNumber
                }
            }
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        item = applicationFound['Items']
        print("GetItem succeeded:")
        print(json.dumps(item, indent=4, cls=DecimalEncoder))

    record_to_predict = {
        "Appln_no"                  : applicationNumber,
        "age"                       : applicationFound['Items'][0]['details']['M']['age']['N'],
        "job"                       : applicationFound['Items'][0]['details']['M']['job']['S'],
        "exp"                       : applicationFound['Items'][0]['details']['M']['exp']['N'],
        "employed"                  : applicationFound['Items'][0]['details']['M']['employed']['N'],
        "education"                 : applicationFound['Items'][0]['details']['M']['education']['S'],
        "marital"                   : applicationFound['Items'][0]['details']['M']['marital']['S'],
        "no_of_dependents"          : applicationFound['Items'][0]['details']['M']['no_of_dependents']['N'],
        "health_Insured"            : applicationFound['Items'][0]['details']['M']['health_Insured']['N'],
        "chronic_illness"           : applicationFound['Items'][0]['details']['M']['chronic_illness']['N'],
        "household_income"          : applicationFound['Items'][0]['details']['M']['household_income']['N'],
        "debt_credit_ratio_perc"    : applicationFound['Items'][0]['details']['M']['debt_credit_ratio_perc']['N'],
        "loan"                      : applicationFound['Items'][0]['details']['M']['loan']['S'],
        "loan_type"                 : applicationFound['Items'][0]['details']['M']['loan_type']['S'],
        "default"                   : applicationFound['Items'][0]['details']['M']['default']['S'],
        "recnt_home_improv"         : applicationFound['Items'][0]['details']['M']['recnt_home_improv']['N'],
        "no_of_props"               : applicationFound['Items'][0]['details']['M']['no_of_props']['N'],
        "missed_pmts_this_yr"       : applicationFound['Items'][0]['details']['M']['missed_pmts_this_yr']['N'],
        "recent_crd_approval"       : applicationFound['Items'][0]['details']['M']['recent_crd_approval']['N'],
        "no_of_accounts"            : applicationFound['Items'][0]['details']['M']['no_of_accounts']['N'],
        "no_risk_invmts"            : applicationFound['Items'][0]['details']['M']['no_risk_invmts']['N'],
        "positive_invtmts"          : applicationFound['Items'][0]['details']['M']['positive_invtmts']['N'],
        "recent_fraud_claims"       : applicationFound['Items'][0]['details']['M']['recent_fraud_claims']['N'],
        "criminal_hist"             : applicationFound['Items'][0]['details']['M']['criminal_hist']['N'],
        "contact"                   : applicationFound['Items'][0]['details']['M']['contact']['S'],
    }

    predictionResponse = ml.predict(
        MLModelId = 'ml-eJyNhR6ftOb',
        Record = record_to_predict,
        PredictEndpoint = 'https://realtime.machinelearning.us-east-1.amazonaws.com'
    )

    return predictionResponse 
