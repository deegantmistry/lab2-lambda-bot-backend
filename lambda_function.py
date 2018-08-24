import time
import os
import logging
import json
import boto3
import uuid
import smallTalkRead
import applicationsRead
import predict

# Configure logger
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# Establish credentials
session_var = boto3.session.Session()
credentials = session_var.get_credentials()

""" --- Parameters --- """
# DynamoDB tables, read lambda function environment variables or initialize with defaults if they not exist.
DYNAMODB_APPLICATIONS_TABLE = os.getenv('DYNAMODB_PRODUCT_TABLE', default='applications')
DYNAMODB_PEERS_TABLE = os.getenv('DYNAMODB_ORDER_TABLE', default='peers')
DYNAMODB_SMALLTALK_TABLE = os.getenv('DYNAMODB_ORDER_TABLE', default='smallTalk')

# Initialize DynamoDB Client
dynamodb = boto3.client('dynamodb')

##############################################################################################
# Functions

""" --- Helper functions --- """

def rreplace(s, old, new, occurrence):
    li = s.rsplit(old, occurrence)
    return new.join(li)


def parse_int(n):
    try:
        return int(n)
    except ValueError:
        return float('nan')


def convert_string_array_to_string(stringArray):
    stringList = ', '.join(map(str, stringArray))
    stringList = rreplace(stringList, ', ', ' and ', 1)
    return stringList


""" --- Generic functions used to simplify interaction with Amazon Lex --- """

def get_slots(intent_request):
    return intent_request['currentIntent']['slots']


def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            'message': message
        }
    }


def close(session_attributes, fulfillment_state, message):
    response = {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }

    return response


def delegate(session_attributes, slots):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': slots
        }
    }


def build_validation_result(is_valid, violated_slot, message_content):

    if message_content is None:
        return {
            "isValid": is_valid,
            "violatedSlot": violated_slot,
        }

    return {
        'isValid': is_valid,
        'violatedSlot': violated_slot,
        'message': {'contentType': 'PlainText', 'content': message_content}
    }


""" --- Functions that interact with other services (backend functions) --- """


def validate_applicationNumber(applicationNumber):
    """
    Called to validate the applicationNumber.
    """
    logger.debug('Provided applicationNumber: {}'.format(applicationNumber))

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

    if len(applicationFound['Items']) == 0:
        return build_validation_result(False,
        'applicationNumber',
        'Sorry, I could not find application number {} in the database. Could you please try another application number?'.format(applicationNumber))

    return build_validation_result(True, None, None)


def validate_peer(peerFirstName, peerLastName):
    """
    Called to validate the peer name.
    """
    logger.debug('Provided peerFirstName: {}'.format(peerFirstName))
    logger.debug('Provided peerLastName: {}'.format(peerLastName))

    peersFoundWithLastName = dynamodb.query(
        TableName='peers',
        IndexName='lastName-index',
        KeyConditionExpression='lastName = :lastName',
        ExpressionAttributeValues={
            ':lastName' : {
                "S":peerLastName
            }
        }
    )

    if len(peersFoundWithLastName['Items']) == 0:
             return build_validation_result(False,
                'peerLastName',
                'Sorry, I could not find anyone with last name {} in the list of your peers. Could you please tell another last name?'.format(peerLastName))

    elif len(peersFoundWithLastName['Items']) > 0 :
        for peer in peersFoundWithLastName['Items']:
            if peer['details']['M']['firstName']['S'] == peerFirstName:
                return build_validation_result(True, None, None)

        return build_validation_result(False,
            'peerFirstName',
            'Sorry, I could not find anyone with first name {} (whose last name is {}) in the list of your peers. Could you please tell another first name?'.format(peerFirstName,peerLastName))

""" --- Functions that control the bot's behavior (bot intent handler) --- """

def getApplicationInfo(intent_request):
    """
    Called when the user triggers the getApplicationInfo intent.
    """

    source = intent_request['invocationSource']
    slots = get_slots(intent_request)

    applicationNumber = slots['applicationNumber']
    queryKey = slots['queryKey']
    applicationNumberVal = validate_applicationNumber(applicationNumber)
    if not applicationNumberVal['isValid']:
        slots[applicationNumberVal['violatedSlot']] = None

        return elicit_slot(intent_request['sessionAttributes'],
                            intent_request['currentIntent']['name'],
                            slots,
                            applicationNumberVal['violatedSlot'],
                            applicationNumberVal['message'])

        output_session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}
        return delegate(output_session_attributes, get_slots(intent_request))

    queryKeyValue = applicationsRead.getDetails(applicationNumber,queryKey)

    if queryKeyValue is not None:
        return close(intent_request['sessionAttributes'],
                    'Fulfilled',
                    {'contentType': 'PlainText',
                    'content': 'The {} for application number {} is {}.'.format(queryKey,applicationNumber,queryKeyValue)})
    else:
        return close(intent_request['sessionAttributes'],
                    'Fulfilled',
                    {'contentType': 'PlainText',
                    'content': 'Sorry, I could not find {} for application number {}.'.format(queryKey,applicationNumber)})


def i_help(intent_request):
    """
    Called when the user triggers the Help intent.
    """

    #Intent fulfillment
    return close(intent_request['sessionAttributes'],
                 'Fulfilled',
                 {'contentType': 'PlainText',
                  'content': "Hi this is Cool Bot, your personal assistant. " \
                             "- You can ask me any details about an application. " \
                             "Or you could ask me to evaluate ability-to-replay. You could also "\
                             " ask me to send a particular mortgage application for a peer review."\
                             " Anything you need...I'm here to help." })

def showQueryAttributes(intent_request):
    """
    Called when the user triggers the showQueryAttributes intent.
    """

    #Intent fulfillment
    return close(intent_request['sessionAttributes'],
                 'Fulfilled',
                 {'contentType': 'PlainText',
                  'content': "Very well, " \
                             "these are the attributes you can query for a particular application: "  \
                             "- job title, company, income, evaluation status, ability to repay, assets, monthly expenditure, credit history."})

def sendForPeerReview(intent_request):
    """
    Called when the user triggers the peerReview intent
    """

    #Intent fulfillment
    slots = get_slots(intent_request)
    source = intent_request['invocationSource']

    applicationNumber = slots['applicationNumber']
    peer = {}
    peer['firstName'] = slots['peerFirstName'].capitalize()
    peer['lastName'] = slots['peerLastName'].capitalize()

    applicationNumberVal = validate_applicationNumber(applicationNumber)
    if not applicationNumberVal['isValid']:
        slots[applicationNumberVal['violatedSlot']] = None

        return elicit_slot(intent_request['sessionAttributes'],
                            intent_request['currentIntent']['name'],
                            slots,
                            applicationNumberVal['violatedSlot'],
                            applicationNumberVal['message'])

        output_session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}
        return delegate(output_session_attributes, get_slots(intent_request))

    peerVal = validate_peer(peer['firstName'],peer['lastName'])
    if not peerVal['isValid']:
        slots[peerVal['violatedSlot']] = None

        return elicit_slot(intent_request['sessionAttributes'],
                            intent_request['currentIntent']['name'],
                            slots,
                            peerVal['violatedSlot'],
                            peerVal['message'])

        output_session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}
        return delegate(output_session_attributes, get_slots(intent_request))

    application = applicationsRead.getDetails(applicationNumber,'pullUpEverything')

    if ('y' in application['details']) and (application['details']['y'] is not None):
        return close(intent_request['sessionAttributes'],
                    'Fulfilled',
                    {'contentType': 'PlainText',
                    'content': 'Done! I\'ve sent application number {} to your colleague {} for a review.'.format(applicationNumber,peer['firstName'])})
    elif ('y' not in application['details']):
        return close(intent_request['sessionAttributes'],
                    'Fulfilled',
                    {'contentType': 'PlainText',
                    'content': 'Application number {} does not seem to be evaluated for a risk score yet. Are you sure you want to send it to your colleague {} for a review?'.format(applicationNumber,peer['firstName'])})
    else:
        return close(intent_request['sessionAttributes'],
                    'Fulfilled',
                    {'contentType': 'PlainText',
                    'content': 'Sorry, I could not send application {} to {}.'.format(applicationNumber,peer['firstName'])})


def i_smallTalk(intent_request):
    """
    Called when the user triggers the smallTalk intent.
    """

    responseAnswer = smallTalkRead.getAnswer(intent_request['inputTranscript'])
    # print("printing intent_request:")
    # print(intent_request)

    #Intent fulfillment
    return close(intent_request['sessionAttributes'],
                 'Fulfilled',
                 {'contentType': 'PlainText',
                  'content': responseAnswer
                 }
            )

def evaluateAbilityToRepayScore(intent_request):
    """
    Called when the user triggers the evaluateAbilityToRepayScore intent
    """

    #Intent fulfillment
    slots = get_slots(intent_request)

    applicationNumber = slots['applicationNumber']
    applicationNumberVal = validate_applicationNumber(applicationNumber)
    if not applicationNumberVal['isValid']:
        slots[applicationNumberVal['violatedSlot']] = None

        return elicit_slot(intent_request['sessionAttributes'],
                            intent_request['currentIntent']['name'],
                            slots,
                            applicationNumberVal['violatedSlot'],
                            applicationNumberVal['message'])

        output_session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}
        return delegate(output_session_attributes, get_slots(intent_request))

    predictionResponse = predict.predict(applicationNumber)

    return close(intent_request['sessionAttributes'],
        'Fulfilled',
        {'contentType': 'PlainText',
        'content': 'Done! I\'ve analyzed application {} and predicted the ability to repay score of {}.'.format(applicationNumber,predictionResponse['Prediction']['predictedValue'],predictionResponse['Prediction']['details']['PredictiveModelType'],predictionResponse['Prediction']['details']['Algorithm'])})


""" --- Dispatch intents --- """

def dispatch(intent_request):
    """
    Called when the user specifies an intent for this bot.
    """

    logger.debug('dispatch userId={}, intentName={}'.format(intent_request['userId'], intent_request['currentIntent']['name']))

    intent_name = intent_request['currentIntent']['name']

    # Dispatch to your bot's intent handlers
    if intent_name == 'getApplicationInfo':
         return getApplicationInfo(intent_request)
    elif intent_name == 'showQueryAttributes':
        return showQueryAttributes(intent_request)
    elif intent_name == 'peerReview':
        return sendForPeerReview(intent_request)
    elif intent_name == 'smallTalk':
        return i_smallTalk(intent_request)
    elif intent_name == 'evaluateAbilityToRepayScore':
        return evaluateAbilityToRepayScore(intent_request)
    elif intent_name == 'Help':
        return i_help(intent_request)

    raise Exception('Intent with name ' + intent_name + ' not supported')


""" --- Main handler --- """

def lambda_handler(event, context):
    """
    Route the incoming request based on intent.
    The JSON body of the request is provided in the event slot.
    """
    # By default, treat the user request as coming from the Pacific timezone.
    os.environ['TZ'] = 'America/Los_Angeles'
    time.tzset()
    logger.info('Received event: {}'.format(event))

    return dispatch(event)
