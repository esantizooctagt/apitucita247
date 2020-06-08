import sys
import logging
import json

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    # TODO implement
    logger.info(event)
    if event['triggerSource'] == 'CustomMessage_SignUp':
        userName = event['request']['custom:userId']
        code = event['request']['codeParameter']
        event['response']['emailSubject'] = "Welcome to the service"
        event['response']['emailMessage'] = "Thank you for signing up. " + code + " is your verification code click here https://console.tucita247.com/verification/" + userName + "/" + code
    if event['triggerSource'] == 'CustomMessage_ResendCode':
        userName = event['request']['custom:userId']
        code = event['request']['codeParameter']
        event['response']['emailSubject'] = "Welcome to the service"
        event['response']['emailMessage'] = "Thank you for signing up. " + code + " is your verification code click here https://console.tucita247.com/verification/" + userName + "/" + code
    if event['triggerSource'] == 'CustomMessage_ForgotPassword':
        userName = event['request']['custom:userId']
        code = event['request']['codeParameter']
        event['response']['emailSubject'] = "Recover Password"
        event['response']['emailMessage'] = "Copy and paste this code " + code + " or click here https://console.tucita247.com/resetpassword/" + userName + "/" + code
    if event['triggerSource'] == 'CustomMessage_AdminCreateUser':
        userName = event['request']['custom:userId']
        code = event['request']['codeParameter']
        event['response']['emailSubject'] = "Your temporary password"
        event['response']['emailMessage'] = "Copy and paste this code " + code + " or click here https://console.tucita247.com/resetpassword/" + userName + "/" + code
    if event['triggerSource'] == 'CustomMessage_ResendCode':
        userName = event['request']['custom:userId']
        code = event['request']['codeParameter']
        event['response']['emailSubject'] = "Resend Act Code"
        event['response']['emailMessage'] = "Copy and paste this code " + code + " or click here https://console.tucita247.com/resetpassword/" + userName + "/" + code
    return event
