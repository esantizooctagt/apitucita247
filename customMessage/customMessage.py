import sys
import logging
import json

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    # TODO implement
    logger.info(event)
    if event['triggerSource'] == 'CustomMessage_SignUp':
        userId = event['request']['userAttributes']['custom:userId']
        code = event['request']['codeParameter']
        event['response']['emailSubject'] = "Welcome to Tu Cita 24/7";
        event['response']['emailMessage'] = "Thank you for signing up. Copy and paste this link https://console.tucita247.com/en/verification/" + userId + "/" + code + " or <a href='https://console.tucita247.com/en/verification/" + userId + "/" + code + "'>Click here</a> to activate your account.";
    if event['triggerSource'] == 'CustomMessage_ResendCode':
        userId = event['request']['userAttributes']['custom:userId']
        code = event['request']['codeParameter']
        event['response']['emailSubject'] = "Welcome to Tu Cita 24/7";
        event['response']['emailMessage'] = "Thank you for signing up. click here https://console.tucita247.com/en/verification/" + userId + "/" + code + " to activate your account.";
    if event['triggerSource'] == 'CustomMessage_ForgotPassword':
        userId = event['request']['userAttributes']['custom:userId']
        code = event['request']['codeParameter']
        event['response']['emailSubject'] = "Recover Password";
        event['response']['emailMessage'] = "Copy and paste this link https://console.tucita247.com/en/resetpassword/" + userId + "/" + code + " or <a href='https://console.tucita247.com/en/resetpassword/" + userId + "/" + code + "'>Click here</a> to reset your password";
    if event['triggerSource'] == 'CustomMessage_AdminCreateUser':
        userId = event['request']['userAttributes']['custom:userId']
        code = event['request']['codeParameter']
        event['response']['emailSubject'] = "Your temporary password";
        event['response']['emailMessage'] = "Click here https://console.tucita247.com/en/resetpassword/" + userId + "/0" + " your temporary passwrord is : " + code;
    if event['triggerSource'] == 'CustomMessage_ResendCode':
        userId = event['request']['userAttributes']['custom:userId']
        code = event['request']['codeParameter']
        event['response']['emailSubject'] = "Resend Act Code";
        event['response']['emailMessage'] = "Copy and paste this code " + code + " or click here https://console.tucita247.com/en/resetpassword/" + userId + "/" + code;
    return event
