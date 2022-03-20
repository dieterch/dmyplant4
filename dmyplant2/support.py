import getpass
import base64
from datetime import datetime as dt
#import sys
import json
import os
import logging

period = 31*24*60*60


def jetzt():
    return dt.now().timestamp()


def forceUpdate(last):
    return jetzt() - period > last


def getCredentials():
    print("Please enter your myPlant login: ")
    print('User: ', end='')
    name = input()
    password = getpass.getpass()
    logging.info('New Credentials saved')
    return {
        "name": base64.b64encode(name.encode('utf-8')).decode('utf-8'),
        "password": base64.b64encode(password.encode("utf-8")).decode('utf-8'),
        "lastupdate": jetzt()
    }


def saveCredentials(cred):
    try:
        with open("./data/.credentials", "w") as file:
            json.dump(cred, file)
    except FileNotFoundError:
        raise


def cred():
    if not os.path.exists(os.getcwd() + '/data'):
        os.makedirs(os.getcwd() + '/data')
    if os.path.exists(os.getcwd() + '/data/.credentials'):
        with open(os.getcwd() + "/data/.credentials", "r", encoding='utf-8-sig') as file:
            cred = json.load(file)
        if forceUpdate(cred['lastupdate']):
            cred = getCredentials()
            saveCredentials(cred)
    else:
        cred = getCredentials()
        saveCredentials(cred)
