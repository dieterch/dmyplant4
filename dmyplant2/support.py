import getpass
import base64
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.backends import default_backend
from cryptography.fernet import Fernet
from datetime import datetime as dt
import yaml
import os
import logging

period = 31*24*60*60

def derive_key(passphrase, generate_salt=False):
    salt = SaltManager(generate_salt)

    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt.get(),
        iterations=1000,
        backend=default_backend()
    )
    return base64.urlsafe_b64encode(kdf.derive(passphrase))

class SaltManager(object):
    def __init__(self, generate, path='.salt'):
        self.generate = generate
        self.path = path

    def get(self):
        if self.generate:
            return self._generate_and_store()
        return self._read()

    def _generate_and_store(self):
        salt = os.urandom(16)
        with open(self.path, 'wb') as f:
            f.write(salt)
        return salt

    def _read(self):
        with open(self.path, 'rb') as f:
            return f.read()


def jetzt():
    return dt.now().timestamp()


def forceUpdate(last):
    return jetzt() - period > last


def getCredentials():
    print("""
Please enter your myPlant login: 
--------------------------------
    """)
    print('User: ', end='')
    name = input()
    password = getpass.getpass(prompt='Password: ')
    totp_secret = getpass.getpass(prompt='TOTP Secret: ')
    
    logging.info('New Credentials saved')
    return {
        "name": name,
        "password": password,
        "totp_secret": totp_secret,
        "lastupdate": jetzt()
    }

def encryptCredentials():
    pass

def decryptCredentials():
    pass

def saveCredentials(cred):
    try:
        with open("./data/.credentials", "wb") as file:
            file.write(yaml.dump(cred).encode('utf-8'))
    except FileNotFoundError:
        raise

def cred():
    if not os.path.exists(os.getcwd() + '/data'):
        os.makedirs(os.getcwd() + '/data')
    if os.path.exists(os.getcwd() + '/data/.credentials'):
        #with open(os.getcwd() + "/data/.credentials", "rb", encoding='utf-8-sig') as file:
        with open(os.getcwd() + "/data/.credentials", "rb") as file:
            cred = yaml.safe_load(file.read())
            #cred = json.load(file)
        if forceUpdate(cred['lastupdate']):
            cred = getCredentials()
            saveCredentials(cred)
    else:
        cred = getCredentials()
        saveCredentials(cred)
