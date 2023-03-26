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
smkey = b'LiMibInGY08vaTJ7Cr7S4SoCw7FJ-ZlB2_0vdHtpu6k='

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
    #print('User: ', end='')
    name = input('User: ')
    password = getpass.getpass(prompt='Password: ')
    totp_secret = getpass.getpass(prompt='TOTP Secret: ')
    
    logging.info('New Credentials saved')
    return {
        "name": name,
        "password": password,
        "totp_secret": totp_secret,
        "lastupdate": jetzt()
    }

def encryptCredentials(cred):
    fenc = Fernet(derive_key(smkey))
    cred_encrypted = fenc.encrypt(yaml.dump(cred).encode())
    return cred_encrypted

def decryptCredentials(cred):
    fdec = Fernet(derive_key(smkey))
    cred_decrypted = yaml.safe_load(fdec.decrypt(cred).decode())
    return cred_decrypted

def saveCredentials(cred):
    try:
        cred_encrypted = encryptCredentials(cred)
        with open("./data/.credentials", "wb") as file:
            #file.write(yaml.dump(cred_encrypted).encode('utf-8'))
            file.write(cred_encrypted)
    except FileNotFoundError:
        raise

def readCredentials():
    try:
        with open(os.getcwd() + "/data/.credentials", "rb") as file:
            #cred_encrypted = yaml.safe_load(file.read())
            cred_encrypted = file.read()
            cred = decryptCredentials(cred_encrypted)
            if forceUpdate(cred['lastupdate']):
                cred = getCredentials()
                saveCredentials(cred)
            return cred
    except Exception:
        raise

def cred():
    if not os.path.exists(os.getcwd() + '/data'):
        os.makedirs(os.getcwd() + '/data')
    if (os.path.exists(os.getcwd() + '/data/.credentials') and os.path.exists(os.getcwd() + '/.salt')):
        cred = readCredentials()
    else:
        derive_key(smkey, generate_salt=True) #(re)initialize cryptography if no file found
        cred = getCredentials()
        saveCredentials(cred)
