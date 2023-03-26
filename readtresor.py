import os
import yaml
import getpass
import base64
import cryptography
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.backends import default_backend
from cryptography.fernet import Fernet
from pprint import pprint

pw = getpass.getpass(prompt='Password: ')
passphrase = pw.encode()
salt = b'\x94\x85\x9d\x9eQA>&\xed\x97\xb1\xae\n\xf8{\xa2'

def derive_key(passphrase, lsalt):
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=lsalt,
        iterations=1000,
        backend=default_backend()
    )
    return base64.urlsafe_b64encode(kdf.derive(passphrase))

fernet = Fernet(derive_key(passphrase, salt))

try:
    with open("tresor", "rb") as file:
        cred_encrypted = file.read()
        cred_decrypted = yaml.safe_load(fernet.decrypt(cred_encrypted).decode())
        pprint(cred_decrypted)
except FileNotFoundError as e:
    print(e)
except cryptography.fernet.InvalidToken:
    print(f"falsches Passwort")
