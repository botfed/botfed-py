import ssl
import os

os.environ['SSLKEYLOGFILE'] = os.path.expanduser('~/ssl-key.log')
context = ssl.create_default_context()
cipher = "ECDHE-RSA-AES256-SHA384"
context.set_ciphers(cipher)