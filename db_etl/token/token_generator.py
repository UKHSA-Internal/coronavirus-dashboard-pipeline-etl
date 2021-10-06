#!/usr/bin python3

"""
<Description of the programme>

Author:        Pouria Hadjibagheri <pouria.hadjibagheri@phe.gov.uk>
Created:       22 Jun 2020
License:       MIT
Contributors:  Pouria Hadjibagheri
"""

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
import logging
from os import getenv
from secrets import token_bytes, token_urlsafe
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from datetime import datetime
from json import dumps
from requests import post as post_request

# 3rd party:

# Internal: 
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Header
__author__ = "Pouria Hadjibagheri"
__copyright__ = "Copyright (c) 2020, Public Health England"
__license__ = "MIT"
__version__ = "0.0.1"
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    "generate_token"
]


def generate_token():
    logging.info("Generating dispatch token.")

    token_time = datetime.now()
    token = {
        "issue_timestamp": token_time.isoformat() + "Z",
        "token": token_urlsafe(64),
        "environment": getenv("API_ENV")
    }

    token_data_raw = dumps(token, separators=(",", ":"))

    # Encryption key (AES256)
    key = getenv("TokenKey").encode()

    # Encrypting the data
    # GCM mode needs 12 fresh bytes every time
    nonce = token_bytes(12)
    aes = AESGCM(key)
    token_data_encrypted = nonce + aes.encrypt(nonce, token_data_raw.encode(), b"")

    # Dispatch request URL
    dispatch_url = getenv("TokenURL")

    response = post_request(
        dispatch_url,
        token_data_raw,
        headers={
            "Content-Type": "application/json",
            "Dispatch-Authentication": getenv("TokenDispatchSecret")
        }
    )

    logging.info(
        f"Token dispatch POST request - "
        f"response status code: {response.status_code}"
    )

    return token_data_encrypted
