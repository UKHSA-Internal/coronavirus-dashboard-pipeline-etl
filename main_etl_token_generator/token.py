#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
import logging
from os import getenv

# 3rd party:

# Internal:
try:
    from __app__.storage.etl_utils import TestOutput, MainOutput
    from __app__.db_etl.token import generate_token
except ImportError:
    from storage.etl_utils import TestOutput, MainOutput
    from db_etl.token import generate_token

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'main'
]


DEBUG = getenv("DEBUG", False)


def main(payload):
    if payload['environment'] == "DEVELOPMENT":
        return f"Not applicable - {payload}"

    logging.info(f"Token generator called with payload: {payload}")

    output_obj = MainOutput if not DEBUG else TestOutput

    token_output = output_obj(f"dispatch/token.bin")
    logging.info(f"\tGenerating new dispatch token")
    token_data = generate_token()
    token_output.set(token_data, content_type="application/octet-stream")
    logging.info(f'\tNew token generated and stored')

    return f"SUCCESS {payload}"


if __name__ == "__main__":
    main("")
