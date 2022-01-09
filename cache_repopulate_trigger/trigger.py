#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
from typing import NoReturn
from json import loads
import logging

# 3rd party:
from azure.functions import ServiceBusMessage

# Internal:
try:
    from __app__.cache_prepopulate import main as repopulate
except ImportError:
    from cache_prepopulate import main as repopulate

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'main'
]


def main(message: ServiceBusMessage) -> NoReturn:
    logging.info(f"--- ServiceBus event has triggered the function. Starting the process")

    message = loads(message.get_body().decode())
    logging.info(f"Message: {message}")

    repopulate(message)

    return None
