#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
from dataclasses import dataclass

# 3rd party:

# Internal: 
try:
    from .queries import MAIN
except ImportError:
    from despatch_ops_workers.archive_dates.queries import MAIN

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'PARAMETERS'
]


PARAMETERS = {
    'MAIN': {
        'query': MAIN,
        'process_name': "MAIN",
        'container': "publicdata",
        'path': "assets/dispatch/dates.json"
    },
}
