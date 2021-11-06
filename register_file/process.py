#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
from json import loads, dumps
from datetime import datetime
from hashlib import blake2b

# 3rd party:
from azure.functions import HttpRequest, HttpResponse
from sqlalchemy.exc import IntegrityError

# Internal: 
try:
    from __app__.db_tables.covid19 import Session, ProcessedFile
    from __app__.data_registration import register_file
except ImportError:
    from db_tables.covid19 import Session, ProcessedFile
    from data_registration import register_file

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Header
__author__ = "Pouria Hadjibagheri"
__copyright__ = "Copyright (c) 2021, Public Health England"
__license__ = "MIT"
__version__ = "0.0.1"
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    "main"
]


async def main(req: HttpRequest) -> HttpResponse:
    body = req.get_body()
    data = loads(body)

    file_name = data.get("fileName")

    if file_name is None:
        return HttpResponse(None, status_code=400)

    instance_id = blake2b(file_name.encode()).hexdigest()
    try:
        register_file(
            filepath=file_name,
            timestamp=datetime.utcnow(),
            instance_id=instance_id
        )
    except IntegrityError:
        return HttpResponse(None, status_code=403)

    response = dumps({
        "instance_id": instance_id
    })
    return HttpResponse(response, status_code=200, headers={"content-type": "application/json"})
