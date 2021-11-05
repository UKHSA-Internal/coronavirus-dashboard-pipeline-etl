#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
from json import loads

# 3rd party:
from azure.functions import HttpRequest, HttpResponse
from sqlalchemy.sql import exists

# Internal: 
try:
    from __app__.db_tables.covid19 import Session, ProcessedFile
except ImportError:
    from db_tables.covid19 import Session, ProcessedFile

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


def file_exists(file_name):
    session = Session()
    try:
        resp = session.query(
            exists()
            .where(ProcessedFile.file_path == file_name)
        ).scalar()

        return resp
    except Exception as err:
        session.rollback()
        raise err
    finally:
        session.close()


async def main(req: HttpRequest) -> HttpResponse:
    body = req.get_body()
    data = loads(body)

    file_name = data.get("fileName")

    if file_name is None:
        return HttpResponse(None, status_code=400)

    found = file_exists(file_name=file_name)
    if found:
        return HttpResponse(None, status_code=403)

    return HttpResponse(None, status_code=204)
