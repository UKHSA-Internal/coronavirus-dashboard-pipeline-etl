#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
import logging
from http import HTTPStatus
from json import dumps

# 3rd party:
from azure.functions import Context, HttpRequest, HttpResponse

# Internal:
from .service import application
from .database import get_latest_count

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    "dispatcher"
]


def robot_text() -> HttpResponse:
    logging.info("Robot hit response")

    data = """\
User-agent: *
Disallow: /
"""
    return HttpResponse(data, mimetype="text/plain; charset=utf-8")


def dispatcher(req: HttpRequest, latestAvailable: str, latestPublished: str,
                     latestReleased: str, context: Context) -> HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    uri = req.route_params.get('uri')
    if uri is not None and ("robot" in uri or "robot.txt" in uri):
        return robot_text()

    if uri == 'latest_count':
        latest_count = dict(value=get_latest_count())
        return HttpResponse(status_code=int(HTTPStatus.OK), body=dumps(latest_count))

    elif uri is None or uri == '/':
        ctx = {
            "latest_available": latestAvailable,
            "latest_published": latestPublished,
            "latest_released": latestReleased
        }

        return application(req, **ctx)

    return HttpResponse(status_code=int(HTTPStatus.NOT_FOUND))
