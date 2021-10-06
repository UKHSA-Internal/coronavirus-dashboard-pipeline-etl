#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
from os import getenv

# 3rd party:

# Internal:

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


__all__ = [
    "ENV_KEY",
    "DEV_ENV",
    "PROD_ENV",
    "ENVIRONMENT",
    "URL_SEPARATOR",
    "JSON_SEPARATORS",
    "GITHUB_BRANCH"
]


ENV_KEY = "API_ENV"
DEV_ENV = "DEVELOPMENT"
STAGING_ENV = "STAGING"
PROD_ENV = "PRODUCTION"


ENV_GITHUB_BRANCH_MAP = {
    DEV_ENV: "development",
    STAGING_ENV: "staging",
    PROD_ENV: "master"
}

ENVIRONMENT = getenv(ENV_KEY, PROD_ENV)

GITHUB_BRANCH = ENV_GITHUB_BRANCH_MAP[ENVIRONMENT]

URL_SEPARATOR = "/"

JSON_SEPARATORS = (",", ":")
