#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
import logging

# 3rd party:

# Internal:
try:
    from __app__.db_etl import run_direct, run_vaccinations_demographics, run_direct_msoas
    from __app__.utilities.generic_types import RawDataPayload
except ImportError:
    from db_etl import run_direct, run_demographics, run_direct_msoas
    from utilities.generic_types import RawDataPayload

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'main'
]


def main(payload):
    logging.info(f"Chunk processor triggered: {payload}")

    handlers = {
        ("vaccination", None): run_direct,
        ("positivity", None): run_direct,
        ("healthcare", None): run_direct,
        ("tests", None): run_direct,
        ("cases", None): run_direct,
        ("deaths", None): run_direct,
        ("vaccinations-by-vaccination-date", "MSOA"): run_direct_msoas,
        ("vaccinations-by-vaccination-date", None): {
            "age-demographics": run_demographics
        },
        ("cases-by-specimen-date", None): {
            "age-demographics": run_demographics
        },
        ("deaths28days-by-death-date", None): {
            "age-demographics": run_demographics
        },
    }

    category = payload["category"]
    subcategory = payload.get("subcategory")
    area_type = payload.get("area_type")

    if area_type is not None and (area_type := area_type.upper()) != "MSOA":
        area_type = None

    logging.info(
        f"MAIN ETL: Chunk processor triggered "
        f"for: '{category}:{subcategory}:{area_type}'"
    )

    handler = None
    if (category, area_type) in handlers:
        handler = handlers[(category, area_type)]

    if isinstance(handler, dict) and subcategory in handler:
        handler = handler[subcategory]

    logging.info(f"> Processor name: {handler.__name__}")

    if handler is None:
        logging.info(f"DONE: No process handler defined for '{category}:{subcategory}'.")
        return f"DONE: No process handler defined for '{category}:{subcategory}'."

    return handler(payload)
