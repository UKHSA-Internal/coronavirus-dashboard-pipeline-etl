#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
import logging
from typing import Dict, Callable, Union, Any
# 3rd party:

# Internal:
from .vaccinations import process_vaccinations
from .testing import process_testing
from .timestamp_boxplots import process as process_boxplots

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    "main"
]

ProcessorType = Dict[str, Callable[[Dict[str, Any]], str]]
HandlerType = Union[ProcessorType, Dict[str, ProcessorType]]


def main(payload):
    # ToDo: This needs to be implemented as a piping process.

    handlers: HandlerType = {
        "vaccination": process_vaccinations,
        # "tests": process_testing  # ToDo: Deprecated - to be removed.
    }

    category = payload["category"]
    subcategory = payload.get("subcategory")

    logging.info(f"Chunk postprocessor triggered for: '{category}:{subcategory}'.")

    try:
        process_boxplots(payload)
    except Exception as err:
        logging.error(err)
        logging.info(f"FAILED: Unable to create boxplot for category {category}.")

    handler = None
    if category in handlers:
        handler = handlers[category]

    if isinstance(handler, dict) and subcategory in handler:
        handler = handler[subcategory]

    if handler is None:
        logging.info(f"DONE: No postprocessor defined for '{category}:{subcategory}'.")
        return f"DONE: No postprocessor defined for '{category}:{subcategory}'."

    logging.info(f"Activity '{category}' triggered.")
    try:
        result = handler(payload)
        return result
    finally:
        logging.info(f">> Activity '{category}:{subcategory}' is done.")
