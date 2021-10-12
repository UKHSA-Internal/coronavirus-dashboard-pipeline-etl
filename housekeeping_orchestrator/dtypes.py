#!/usr/bin python3

from typing import TypedDict, Union, List


__all__ = [
    "ArchiverPayload",
    "RetrieverPayload",
    "RemoverPayload",
    "GenericPayload"
]


class RemoverPayload(TypedDict):
    removables: List[str]
    date: str


class ArchiverPayload(TypedDict):
    date: str
    category: str
    subcategory: Union[str, None]
    filename: str
    from_path: str
    content_type: str


class RetrieverPayload(TypedDict):
    timestamp: str
    environment: str


class GenericPayload(TypedDict):
    timestamp: str
    environment: str
    content: Union[List[ArchiverPayload], RetrieverPayload, RemoverPayload]
