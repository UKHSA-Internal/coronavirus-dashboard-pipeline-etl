#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
import logging
from datetime import datetime
from enum import IntEnum
from itertools import chain

# 3rd party:
from sqlalchemy import text

# Internal:
try:
    from __app__.db_tables.covid19 import Session
    from __app__.db_etl_update_db.queries import (
        STATS_QUERY, PERMISSIONS_QUERY
    )
except ImportError:
    from db_tables.covid19 import Session
    from db_etl_update_db.queries import (
        STATS_QUERY, PERMISSIONS_QUERY
    )

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'main',
    'DatabaseTaskMode'
]


class DatabaseTaskMode(IntEnum):
    GET_TASKS = 0
    RUN_TASKS = 1


def get_partition_ids(date, category):
    date = datetime.strptime(date, "%Y-%m-%d")
    partition_names = [
        "other",
        "utla",
        "ltla",
        "nhstrust",
        "msoa"
    ]

    if "msoa" in category.lower():
        partition_names = ["msoa"]

    partitions = [
        f"{date:%Y_%-m_%-d}|{partition}"
        for partition in partition_names
    ]

    return partitions


def stats_query(date, category):
    query = STATS_QUERY.format(
        datestamp=date,
        partitions=f'{{{str.join(",", get_partition_ids(date, category))}}}'
    )

    return {
        "query": query,
        "preparation": None,
        "mapped": True
    }


def perms_query():
    for row in PERMISSIONS_QUERY.splitlines():
        yield {
            "query": row,
            "preparation": "SET LOCAL citus.multi_shard_modify_mode TO 'sequential';",
            "mapped": False
        }


def execute_task(query):
    session = Session()
    try:
        query_str = query["query"]

        if (prep := query["preparation"]) is not None:
            query_str += f"{prep}\n{query_str}"

        query_str = f"BEGIN;\n{query_str}\nCOMMIT;"
        if query["mapped"]:
            query_str = text(query_str)

        session.execute(query_str)
        session.flush()

    except Exception as err:
        session.rollback()
        raise err
    finally:
        session.close()

    return None


def main(payload):
    logging.info(f"DB setting updater triggered - payload: {payload}")
    category = payload.get("category")
    mode = payload.get("mode")

    if mode == DatabaseTaskMode.GET_TASKS:
        tasks = list()

        for task in chain([stats_query(payload['date'], category)], perms_query()):
            tasks.append({
                **payload,
                "category": category,
                "mode": DatabaseTaskMode.RUN_TASKS,
                "date": payload["date"],
                "task": task,
            })

            logging.info(f"Generated task: {tasks[-1]}")

        return tasks

    elif mode == DatabaseTaskMode.RUN_TASKS:
        task = payload["task"]
        execute_task(query=task)
        logging.info(f"Executed task: {task}")

        return f"DONE - {payload}"


if __name__ == "__main__":
    from json import dumps, loads

    test_tasks = main({
        "date": datetime.utcnow().strftime("%Y-%m-%d"),
        "category": "POSITIVITY & PEOPLE TESTED",
        "mode": DatabaseTaskMode.GET_TASKS
    })

    serialised_tasks = dumps(test_tasks)

    for tsk in loads(serialised_tasks):
        main(tsk)
