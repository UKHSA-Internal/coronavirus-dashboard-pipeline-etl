#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
import logging
from asyncio import get_event_loop, gather
from os import getenv

# 3rd party:
from orjson import dumps
from azure.functions import TimerRequest
from sqlalchemy import text

# Internal: 
try:
    from __app__.database.postgres import Connection
    from __app__.storage import AsyncStorageClient
    from __app__.db_tables import covid19 as db
except ImportError:
    from database.postgres import Connection
    from storage import AsyncStorageClient
    from db_tables import covid19 as db

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'main'
]


ENVIRONMENT = getenv("API_ENV")


shared_tables = [
    'covid19.tag',
    'covid19.metric_tag',
    'covid19.page',
    'covid19.announcement',
    'covid19.metric_asset',
    'covid19.metric_asset_to_metric',
    'covid19.change_log',
    'covid19.change_to_metric',
    'covid19.change_log_to_page',
    'public.auth_group',
    'public.auth_group_permissions',
    'public.auth_permission',
    'public.auth_user',
    'public.auth_user_groups',
    'public.auth_user_user_permissions',
    'public.django_admin_log',
    'public.django_content_type',
    'public.django_migrations',
    'public.guardian_groupobjectpermission',
    'public.guardian_userobjectpermission',
    'public.service_admin_oversightrecord',
    'public.service_admin_service',
    'public.service_admin_service_oversight_records',
    # 'public.django_session',
]


DATA_QUERY = "SELECT {columns} FROM {table_name};"


STRUCTURE_QUERY = """\
SELECT
    pga.attname                              AS column_name,
    format_type(pga.atttypid, pga.atttypmod) AS data_type,
    pga.attname IN (
        SELECT pag2.attname AS name
        FROM pg_class AS pgc2
            JOIN pg_index     AS pgi  ON pgc2.oid = pgi.indrelid  AND pgi.indisprimary
            JOIN pg_attribute AS pag2 ON pgc2.oid = pag2.attrelid AND pag2.attnum = ANY(pgi.indkey)
        WHERE pgc2.oid = (:table)::regclass::oid
    ) AS is_pk
FROM pg_attribute AS pga
WHERE pga.attnum > 0
  AND NOT pga.attisdropped
  AND pga.attrelid::REGCLASS::VARCHAR = :table;\
"""


async def get_data(table_name: str):
    logging.info(f"> Processing '{table_name}'")

    session = db.Session()
    conn = session.connection()
    try:
        q = conn.execute(text(STRUCTURE_QUERY), table=table_name)
        table_struct = q.fetchall()
    except Exception as err:
        session.rollback()
        raise err
    finally:
        session.close()

    columns = [f"\"{row['column_name']}\"" for row in table_struct]

    query = DATA_QUERY.format(
        table_name=table_name,
        columns=str.join(", ", columns)
    )

    async with Connection() as conn:
        data = await conn.fetch(query)

    if not len(data):
        return True

    logging.info(f">> Extracted {len(data)} rows from '{table_name}'")

    json_data = dumps({
        "primary_keys": [item['column_name'] for item in table_struct if item['is_pk']],
        "columns": {row['column_name']: row['data_type'] for row in table_struct},
        "table_name": table_name,
        "data": list(map(dict, data))
    })

    path = f"{table_name.replace('.', '__')}.json"
    async with AsyncStorageClient(container="migrations", path=path) as blob_cli:
        await blob_cli.upload(json_data.decode())

    logging.info(f">> Stored data for '{table_name}' in the storage")

    return True


async def main(timer: TimerRequest):
    logging.info(f"--- Web hook has triggered the function. Starting the process...")

    if ENVIRONMENT != "PRODUCTION":
        return

    event_loop = get_event_loop()

    tasks = list()
    for name in shared_tables:
        task = event_loop.create_task(get_data(name))
        tasks.append(task)

    await gather(*tasks)


if __name__ == "__main__":
    loop = get_event_loop()
    loop.run_until_complete(main(""))
