#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
import logging
from typing import NoReturn
from os import getenv
from gzip import decompress
from asyncio import get_event_loop
from datetime import datetime
from sqlalchemy.dialects.postgresql import insert, dialect as postgres

# 3rd party:
from orjson import loads
from azure.functions import InputStream

# Internal:
try:
    from __app__.database.postgres import Connection
    from __app__.db_tables import covid19 as db
except ImportError:
    from database.postgres import Connection
    from db_tables import covid19 as db

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'main'
]


ENVIRONMENT = getenv("API_ENV")

TEMP_TABLE = "CREATE TEMPORARY TABLE {table_name}_temp ({table_struct})"

# UPSERT = """\
# INSERT INTO {table_name} ({column_names})
# VALUES
#     ({column_values})
# ON CONFLICT ({primary_keys})
#     DO UPDATE
#         SET {updates}
#     WHERE {passed_rows}\
# """


UPSERT = """\
INSERT INTO {table_name} ({column_names})
    SELECT * FROM {temp_table_name}_temp
ON CONFLICT ({primary_keys})
    DO UPDATE
        SET {updates}
    WHERE {passed_rows}\
"""

modeled_data = {
    'covid19.tag': db.Tag,
    'covid19.metric_tag': db.MetricTag,
    'covid19.page': db.Page,
    'covid19.announcement': db.Announcement,
    'covid19.metric_asset': db.MetricAsset,
    'covid19.metric_asset_to_metric': db.MetricAssetToMetric,
}


async def main(blob: InputStream) -> NoReturn:
    logging.info(f"--- Function triggered by a blob event. Starting the process...")

    if ENVIRONMENT == "PRODUCTION":
        return

    blob_payload = blob.read()
    payload = loads(decompress(blob_payload))

    temp_table_struct = [
        f'"{name}" {dtype}'
        for name, dtype in payload['columns'].items()
    ]

    temp_table_name = payload['table_name'].split(".")[1]

    temp_table_query = TEMP_TABLE.format(
        table_name=temp_table_name,
        table_struct=str.join(", ", temp_table_struct)
    )

    updates = [
        f'"{col}"=EXCLUDED."{col}"'
        for col in payload['columns']
        if col not in payload['primary_keys']
    ]

    passed_rows = [
        f'{payload["table_name"]}."{col}" <> EXCLUDED."{col}"'
        for col in payload['columns']
        if col not in payload['primary_keys']
    ]

    escaped_columns = [f'"{col}"' for col in payload['columns']]
    escaped_pks = [f'"{col}"' for col in payload['primary_keys']]

    upsert_statement = UPSERT.format(
        table_name=payload['table_name'],
        temp_table_name=temp_table_name,
        column_names=str.join(", ", escaped_columns),
        primary_keys=str.join(", ", escaped_pks),
        updates=str.join(", ", updates),
        passed_rows=str.join(" OR ", passed_rows)
    )

    for column, dtype in payload['columns'].items():
        if "time" not in dtype and "date" not in dtype:
            continue

        for item in payload['data']:
            if item[column] is None:
                continue

            item[column] = datetime.fromisoformat(item[column])

    if (table_name := payload['table_name']) in modeled_data:
        table = modeled_data[table_name]
        session = db.Session()
        conn = session.connection()
        try:
            insert_stmt = (
                insert(table.__table__)
                .values(payload['data'])
            )

            index_elms = [getattr(table, col) for col in payload['primary_keys']]

            set_data = {
                getattr(table, col).name: getattr(insert_stmt.excluded, col)
                for col in payload['columns']
                if col not in payload['primary_keys']
            }

            stmt = insert_stmt.on_conflict_do_update(
                index_elements=index_elms,
                set_=set_data if len(set_data) else {
                    getattr(table, col).name: getattr(insert_stmt.excluded, col)
                    for col in payload['columns']
                }
            )

            conn.execute(stmt)
            session.flush()
        except Exception as err:
            session.rollback()
            raise err
        finally:
            session.close()

        return

    async with Connection() as db_client, db_client.transaction(isolation='serializable'):
        await db_client.execute(temp_table_query)

        await db_client.copy_records_to_table(
            f"{temp_table_name}_temp",
            records=[tuple(item.values()) for item in payload['data']]
        )

        await db_client.execute(upsert_statement)


if __name__ == "__main__":
    with open("/Users/pouria/Downloads/public__auth_user.json", "rb") as fp:
        get_event_loop().run_until_complete(main(fp))
