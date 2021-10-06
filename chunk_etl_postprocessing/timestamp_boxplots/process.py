#!/usr/bin python3

"""
<Description of the programme>

Author:        Pouria Hadjibagheri <pouria.hadjibagheri@phe.gov.uk>
Created:       21 Aug 2021
License:       MIT
Contributors:  Pouria Hadjibagheri
"""

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
from io import BytesIO
from datetime import datetime
import re

# 3rd party:
from matplotlib.pyplot import subplots, close
from sqlalchemy import text
from pandas import DataFrame

# Internal:
try:
    from chunk_etl_postprocessing.timestamp_boxplots.queries import CATEGORY_TIMESTAMPS
    from __app__.db_tables.covid19 import Session
    from __app__.storage import StorageClient
    from __app__.utilities.data_files import category_label
except ImportError:
    from chunk_etl_postprocessing.timestamp_boxplots.queries import CATEGORY_TIMESTAMPS
    from db_tables.covid19 import Session
    from storage import StorageClient
    from utilities.data_files import category_label

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Header
__author__ = "Pouria Hadjibagheri"
__copyright__ = "Copyright (c) 2021, Public Health England"
__license__ = "MIT"
__version__ = "0.0.1"
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'process'
]


def to_mins(timestamp: datetime):
    """
    Calculates n minutes from midnight.
    """
    delta = timestamp - datetime(timestamp.year, timestamp.month, timestamp.day)
    return delta.seconds // 60


def store_graph(data: BytesIO, category, timestamp):
    with StorageClient(
        "static",
        re.sub(r"[:\s'\"&]+", "_", f"admin/releases/{category}/{timestamp}.png"),
        content_type="image/png",
        cache_control="max-age=60, must-revalidate",
        content_language=None,
        compressed=False
    ) as client:
        client.upload(data.read())

    return True


def get_data(category: str, timestamp):
    query = text(CATEGORY_TIMESTAMPS)

    session = Session()
    conn = session.connection()
    try:
        resp = conn.execute(query, category=category, timestamp=timestamp)
        raw_data = resp.fetchall()
    except Exception as err:
        session.rollback()
        raise err
    finally:
        session.close()

    df = DataFrame(raw_data, columns=['timestamp'])

    df.timestamp = (
        df
        .timestamp.map(to_mins)
        .astype({"timestamp": int})
    )
    return df


def process(payload):
    category = category_label(payload)
    # category = payload['category']

    if category is None:
        return f"Nothing to process: {payload['timestamp']}"

    ts_num = to_mins(datetime.fromisoformat(payload["timestamp"]))
    df = get_data(category, datetime.fromisoformat(payload["timestamp"]))

    colour = "g"
    if ts_num > df.loc[:, "timestamp"].quantile(0.75):
        colour = "r"

    fig, ax = subplots(figsize=[4, .5])
    bax = ax.boxplot(
        df.timestamp,
        vert=False,
        widths=[.06],
        patch_artist=True,
        flierprops=dict(marker='.', markersize=1),
        boxprops=dict(linewidth=1),
        medianprops=dict(lw=2, c='w'),
    )
    ax.scatter([ts_num], [1], marker="o", s=60, c=colour, zorder=40, edgecolors='w')
    ax.set_ylim([.95, 1.05])
    ax.axis('off')

    for cap in bax['caps']:
        cap.set_ydata(cap.get_ydata() + (-.015, +.015))

    for box in bax['boxes']:
        box.set_facecolor('k')

    fp = BytesIO()
    fig.savefig(
        fp,
        format='png',
        dpi=150,
        transparent=True,
        pil_kwargs={'progressive': True}
    )
    fp.seek(0)

    store_graph(fp, category, payload['timestamp'].split("T")[0])

    close(fig)

    return f"DONE: box generated {payload['timestamp']}:{category}"


def proc(ts, cat):
    process({"category": cat, "timestamp": ts['timestamp'].isoformat()})


if __name__ == '__main__':
    cats = [
        "MAIN",
        "MSOA",
        "AGE-DEMOGRAPHICS: VACCINATION - EVENT DATE",
        "VACCINATION",
        "MSOA: VACCINATION - EVENT DATE",
        "AGE DEMOGRAPHICS: CASE - EVENT DATE",
        "POSITIVITY & PEOPLE TESTED",
        "AGE-DEMOGRAPHICS: DEATH28DAYS - EVENT DATE",
    ]

    q = text("""
SELECT timestamp
FROM covid19.release_reference AS rr
LEFT JOIN covid19.release_category AS rc ON rc.release_id = rr.id
WHERE process_name = :category
""")

    from multiprocessing import Pool, cpu_count
    from functools import partial

    ss = Session()
    ss_conn = ss.connection()
    try:
        for cat in cats:
            ss_resp = ss_conn.execute(q, category=cat)

            # category_label({
            #     "category": cat,
            #     "timestamp":
            #     # "area_type": "msoa",
            #     # "subcategory": "age-demographics"
            # }))

            ss_raw_data = ss_resp.fetchall()

            fn = partial(proc, cat=cat)

            with Pool(processes=cpu_count() - 1) as pool:
                pool.map(fn, ss_raw_data)

    except Exception as err:
        ss.rollback()
        raise err
    finally:
        ss.close()

    # print(ss_raw_data)
    # process({
    #     "category": "vaccinations-by-vaccination-date",
    #     "timestamp": ss_raw_data["ts"].isoformat(),
    #     # "area_type": "msoa",
    #     "subcategory": "age-demographics"
    # })
