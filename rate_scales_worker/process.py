#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
from datetime import datetime
import logging
from io import BytesIO

# 3rd party:

from pandas import DataFrame
from sqlalchemy import text
from matplotlib.pyplot import subplots, close
from matplotlib import markers

# Internal:
try:
    from __app__.storage import StorageClient
    from __app__.db_tables.covid19 import Session
    from __app__.despatch_ops_workers.utils.utils.variables import AREA_TYPE_PARTITION
    from .queries import RATES
except ImportError:
    from storage import StorageClient
    from despatch_ops_workers.rate_scales.queries import RATES
    from db_tables.covid19 import Session
    from despatch_ops_workers.utils.variables import AREA_TYPE_PARTITION

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    "main"
]

# Area colours
area_colours = [
    "#e0e543",  # [0 , 10)
    "#74bb68",  # [10 , 50)
    "#399384",  # [50 , 100)
    "#2067AB",  # [100 , 200)
    "#12407F",  # [200 , 400)
    "#53084A",  # [400 , 800)
    "#2B0226",  # [800 , 3200)
]

# Non-linear axis ticks.
x_ticks = [0, 10, 50, 100, 200, 400, 800, 3200]


def store_graph(data: BytesIO, area_type, area_code, date):
    # Comment for testing
    with StorageClient(
        "publicdata",
        f"assets/frontpage/scales/{date}/{area_type}/{area_code}.jpg",
        content_type="image/jpeg",
        cache_control="max-age=60, must-revalidate",
        content_language=None,
        compressed=False
    ) as client:
        client.upload(data.read())

    return True


def get_latest_scale_records(payload):
    date = datetime.fromisoformat(payload["timestamp"])
    area_type = payload["area_type"]

    query = RATES.format(
        area_type=AREA_TYPE_PARTITION[area_type],
        date=f"{date:%Y_%-m_%-d}"
    )

    metric, attr = "newCasesBySpecimenDateRollingRate", "value"
    if area_type == "msoa":
        metric, attr = "newCasesBySpecimenDate", "rollingRate"

    session = Session()
    conn = session.connection()
    try:
        resp = conn.execute(
            text(query),
            metric=metric,
            attr=attr,
            area_type=area_type
        )
        raw_data = resp.fetchall()
    except Exception as err:
        session.rollback()
        raise err
    finally:
        session.close()

    data = DataFrame(raw_data, columns=["area_type", "area_code", "rate"])

    results = {
        "records": data.to_dict(orient="records"),
        "percentiles": {
            "min": data.loc[:, "rate"].min(),
            "0.1": data.loc[:, "rate"].quantile(0.1).round(1),
            "0.4": data.loc[:, "rate"].quantile(0.4).round(1),
            "median": data.loc[:, "rate"].median().round(1),
            "0.6": data.loc[:, "rate"].quantile(0.6).round(1),
            "0.9": data.loc[:, "rate"].quantile(0.9).round(1),
            "max": data.loc[:, "rate"].max(),
        },
        "timestamp": payload["timestamp"]
    }

    return results


def generate_scale_graph(payload):
    area_code = payload["area_code"]
    area_type = payload["area_type"]
    rate = payload["rate"]
    percentiles = payload["percentiles"]

    fig, ax = subplots(figsize=[5, 1.65])

    ax.plot([percentiles['median']] * 2, [-.7, .94], c='w', lw=8)
    ax.plot([percentiles['median']] * 2, [-.7, .93], c='k', lw=6)

    ax.plot(rate, 2.75, marker=markers.CARETDOWNBASE, markersize=25, c='k', clip_on=False)
    ax.annotate(
        ("%.1f" if rate % 1 else "%d") % rate,
        (rate, 2.85),
        fontsize=24,
        ha="center",
        va='bottom',
        fontweight='bold',
        clip_on=False
    )

    # Default axis range:
    # [10th percentile, 60th percentile]
    ax_min, ax_max = percentiles["0.1"], percentiles["0.9"]

    if percentiles["0.9"] <= rate:
        # If rate is greater than the 90th percentile,
        # set axis extrema to [40th percentile, max rate].
        ax_min, ax_max = percentiles["0.4"], percentiles["max"] + 50

    elif percentiles["0.1"] >= rate:
        # If rate is smaller than the 10th percentile,
        # set axis extrema to [0, 60th percentile].
        ax_min, ax_max = 0, percentiles["0.6"]

    # If axis extrema are between two succeeding ticks,
    # do not set the predefined scale - i.e. revert to
    # default (linearly spaced ticks).
    xtick_extrema = sorted([*x_ticks, ax_min, ax_max])
    left_tick_ind = xtick_extrema.index(ax_min)
    right_tick_ind = xtick_extrema.index(ax_max)

    if right_tick_ind - left_tick_ind > 1:
        ax.set_xticks(x_ticks[1:])

    ax.set_xlim([ax_min, ax_max])

    # Position of "... average" label.
    # Depends on axis extrema to prevent spillage
    # from the sides - i.e. push the graph inwards.
    label_position = "left"

    if percentiles['median'] > (ax_max - 70):
        label_position = "right"

    elif percentiles['median'] < (ax_min + 70):
        label_position = "left"

    ax.set_yticks([])
    ax.set_ylim([-1, 3.5])

    # Text offset to prevent overlap of text and
    # median line. This must be relative as axis
    # extrema and thus the length varies.
    offset = (ax_max - ax_min) / 40

    ax.annotate(
        "England average" if area_type.lower() == "msoa" else "UK average",
        (percentiles['median'] + (offset if label_position == "left" else -offset), -.9),
        fontsize=16,
        ha=label_position,
        va='bottom',
        fontweight="bold",
        clip_on=False
    )

    for start, end, colour in zip(x_ticks[:-1], x_ticks[1:], area_colours):
        ax.fill_betweenx([0, 1], start, end, color=colour)
        ax.plot([start, start], [0, 1], c='w', lw=.8)

    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    ax.spines['bottom'].set_visible(False)
    ax.spines['left'].set_visible(False)
    ax.xaxis.tick_top()

    ax.spines['top'].set_position(('data', 1))
    ax.set_xticklabels(ax.get_xticks(), fontsize=14)
    ax.tick_params(axis='x', pad=2)

    fig.tight_layout(pad=0.01)

    img = BytesIO()
    fig.savefig(
        img,
        format="jpg",
        dpi=150,
        pil_kwargs={'optimize': True, 'progressive': True}
    )
    img.seek(0)

    store_graph(img, area_type, area_code, payload["date"])

    close(fig)

    return f"DONE: scale item {payload['timestamp']}:{area_type}:{area_code}"


def main(payload):
    if payload["type"] == "RETRIEVE":
        return get_latest_scale_records(payload)

    elif payload["type"] == "GENERATE":
        generate_scale_graph(payload)
        return f"DONE: {payload}"

    raise ValueError("Undefined workflow for rate scale generators.")


# Uncomment for testing
if __name__ == '__main__':
    from datetime import timedelta
    from multiprocessing import Pool, cpu_count
    ts = (datetime.utcnow() - timedelta(days=0)).isoformat()

    for a_type in ["nation", "region", "utla", "ltla", "msoa"]:
        res = get_latest_scale_records({
            "timestamp": ts,
            "area_type": a_type
        })

        payloads = list()
        for item in res["records"]:
            payloads.append({
                "date": ts.split("T")[0],
                "timestamp": res['timestamp'],
                "area_type": item['area_type'],
                "area_code": item['area_code'],
                "rate": item['rate'],
                "percentiles": res['percentiles']
            })

        with Pool(processes=cpu_count() - 2) as pool:
            pool.map(generate_scale_graph, payloads)
