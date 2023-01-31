#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
from datetime import datetime

# 3rd party:
from requests import get
from plotly import express as px, graph_objects as go
from sqlalchemy import text
from pandas import DataFrame, cut

# Internal:
try:
    from __app__.storage import StorageClient
    from __app__.db_tables.covid19 import Session
    from .queries import MAIN
except ImportError:
    from storage import StorageClient
    from db_tables.covid19 import Session
    from despatch_ops_workers.landing_page_map.queries import MAIN

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


__all__ = [
    'generate_landing_page_map'
]


LAYOUT = go.Layout(
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(0,0,0,0)',
    margin={
        'l': 0,
        'r': 0,
        'b': 0,
        't': 0,
    },
    showlegend=False,
    height=600,
    width=400,
    autosize=False,
    xaxis={
        "showgrid": False,
        "zeroline": False,
        "showline": False,
        "ticks": "outside",
        "tickson": "boundaries",
        "type": "date",
        "tickformat": '%b',
        "tickfont": {
            "family": '"GDS Transport", Arial, sans-serif',
            "size": 20,
            "color": "#6B7276"
        }
    }
)


figure_kws = dict(
    locations="areaCode",
    featureidkey="properties.code",
    color="categories",
    mapbox_style="white-bg",
    zoom=4.75,
    center={"lat": 54.4, "lon": -3}
)

colour_scales = [
    "#e0e543",  # [0 , 10)
    "#74bb68",  # [10 , 50)
    "#399384",  # [50 , 100)
    "#2067AB",  # [100 , 200)
    "#12407F",  # [200 , 400)
    "#640058",  # [400 , 800)
    "#3b0930",  # [800 , 1600)
    "#000000",  # [1600)
]


def get_geojson() -> dict:
    geojson = get("https://coronavirus.data.gov.uk/downloads/maps/utla-ref.geojson").json()
    return geojson


def get_style() -> dict:
    style = get("https://coronavirus.data.gov.uk/public/assets/geo/style_v4.json").json()
    style['layers'] = []
    return style


def store_image(image: bytes):
    with StorageClient(
            container="publicdata",
            path=f"assets/frontpage/images/map.png",
            content_type="image/png",
            cache_control="max-age=300, stale-while-revalidate=30",
            content_language=None,
            compressed=False
    ) as client:
        client.upload(image)


def plot_map(data):
    geojson = get_geojson()
    geo_style = get_style()

    colour_scale_binning = [
        0,
        10,
        50,
        100,
        200,
        400,
        800,
        1600,
        10000
    ]

    max_value = max(data['newCasesBySpecimenDateRollingRate'])
    colour_scale_binning = list(filter(lambda x: x < max_value, colour_scale_binning))

    data = data.assign(
        categories=cut(
            data["newCasesBySpecimenDateRollingRate"],
            bins=colour_scale_binning,
            labels=list(map(str, colour_scale_binning[:-1]))
        )
    ).dropna(axis=0, how='any')

    fig = px.choropleth_mapbox(
        data,
        geojson=geojson,
        color_discrete_map=dict(zip(map(str, colour_scale_binning), colour_scales)),
        **figure_kws
    )

    fig.update_geos(fitbounds="locations", resolution=110)
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
    fig.update_layout(LAYOUT)
    fig.update_traces(marker_line_width=0)
    fig.update_layout({
        "mapbox": {
            "layers": [
                {
                    "sourcetype": 'geojson',
                    "source": geojson,
                    "type": 'line',
                    "color": '#fff',
                    "line": {
                        "width": .1,
                    },
                },
            ],
            "style": geo_style,
        }
    })
    fig['layout']['geo']['subunitcolor'] = 'rgba(0,0,0,0)'
    fig["layout"].pop("updatemenus")

    image = fig.to_image(format="png", scale=2)

    return image


def get_data(timestamp: datetime):
    query = MAIN.format(date=f"{timestamp:%Y_%-m_%-d}")
    metric = "newCasesBySpecimenDateRollingRate"

    session = Session()
    conn = session.connection()
    try:
        resp = conn.execute(
            text(query),
            metric=metric,
            datestamp=f'{timestamp::%Y-%m-%d}'
        )
        raw_data = resp.fetchall()
    except Exception as err:
        session.rollback()
        raise err
    finally:
        session.close()

    print(len(raw_data))
    return DataFrame(raw_data, columns=["areaType", "areaCode", metric])


def generate_landing_page_map(payload):
    timestamp = datetime.fromisoformat(payload["timestamp"])
    data = get_data(timestamp)
    image = plot_map(data)
    store_image(image)

    return f"DONE: landing page map at '{payload['timestamp']}'"


if __name__ == "__main__":
    from datetime import timedelta
    generate_landing_page_map({"timestamp": (datetime.utcnow() - timedelta(days=1)).isoformat()})
