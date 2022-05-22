#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
from typing import Dict, Callable, Union
from operator import itemgetter

# 3rd party:
from plotly import graph_objects as go
from numpy import zeros, NaN
from pandas import Series

# Internal: 

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'plot_thumbnail',
    'plot_vaccinations'
]


IsImproving: Dict[str, Callable[[Union[int, float]], bool]] = {
    "newCasesByPublishDate": lambda x: x < 0,
    "newCasesBySpecimenDate": lambda x: x < 0,
    "newDeaths28DaysByPublishDate": lambda x: x < 0,
    "newDeaths28DaysByDeathDate": lambda x: x < 0,
    "newVirusTestsByPublishDate": lambda x: 0,
    "newAdmissions": lambda x: x < 0,
}


TIMESERIES_LAYOUT = go.Layout(
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(0,0,0,0)',
    margin={
        'l': 0,
        'r': 0,
        'b': 4,
        't': 0,
    },
    showlegend=False,
    height=350,
    autosize=False,
    xaxis={
        "showgrid": False,
        "zeroline": False,
        "showline": False,
        "ticks": "outside",
        "tickson": "boundaries",
        "type": "date",
        "tickformat": '%b',
        # "tickvals": x[::30],
        # "tickmode": 'array',
        "tickfont": {
            "family": '"GDS Transport", Arial, sans-serif',
            "size": 20,
            "color": "#6B7276"
        }
    }
)

WAFFLE_LAYOUT = dict(
    margin=dict(
        l=0,
        r=0,
        t=0,
        b=0
    ),
    showlegend=False,
    plot_bgcolor="rgba(231,231,231,0)",
    paper_bgcolor="rgba(255,255,255,0)",
    xaxis=dict(
        showgrid=False,
        ticks=None,
        showticklabels=False
    ),
    yaxis=dict(
        showgrid=False,
        ticks=None,
        showticklabels=False,
        scaleratio=1,
        scaleanchor="x",
    ),
)

COLOURS = {
    "good": {
        "line": "rgba(0,90,48,1)",
        "fill": "rgba(204,226,216,1)"
    },
    "bad": {
        "line": "rgba(148,37,20,1)",
        "fill": "rgba(246,215,210,1)"
    },
    "neutral": {
        "line": "rgba(56,63,67,1)",
        "fill": "rgba(235,233,231,1)"
    }
}


def get_colour(change, metric_name) -> dict:
    change_value = float(change or 0)
    improving = IsImproving[metric_name](change_value)

    trend_colour = COLOURS["neutral"]

    if isinstance(improving, bool):
        if improving:
            trend_colour = COLOURS["good"]
        else:
            trend_colour = COLOURS["bad"]

    return trend_colour


def plot_thumbnail(timeseries, change, metric_name: str) -> str:
    get_date = itemgetter("date")
    get_value = itemgetter("value")

    trend_colour = get_colour(get_value(change), metric_name)

    x = list(map(get_date, timeseries))
    y = Series(list(map(get_value, timeseries))).rolling(7, center=True).mean()
    fig = go.Figure(
        go.Scatter(
            x=x[13:],
            y=y[13:],
            line={
                "width": 2,
                "color": COLOURS['neutral']['line']
            }
        ),
        layout=TIMESERIES_LAYOUT
    )

    fig.add_trace(
        go.Scatter(
            x=x[:14],
            y=y[:14],
            line={
                "width": 2
            },
            mode='lines',
            fill='tozeroy',
            hoveron='points',
            opacity=.5,
            line_color=trend_colour['line'],
            fillcolor=trend_colour['fill'],
        )
    )

    fig.update_yaxes(showticklabels=False)
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=False)

    return fig.to_image(format="svg", height='150px').decode()


def get_vaccination_matrices(threshold, identifier):
    data = zeros(100)

    if identifier > 1:
        data[:] = NaN

    data[:threshold] = identifier
    data = data.reshape([10, 10])

    return data


def plot_vaccinations(data):
    first_dose = data["first_dose"]
    second_dose = data["second_dose"]
    third_dose = data["third_dose"]

    first_dose_matrix = get_vaccination_matrices(first_dose, 1)
    second_dose_matrix = get_vaccination_matrices(second_dose, 2)
    third_dose_matrix = get_vaccination_matrices(third_dose, 3)

    fig = go.Figure()

    fig.add_trace(
        go.Heatmap(
            z=first_dose_matrix,
            hoverongaps=False,
            showscale=False,
            ygap=3,
            xgap=3,
            colorscale=[
                [0, "rgba(216,216,216,1)"],
                [.5, "rgba(119,196,191,1)"],
                [.9, "rgba(119,196,191,1)"],
                [1, "rgba(119,196,191,1)"],
            ]
        )
    )

    fig.add_trace(
        go.Heatmap(
            z=second_dose_matrix,
            hoverongaps=False,
            showscale=False,
            ygap=3,
            xgap=3,
            colorscale=[
                [0, "rgba(216,216,216,1)"],
                [.5, "rgba(0,156,145,1)"],
                [.9, "rgba(0,156,145,1)"],
                [1, "rgba(0,156,145,1)"],
            ]
        )
    )

    fig.add_trace(
        go.Heatmap(
            z=third_dose_matrix,
            hoverongaps=False,
            showscale=False,
            ygap=3,
            xgap=3,
            colorscale=[
                [0, "rgba(216,216,216,1)"],
                [.5, "rgba(0,65,61,1)"],
                [.9, "rgba(0,65,61,1)"],
                [1, "rgba(0,65,61,1)"],
            ]
        )
    )

    fig.update_layout(
        width=400,
        height=400,
        **WAFFLE_LAYOUT
    )

    return fig.to_image(format="svg").decode()

