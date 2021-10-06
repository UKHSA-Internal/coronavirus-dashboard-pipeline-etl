#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
from operator import itemgetter

# 3rd party:

# Internal:
try:
    from .data import get_data
    from .utils import get_change_data
    from .visualisation import plot_thumbnail
except ImportError:
    from headline_graphs.data import get_data
    from headline_graphs.utils import get_change_data
    from headline_graphs.visualisation import plot_thumbnail

try:
    from __app__.utilities.latest_data import get_published_timestamp
    from __app__.constants.website import headline_metrics
except ImportError:
    from utilities.latest_data import get_published_timestamp
    from constants.website import headline_metrics, vaccinations

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'generate_graphs'
]


AREA_NAME = "United Kingdom"

get_metric_name = itemgetter("metric")


def generate_graphs():
    timestamp = get_published_timestamp(raw=True)

    for metric_name in map(get_metric_name, headline_metrics.values()):
        data = get_data(timestamp, AREA_NAME, metric_name)
        change = get_change_data(data, metric_name)
        image = plot_thumbnail(data, change, metric_name)

        with open(f"result/{metric_name}.svg", "w") as pointer:
            pointer.write(image)


if __name__ == '__main__':
    generate_graphs()
