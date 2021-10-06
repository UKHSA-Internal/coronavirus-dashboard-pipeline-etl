#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
from operator import itemgetter

# 3rd party:

# Internal:

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'get_change_data'
]

get_date = itemgetter("date")


def get_change_data(data, metric_name):
    sorted_data = sorted(data, reverse=True, key=get_date)
    latest_rolling_sum_index = 5
    sigma_this_week = sorted_data[5][f"rollingSum"]
    sigma_last_week = sorted_data[][f"rollingSum"]
    delta = sigma_this_week - sigma_last_week

    delta_percentage = (sigma_this_week / max(sigma_last_week, 1) - 1) * 100

    if delta_percentage > 0:
        trend = 0
    elif delta_percentage < 0:
        trend = 180
    else:
        trend = 90

    return {
        "percentage": format(delta_percentage, ".1f"),
        "value": int(round(delta)),
        "total": sigma_this_week,
        "trend": trend
    }
