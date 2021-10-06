#!/usr/bin python3


def prepare_for_dispatch(*json_data):
    """
    Prepares and structures the final datasets for dispatch
    to the service.

    json_data
    ---------
    str, str, ...
        JSON data as individual pieces of string.

    Returns
    -------
    str
        Formatted data as JSON.

    Example
    -------
        prepare_for_dispatch(dataset_a, dataset_b, dataset_c)
    """
    from functools import reduce
    from itertools import chain
    from json import loads, dumps

    # Keys to be excluded from the process.
    excluded = ['disclaimer', 'fileUpdatedAt']

    # Parsing JSON strings
    data = list(map(loads, json_data))

    # Extract first-level keys from the data
    keys = reduce(lambda x, y: [*x, *y], map(dict.keys, data))

    # Create empty dict of dicts using the keys
    output = {
        key: dict()
        for key in set(keys)
        if key not in excluded
    }

    # Extracts keys and values from each item [generator].
    # Note: The output is a nested array of dict items.
    data_items = map(dict.items, data)

    # Creates a 1D array of dict items [generator].
    chained_data = chain.from_iterable(data_items)

    # Only includes the data keys [generator].
    filtered_data = filter(lambda item: item[0] in output, chained_data)

    # Re-structuring the data
    for area_type, area_data in filtered_data:
        for area_code, area_params in area_data.items():
            try:
                output[area_type][area_code].update(area_params)
            except KeyError:
                # If the `area_code` doesn't existing, create it
                # and assign the current `area_params`.
                output[area_type][area_code] = area_params

    if "uk" in output:
        output["overview"] = output.pop("uk")

    json_data = dumps(output, separators=(",", ":"))

    return json_data
