#!/usr/bin python3

from io import StringIO
from functools import lru_cache, partial
from datetime import datetime
from json import dumps

from pandas import read_csv, concat, DataFrame

from storage import StorageClient
from msoa_etl_db.processor import dry_run


@lru_cache()
def get_msoa_poplation():
    with StorageClient("pipeline", "assets/msoa_pop2019.csv") as client:
        population_io = StringIO(client.download().readall().decode())

    result = (
        read_csv(population_io)
        .rename(columns={"MSOA11CD": "areaCode", "Pop2019": "population"})
        .set_index(["areaCode"])
        .to_dict()
    )

    return result["population"]


def process_data(area_code: str, data_path: str) -> DataFrame:
    population = get_msoa_poplation()

    payload = dumps({
        "data_path": {
            "container": "rawsoadata",
            "path": data_path
        },
        "area_code": area_code,
        "area_type": "msoa",
        "metric": "newCasesBySpecimenDate",
        "partition_id": "N/A",
        "population": population[area_code],
        "area_id": -1,
        "metric_id": -1,
        "release_id": -1,
        "timestamp": datetime.utcnow().isoformat()
    })

    return dry_run(payload)


def local_test(data_path, msoa_codes):
    func = partial(process_data, data_path=data_path)
    data = concat(map(func, msoa_codes))

    return data


if __name__ == '__main__':
    codes = [
        "E02003377",
        "E02000977",
        "E02003539",
        "E02003106",
        "E02003984",
        "E02003135",
    ]

    result = local_test("daily_msoa_cases_202103101048.csv", codes)
    result.to_csv("request_data.csv")
