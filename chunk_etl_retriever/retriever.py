#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
from tempfile import TemporaryFile
from typing import Union
from datetime import datetime

# 3rd party:
from pandas import read_parquet, DataFrame
from azure.storage.blob import StandardBlobTier

# Internal: 
try:
    from __app__.storage import StorageClient
except ImportError:
    from storage import StorageClient

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'main'
]


def get_data(path: str, fp):
    with StorageClient(container="rawdbdata", path=path) as cli:
        cli.download().readinto(fp)

    fp.seek(0)


def store_data():
    kws = dict(
        container="pipeline",
        content_type="application/octet-stream",
        cache_control="no-cache, max-age=0, must-revalidate",
        compressed=False,
        tier='Cool'
    )

    client = StorageClient(**kws)
    container = client.get_container()
    content_settings = getattr(client, '_content_settings')

    def upload(data: DataFrame, category: str, subcategory: Union[str, None], date: str):

        area_type = data.iloc[0].areaType
        area_code = data.iloc[0].areaCode

        if subcategory:
            path = f"etl/{category}/{subcategory}/{date}/{area_type}_{area_code}.ft"
        else:
            path = f"etl/{category}/{date}/{area_type}_{area_code}.ft"

        with TemporaryFile() as fp:
            _ = (
                data
                .sort_values(["areaType", "areaCode", "date"], ascending=[True, True, False])
                .dropna(how='all', axis=1)
                .reset_index(drop=True)
                .to_feather(fp)
            )
            fp.seek(0)

            container.upload_blob(
                data=fp,
                name=path,
                content_settings=content_settings,
                overwrite=True,
                standard_blob_tier=StandardBlobTier.Cool,
                timeout=60,
                max_concurrency=10
            )

        response = {
            "path": path,
            "area_type": area_type,
            "area_code": area_code,
            "category": category,
            "subcategory": subcategory,
            "date": date
        }

        return response

    return upload


def main(payload):
    file_path = payload["path"]
    date = payload["date"]
    category = payload["category"]
    subcategory = payload.get("subcategory")

    with TemporaryFile() as fp:
        get_data(file_path, fp)
        df = read_parquet(fp)

    df.date = df.date.map(lambda x: x.strftime("%Y-%m-%d"))

    store_fn = store_data()
    paths = (
        df
        .groupby(["areaType", "areaCode"])
        .apply(
            store_fn,
            category=category,
            subcategory=subcategory,
            date=date,
        )
        .values
        .tolist()
    )

    return paths


if __name__ == "__main__":
    main({
        "path": "2021-05-21/vaccinations-by-vaccination-date_age-demographics_202105211607.parquet",
        "date": "2021-05-21",
        "category": "vaccination-by-vaccination-date",
        "subcategory": "age-demographics"
    })
