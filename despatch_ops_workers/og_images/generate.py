#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
from os import path
from io import BytesIO
from datetime import datetime
from tempfile import gettempdir

# 3rd party:
from jinja2 import FileSystemLoader, Environment
from svglib.svglib import svg2rlg
from reportlab.graphics import renderPM
from sqlalchemy import text

# Internal:
try:
    from .queries import MAIN
    from __app__.storage import StorageClient
    from __app__.db_tables.covid19 import Session
except ImportError:
    from storage import StorageClient
    from despatch_ops_workers.og_images.queries import MAIN
    from db_tables.covid19 import Session

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'generate_og_images'
]


curr_dir = path.split(path.abspath(__file__))[0]
file_loader = FileSystemLoader(path.join(curr_dir, "templates"))
env = Environment(loader=file_loader)

temp_svg_path = path.join(gettempdir(), "temp.svg")

structure = {
    "newCasesByPublishDate": "cases",
    "newDeaths28DaysByPublishDate": "deaths",
    "cumPeopleVaccinatedSecondDoseByPublishDate": "vaccination",
    "newAdmissions": "admissions",
}

storage_kws = dict(
    container="downloads",
    content_type="image/png",
    cache_control="no-cache, max-age=0",
    content_language=None,
    compressed=False
)


def store_png(filename, svg_image):
    with open(temp_svg_path, "w") as tmp_file:
        print(svg_image, file=tmp_file)

    drawing = svg2rlg(temp_svg_path)
    png_img = BytesIO()
    renderPM.drawToFile(drawing, png_img, fmt="PNG")
    png_img.seek(0)
    date = datetime.now().strftime("%Y%m%d")
    name = f"{filename}_{date}"

    with StorageClient(path=f"og-images/{name}.png", **storage_kws) as cli:
        cli.upload(png_img.read())


def get_data(query, metric):
    session = Session()
    conn = session.connection()
    try:
        resp = conn.execute(
            text(query),
            metric=metric
        )
        raw_data = resp.fetchone()
    except Exception as err:
        session.rollback()
        raise err
    finally:
        session.close()

    data = {
        f'{metric}Date': f'{raw_data[1]:%-d %b}',
        metric: format(int(raw_data[2]), ",d")
    }

    return data


def create_asset(timestamp):
    data_ts = datetime.fromisoformat(timestamp)
    query = MAIN.format(date=f"{data_ts:%Y_%-m_%-d}")

    data = dict()
    for metric in structure:
        item = get_data(query, metric)
        data.update(item)

    template = env.get_template("summary.svg")

    svg_image = template.render(
        **data,
        timestamp=f"{data_ts:%-d %b %Y}"
    )

    store_png("og-summary", svg_image)


def generate_og_images(payload):
    timestamp = payload["timestamp"]

    create_asset(timestamp)

    return f"DONE: OG images {timestamp}"


if __name__ == "__main__":
    generate_og_images({"timestamp": datetime.utcnow().isoformat()})
