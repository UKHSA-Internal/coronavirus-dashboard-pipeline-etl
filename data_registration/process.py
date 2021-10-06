#!/usr/bin python3

"""
<Description of the programme>

Author:        Pouria Hadjibagheri <pouria.hadjibagheri@phe.gov.uk>
Created:       30 Sep 2021
License:       MIT
Contributors:  Pouria Hadjibagheri
"""

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
import logging
from datetime import datetime

# 3rd party:
from sqlalchemy.exc import IntegrityError
from sqlalchemy import select, and_

# Internal: 
try:
    from __app__.db_tables.covid19 import Session, ProcessedFile
    from __app__.utilities.data_files import category_label, parse_filepath
except ImportError:
    from db_tables.covid19 import Session, ProcessedFile
    from utilities.data_files import category_label, parse_filepath

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Header
__author__ = "Pouria Hadjibagheri"
__copyright__ = "Copyright (c) 2021, Public Health England"
__license__ = "MIT"
__version__ = "0.0.1"
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


__all__ = [
    'register_file',
    'set_file_releaseid'
]


def register_file(filepath: str, timestamp: datetime, instance_id: str, release_id=None) -> True:
    parsed_filepath = parse_filepath(filepath)

    processed_file = ProcessedFile(
        file_path=filepath,
        type=category_label(parsed_filepath),
        timestamp=timestamp,
        release_id=release_id,
        process_id=instance_id
    )

    session = Session()
    try:
        session.add(processed_file)
        session.flush()

    except IntegrityError as err:
        session.rollback()

        query = session.execute(
            select([
                ProcessedFile.id,
            ])
            .where(
                and_(
                    ProcessedFile.file_path == filepath,
                    ProcessedFile.process_id == instance_id
                )
            )
        )
        result = query.fetchone()

        if result is not None:
            return True

        logging.info("Record already exists.")
        raise err

    except Exception as err:
        session.rollback()
        raise err

    finally:
        session.close()

    return True


def set_file_releaseid(filepath: str, release_id: int) -> True:
    session = Session()

    try:
        session.begin()

        session.query(
            ProcessedFile
        ).filter(
            ProcessedFile.file_path == filepath
        ).update({
            "release_id": release_id
        })

        session.commit()
        session.flush()
    except IntegrityError as err:
        session.rollback()
        logging.info("Record already exists.")
        raise err
    except Exception as err:
        session.rollback()
        raise err
    finally:
        session.close()

    return True


if __name__ == "__main__":
    set_file_releaseid(
        "data_20210930135.json",
        release_id=63942
    )
