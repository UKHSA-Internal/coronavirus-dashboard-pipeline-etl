#!/usr/bin python3

# Imports
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Python:
from datetime import datetime
from os import getenv
from uuid import uuid4, UUID as PyUUID

# 3rd party:
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.pool import NullPool
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy import (
    Column, DATE, VARCHAR, BOOLEAN, TEXT, Enum,
    PrimaryKeyConstraint, TIMESTAMP, UniqueConstraint,
    ForeignKey, INTEGER, CHAR, TypeDecorator
)
from sqlalchemy.dialects.postgresql.json import JSONB
from sqlalchemy.dialects.postgresql import UUID as PostgresUUID, NUMERIC

# Internal:

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

__all__ = [
    'ProcessedFile',
    'ReleaseReference',
    'MainData',
    'AreaReference',
    'MetricReference',
    'ReleaseCategory',
    'Session',
    'DB_INSERT_MAX_ROWS',
    'Tag',
    'MetricTag',
    'Page',
    'Announcement',
    'MetricAsset',
    'MetricAssetToMetric',
    'PrivateReport',
    'Despatch',
    'DespatchToRelease',
    'ReportRecipient'
]

DB_INSERT_MAX_ROWS = 8_000

DB_URL = getenv("DB_URL")

engine = create_engine(
    DB_URL,
    poolclass=NullPool,
    connect_args={'charset':'utf8'},
)

Session = sessionmaker(bind=engine, autocommit=True)


release_categories = Enum(
    'MAIN',
    'MSOA',
    'VACCINATION',
    'AGE DEMOGRAPHICS: CASE - EVENT DATE',
    'AGE-DEMOGRAPHICS: DEATH28DAYS - EVENT DATE',
    'AGE-DEMOGRAPHICS: VACCINATION - EVENT DATE',
    'MSOA: VACCINATION - EVENT DATE',
    'POSITIVITY & PEOPLE TESTED',
    'HEALTHCARE',
    'TESTING: MAIN',
    'CASES: MAIN',
    create_type=False
)


@event.listens_for(engine, "before_cursor_execute")
def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
    if executemany:
        try:
            cursor.fast_executemany = True
        except Exception:
            pass


base = declarative_base()


class UUID(TypeDecorator):
    """Platform-independent GUID type.

     Uses PostgreSQL's UUID type, otherwise uses
     CHAR(32), storing as stringified hex values.

     """
    impl = CHAR
    cache_ok = True

    def load_dialect_impl(self, dialect):
        if dialect.name == 'postgresql':
            return dialect.type_descriptor(PostgresUUID())
        else:
            return dialect.type_descriptor(CHAR(32))

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        elif dialect.name == 'postgresql':
            return str(value)
        else:
            if not isinstance(value, PyUUID):
                return "%.32x" % PyUUID(value).int
            else:
                # hexstring
                return "%.32x" % value.int

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        else:
            if not isinstance(value, PyUUID):
                value = PyUUID(value)
            return value


class ProcessedFile(base):
    __tablename__ = "processed_file"

    id = Column("id", UUID(), primary_key=True, default=uuid4)
    file_path = Column("file_path", VARCHAR(255), nullable=False, unique=True)
    type = Column("type", release_categories, nullable=False)
    release_id = Column("release_id", INTEGER(), nullable=True)
    timestamp = Column("timestamp", TIMESTAMP(timezone=True), default=datetime.utcnow)
    process_id = Column("process_id", VARCHAR(255), nullable=False, unique=True)

    __table_args__ = (
        {'schema': 'covid19'},
    )


class ReleaseReference(base):
    __tablename__ = "release_reference"

    id = Column("id", INTEGER(), primary_key=True, autoincrement=True)
    timestamp = Column("timestamp", TIMESTAMP(), unique=True)
    released = Column("released", BOOLEAN(), nullable=False, default=False)

    __table_args__ = {'schema': 'covid19'}


class Despatch(base):
    __tablename__ = "despatch"

    id = Column("id", INTEGER(), primary_key=True, autoincrement=True)
    timestamp = Column("timestamp", TIMESTAMP(), unique=True, nullable=False)

    __table_args__ = {'schema': 'covid19'}


class DespatchToRelease(base):
    __tablename__ = "despatch_to_release"

    id = Column("id", INTEGER(), primary_key=True, autoincrement=True)
    release_id = Column(
        "release_id",
        INTEGER(),
        ForeignKey(
            ReleaseReference.id,
            ondelete="CASCADE",
            deferrable=False
        ),
        nullable=False
    )
    despatch_id = Column(
        "despatch_id",
        INTEGER(),
        ForeignKey(
            Despatch.id,
            ondelete="CASCADE",
            deferrable=False
        ),
        nullable=False
    )

    __table_args__ = (
        UniqueConstraint(release_id, despatch_id),
        {'schema': 'covid19'},
    )


class MainData(base):
    __tablename__ = "time_series"

    hash = Column("hash", VARCHAR(24), nullable=False, unique=True)
    release_id = Column(
        "release_id",
        ForeignKey("covid19.release_reference.id")
    )
    area_id = Column(
        "area_id",
        ForeignKey("covid19.area_reference.id")
    )
    metric_id = Column(
        "metric_id",
        ForeignKey("covid19.metric_reference.id")
    )
    partition_id = Column("partition_id", VARCHAR(26), nullable=False)
    date = Column("date", DATE(), nullable=False)
    payload = Column("payload", JSONB(), nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint(hash, area_id, metric_id, release_id, partition_id),
        UniqueConstraint(hash, partition_id),
        {'schema': 'covid19'}
    )


class AreaReference(base):
    __tablename__ = "area_reference"

    id = Column("id", INTEGER(), unique=True, autoincrement=True)
    area_type = Column("area_type", VARCHAR(15), nullable=False)
    area_code = Column("area_code", VARCHAR(12), nullable=False)
    area_name = Column("area_name", VARCHAR(120), nullable=False)
    unique_ref = Column("unique_ref", VARCHAR(26), nullable=False, unique=True)

    __table_args__ = (
        PrimaryKeyConstraint(area_type, area_code),
        UniqueConstraint(area_type, area_code),
        {'schema': 'covid19'}
    )


class MetricReference(base):
    __tablename__ = "metric_reference"

    id = Column("id", INTEGER(), primary_key=True, autoincrement=True)
    metric = Column("metric", VARCHAR(120), nullable=False, unique=True)
    released = Column("released", BOOLEAN(), nullable=False, default=False)
    metric_name = Column("metric_name", VARCHAR(150), nullable=True)
    source_metric = Column("source_metric", BOOLEAN(), nullable=False, default=False)
    category = Column("category", UUID(), nullable=True)

    __table_args__ = {'schema': 'covid19'}


class Tag(base):
    __tablename__ = "tag"

    id = Column("id", UUID(), nullable=False, primary_key=True)
    association = Column("association", VARCHAR(30), nullable=False)
    tag = Column("tag", VARCHAR(40), nullable=False)

    __table_args__ = {'schema': 'covid19'}


class MetricTag(base):
    __tablename__ = "metric_tag"

    id = Column("id", UUID(), nullable=False)
    metric_id = Column("metric_id", VARCHAR(120), nullable=False)
    tag_id = Column("tag_id", UUID(), nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint(id, metric_id, tag_id),
        {'schema': 'covid19'}
    )


class Announcement(base):
    __tablename__ = "announcement"

    id = Column("id", UUID(), nullable=False, primary_key=True)
    heading = Column("heading", VARCHAR(120), nullable=True)
    appear_by_update = Column("appear_by_update", DATE(), nullable=False)
    disappear_by_update = Column("disappear_by_update", DATE(), nullable=False)
    date = Column("date", DATE(), nullable=True)
    body = Column("body", TEXT(), nullable=False)
    type = Column("type", VARCHAR(10), nullable=False, default='BANNER')
    released = Column("released", BOOLEAN(), nullable=False, default=False)
    details = Column("details", TEXT(), nullable=True)

    __table_args__ = {'schema': 'covid19'}


class MetricAsset(base):
    __tablename__ = "metric_asset"

    id = Column("id", UUID(), nullable=False, primary_key=True)
    label = Column("label", VARCHAR(255), nullable=False)
    body = Column("body", TEXT(), nullable=False)
    last_modified = Column("last_modified", TIMESTAMP(), nullable=False)
    released = Column("released", BOOLEAN(), nullable=False, default=False)

    __table_args__ = {'schema': 'covid19'}


class MetricAssetToMetric(base):
    __tablename__ = "metric_asset_to_metric"

    id = Column("id", UUID(), nullable=False, primary_key=True)
    asset_type = Column("asset_type", VARCHAR(50), nullable=False)
    asset_id = Column("asset_id", UUID(), nullable=False)
    metric_id = Column("metric_id", VARCHAR(120), nullable=False)
    order = Column("order", INTEGER(), nullable=True)

    __table_args__ = {'schema': 'covid19'}


class Page(base):
    __tablename__ = "page"

    id = Column("id", UUID(), nullable=False, primary_key=True)
    title = Column("title", VARCHAR(120), nullable=False)
    uri = Column("uri", VARCHAR(150), nullable=False)
    data_category = Column("data_category", BOOLEAN(), nullable=True, default=False)

    __table_args__ = {'schema': 'covid19'}


class ReleaseCategory(base):
    __tablename__ = "release_category"

    release_id = Column(
        "release_id",
        INTEGER(),
        ForeignKey(
            'covid19.release_reference.id',
            ondelete="CASCADE"
        ),
        nullable=False
    )
    process_name = Column(
        "process_name",
        release_categories,
        nullable=False
    )

    release = relationship('ReleaseReference', backref='categories')

    __table_args__ = (
        PrimaryKeyConstraint(release_id, process_name),
        {'schema': 'covid19'}
    )


class PrivateReport(base):
    __tablename__ = 'private_report'

    id = Column(
        "id",
        UUID(),
        unique=True,
        nullable=False,
        default=uuid4
    )
    slug_id = Column(
        "slug_id",
        VARCHAR(50),
        nullable=False
    )
    release_id = Column(
        "release_id",
        ForeignKey("covid19.release_reference.id"),
        nullable=False
    )
    metric = Column(
        "metric",
        ForeignKey("covid19.metric_reference.metric"),
        nullable=False
    )
    area_id = Column(
        "area_id",
        ForeignKey("covid19.area_reference.id"),
        nullable=False
    )
    date = Column(
        "date",
        DATE(),
        nullable=False
    )
    value = Column(
        "value",
        NUMERIC(),
        nullable=True
    )

    __table_args__ = (
        PrimaryKeyConstraint(id, slug_id),
        {'schema': 'covid19'}
    )


class ReportRecipient(base):
    __tablename__ = "report_recipients"

    id = Column("id", UUID(), unique=True, nullable=False, primary_key=True, default=uuid4)
    recipient = Column("recipient", VARCHAR(255), nullable=False, unique=True)
    note = Column("note", TEXT(), nullable=True)
    date_added = Column("date_added", TIMESTAMP(timezone=False), nullable=False, default=datetime.utcnow)
    created_by = Column("created_by", INTEGER(), nullable=False)
    approved_by = Column("approved_by", INTEGER(), nullable=True)
    deactivated = Column("deactivated", BOOLEAN(), nullable=False, default=False)

    __table_args__ = {'schema': 'covid19'}
