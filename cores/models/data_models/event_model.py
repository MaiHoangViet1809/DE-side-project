from enum import Enum

from sqlalchemy import Column, DATETIME, VARCHAR, DATE
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

from cores.models.data_models.data_model_creation import BehaviorStandardDataModel


# declarative base class
_Base = declarative_base()


class TableEvent(_Base, BehaviorStandardDataModel):
    """
    A SQLAlchemy ORM class representing a table for logging framework events.

    Inherits from:
        - `_Base`: The declarative base for SQLAlchemy ORM models.
        - `BehaviorStandardDataModel`: Provides standard data model behavior.

    Attributes:
        __tablename__ (str): The name of the table.
        __table_args__ (dict): Table configuration, including schema.

        LOG_DTTM (Column): The timestamp of the log entry. Defaults to the current time. (Primary key)
        BATCH_JOB_ID (Column): The ID of the batch job. (Primary key)
        RUN_DATE (Column): The execution run date. Indexed and a primary key.
        ENVIRONMENT_NAME (Column): The environment name. Indexed and a primary key.
        TABLE_NAME (Column): The table name. Indexed and a primary key.
        EVENT_NAME (Column): The name of the event. Cannot be null.
        EVENT_STAGE (Column): The stage of the event.
        EVENT_DATA (Column): Additional data related to the event.
        START_DTTM (Column): The start timestamp of the event. Cannot be null.
        END_DTTM (Column): The end timestamp of the event.
    """

    __tablename__  = "TBL_FRAMEWORK_EVENT"
    __table_args__ = {"schema": "CONFIG"}

    LOG_DTTM     = Column(DATETIME, server_default=func.now(), primary_key=True)
    BATCH_JOB_ID = Column(VARCHAR(100), primary_key=True)

    RUN_DATE         = Column(DATE, index=True, primary_key=True)
    ENVIRONMENT_NAME = Column(VARCHAR(100), index=True, primary_key=True)
    TABLE_NAME       = Column(VARCHAR(100), index=True, primary_key=True)

    EVENT_NAME   = Column(VARCHAR(100), nullable=False)
    EVENT_STAGE  = Column(VARCHAR(20))
    EVENT_DATA   = Column(VARCHAR(5000))

    START_DTTM   = Column(DATETIME, nullable=False)
    END_DTTM     = Column(DATETIME)


class EventType(Enum):
    """
    Enum for defining different types of events.

    Attributes:
        refresh_data (str): Represents a data refresh event.
    """
    refresh_data = "REFRESH-DATA"

