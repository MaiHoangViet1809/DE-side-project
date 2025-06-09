from sqlalchemy import create_engine
from pandas import DataFrame

from cores.utils.providers import ProvideHookFramework


class BehaviorStandardDataModel:
    """
    A base class defining standard behaviors for SQLAlchemy ORM models, such as table creation,
    record insertion, and data retrieval.

    Attributes:
        __tablename__ (str): The name of the database table.
        __table_args__ (list | dict): Additional table arguments, including schema configuration.
    """

    __tablename__ : str
    __table_args__ : list | dict

    @classmethod
    @ProvideHookFramework # noqa
    def create_log_table(cls, hook=None, force_recreate=False):
        """
        Creates the table in the database if it does not already exist. Optionally, force recreates the table.

        Args:
            hook: A database connection hook.
            force_recreate (bool): Whether to force recreate the table if it already exists. Default is False.

        Raises:
            UserPromptError: Prompts for confirmation before recreating the table.

        Process:
            - Checks if the table exists in the database.
            - If not, creates the table.
            - If `force_recreate` is True, prompts the user for confirmation and recreates the table.
        """
        uri = hook.get_connection_uri()
        engine = create_engine(uri)

        schema = cls.__table_args__.get("schema") if isinstance(cls.__table_args__, dict) else cls.__table_args__[-1].get("schema")
        check_log_backup = hook.is_table_exists(table_name=cls.__tablename__, schema_name=schema)

        if not check_log_backup:
            cls.__table__.create(engine)  # noqa
        elif force_recreate:
            if input("Are you sure you want to force recreate the table log ? (Y/N)") == "Y":
                cls.__table__.drop(engine)  # noqa
                cls.__table__.create(engine)  # noqa

    @classmethod
    @ProvideHookFramework # noqa
    def save_record(cls, hook=None, **kwargs):
        """
        Saves or updates a record in the database.

        Args:
            hook: A database connection hook.
            **kwargs: Column values for the record to save or update.

        Process:
            - Uses `session.merge()` to insert or update the record based on primary key conflicts.
            - Commits the transaction after the operation.
        """
        with hook as session:
            session.merge(cls(**kwargs))
            session.commit()

        # from sqlalchemy import insert, update, delete
        # from sqlalchemy.inspection import inspect

        # primary_keys = [key.name for key in inspect(cls).primary_key]
        # data_update = {k: v for k, v in kwargs.items() if k not in primary_keys}

        # stmt = insert(cls).values(**kwargs)
        # stmt = stmt.on_conflict_do_update(index_elements=primary_keys, set_=data_update)

        # hook.run_under_session(
        #     statement=stmt,
        #     # params=[kwargs,],
        #     fetch_data=False,
        # )

    @classmethod
    @ProvideHookFramework
    def get_records(cls, conditions = None, hook = None , **kwargs) -> DataFrame:
        """
        Retrieves records from the database based on the given conditions.

        Args:
            conditions: SQLAlchemy conditions for filtering records. Default is None.
            hook: A database connection hook.
            **kwargs: Additional arguments for filtering or customization.

        Returns:
            DataFrame: A Pandas DataFrame containing the retrieved records.

        Process:
            - Constructs a `SELECT` statement for the table.
            - Applies conditions, if provided.
            - Executes the query and loads the results into a Pandas DataFrame.
            - Resets the index of the DataFrame before returning it.
        """
        from sqlalchemy.sql.expression import select
        from pandas import read_sql

        stmt = select(cls)
        if conditions is not None:
            stmt = stmt.where(conditions)

        with hook as session:
            df = read_sql(stmt, con=session.bind)

        return df.reset_index(drop=True)
