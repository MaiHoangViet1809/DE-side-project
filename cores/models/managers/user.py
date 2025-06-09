from abc import ABC, abstractmethod
from typing import Literal
from pandas import DataFrame


class UserManager(ABC):
    """
    Abstract base class for managing database users.

    Attributes:
        hook: A database connection hook for executing SQL commands.
    """
    def __init__(self, hook):
        """
        Initializes the UserManager.

        Args:
            hook: A database connection hook.
        """
        self.hook = hook

    @abstractmethod
    def create_login(self, user: str, password: str):
        """
        Abstract method to create a login.

        Args:
            user (str): The username.
            password (str): The password.
        """
        pass

    @abstractmethod
    def create_user(self, user: str):
        """
        Abstract method to create a user.

        Args:
            user (str): The username.
        """
        pass

    @abstractmethod
    def drop_user(self, user: str):
        """
        Abstract method to drop a user.

        Args:
            user (str): The username.
        """
        pass

    @abstractmethod
    def deactivate_user(self, user: str):
        """
        Abstract method to deactivate a user.

        Args:
            user (str): The username.
        """
        pass

    def grant_table_to_user(self, table_name: str, user_or_schema: str, permissions: list = ("SELECT",)):
        """
        Grants permissions on a table to a user or schema.

        Args:
            table_name (str): The table name.
            user_or_schema (str): The user or schema name.
            permissions (list): List of permissions to grant. Default is ["SELECT"].
        """
        self.hook.run_sql(f"GRANT {', '.join(permissions)} ON {table_name} TO [{user_or_schema}]", log_sql=True)

    def revoke_table_from_user(self, table_name: str, user_or_schema: str, permissions: list = ("SELECT",)):
        """
        Revokes permissions on a table from a user or schema.

        Args:
            table_name (str): The table name.
            user_or_schema (str): The user or schema name.
            permissions (list): List of permissions to revoke. Default is ["SELECT"].
        """
        self.hook.run_sql(f"REVOKE {', '.join(permissions)} ON {table_name} FROM [{user_or_schema}]", log_sql=True)


class MSSQLUserManager(UserManager):
    """
    A concrete implementation of UserManager for managing Microsoft SQL Server users.
    """

    @staticmethod
    def get_login_name(user: str):
        """
        Generates the login name for a user.

        Args:
            user (str): The username.

        Returns:
            str: The login name.
        """
        return f"{user}"

    def create_login(self, user: str, password: str,
                     default_database: str = "EDW",
                     check_expiration: Literal["ON", "OFF"] = "OFF",
                     check_policy: Literal["ON", "OFF"] = "OFF",
                     ):
        """
        Creates a login in the database.

        Args:
            user (str): The username.
            password (str): The password.
            default_database (str): The default database for the login. Default is "EDW".
            check_expiration (Literal["ON", "OFF"]): Whether to enforce password expiration. Default is "OFF".
            check_policy (Literal["ON", "OFF"]): Whether to enforce password policy. Default is "OFF".
        """
        # create login first
        self.hook.run_sql(f"""
                IF SUSER_ID(N'{self.get_login_name(user=user)}') IS NULL
                    CREATE LOGIN [{self.get_login_name(user=user)}] WITH PASSWORD='{password}', 
                    DEFAULT_DATABASE={default_database}, 
                    CHECK_EXPIRATION={check_expiration}, 
                    CHECK_POLICY={check_policy}
                """)

    def create_user(self, user: str, database: str = "EDW"):
        """
        Creates a user in the database.

        Args:
            user (str): The username.
            database (str): The database name. Default is "EDW".
        """
        # create user
        self.hook.run_sql(
            f"""
                USE {database}; IF USER_ID(N'{self.get_login_name(user=user)}') IS NULL CREATE USER [{user}] FOR LOGIN [{self.get_login_name(user=user)}];
            """,
            log_sql=True
        )

    def attach_schema_to_user(self, user, schema: str, permissions: list = ("SELECT",)):
        """
        Attaches a schema to a user and grants permissions.

        Args:
            user (str): The username.
            schema (str): The schema name.
            permissions (list): List of permissions to grant. Default is ["SELECT"].
        """
        # Set default schema (to query without specify schema name)
        self.hook.run_sql(f"ALTER USER [{user}] WITH DEFAULT_SCHEMA = {schema}", log_sql=True)
        # grant user SELECT, INSERT, UPDATE, DELETE on a schema
        self.hook.run_sql(f"GRANT {', '.join(permissions)} ON SCHEMA::{schema} TO [{user}]", log_sql=True)

    def kill_login_session(self, user: str):
        """
        Kills all sessions for a given login.

        Args:
            user (str): The username.
        """
        sql = f"""
        DECLARE @sql NVARCHAR(MAX) = '';
      SELECT @sql += 'KILL ' + CONVERT(VARCHAR(11), session_id) + ';' 
        FROM sys.dm_exec_sessions
       WHERE login_name = '{self.get_login_name(user)}';
       EXEC sys.sp_executesql @sql;
        """
        self.hook.run_sql(sql)

    def drop_user(self, user: str):
        """
        Drops a user and their associated login.

        Args:
            user (str): The username.
        """
        self.kill_login_session(user)
        self.hook.run_sql(f"DROP USER [{user}]")
        self.hook.run_sql(f"DROP LOGIN [{self.get_login_name(user=user)}]")

    def deactivate_user(self, user: str):
        """
        Deactivates a user.

        Args:
            user (str): The username.
        """
        self.kill_login_session(user)
        self.hook.run_sql(f"ALTER LOGIN [{self.get_login_name(user)}] DISABLE")

    def activate_user(self, user: str):
        """
        Activates a user.

        Args:
            user (str): The username.
        """
        self.kill_login_session(user)
        self.hook.run_sql(f"ALTER LOGIN [{self.get_login_name(user)}] ENABLE")

    def get_users(self) -> DataFrame:
        """
        Retrieves a list of users and their details.

        Returns:
            DataFrame: A DataFrame containing user details.
        """
        return self.hook.get_pandas_df("""
            select DISTINCT 
                    sp.name as login,
                    sp.type,
                    sp.type_desc as login_type,
                    sl.password_hash,
                    sp.create_date,
                    sp.modify_date,
                    case when sp.is_disabled = 1 then 'Disabled'
                        else 'Enabled' end as status,
                    sp.default_database_name,
                    dp.default_schema_name,
                    PERM.LIST_PERM
            from master.sys.server_principals sp
                    left join master.sys.sql_logins sl
                        on sp.principal_id = sl.principal_id
                    left join edw.sys.database_principals dp
                        on sp.name = dp.name
                        and sp.type = dp.type
                    LEFT JOIN (
                        SELECT SCHEMA_NAME(major_id) SCHEMA_NAME
                            ,USER_NAME(grantee_principal_id) USER_NAME
                            ,STRING_AGG(permission_name, ',') LIST_PERM
                        FROM sys.database_permissions AS PERM
                        JOIN sys.database_principals AS Prin
                            ON PERM.major_ID = Prin.principal_id
                        WHERE PERM.major_id = SCHEMA_ID('SELF_ANALYSIS')
                        AND PERM.state_desc = 'GRANT'
                        AND PERM.class_desc = 'SCHEMA'
                        GROUP BY SCHEMA_NAME(major_id)
                                ,USER_NAME(grantee_principal_id)
                    ) PERM 
                          ON PERM.USER_NAME = sp.name
                         AND PERM.SCHEMA_NAME = dp.default_schema_name
            where sp.type not in ('G', 'R', 'C', 'U')
                and sp.name not like '##MS_%##'
            order by sp.name
        """)

    def check_password(self, user: str, password: str):
        """
        Checks if the given password matches the user's password.

        Args:
            user (str): The username.
            password (str): The password to verify.

        Returns:
            bool: True if the password matches, False otherwise.
        """
        data = self.hook.get_pandas_df(f"""
            SELECT PWDCOMPARE(N'{password}',password_hash) RESULT
              FROM sys.sql_logins s
             WHERE s.name = '{user}'
        """)["RESULT"].to_list()
        if data:
            return data[0] == 1
        else:
            return False

    def change_password(self, user: str, new_password: str):
        """
        Changes the password for a user.

        Args:
            user (str): The username.
            new_password (str): The new password.
        """
        self.hook.run_sql(f"""ALTER LOGIN [{user}] WITH PASSWORD = '{new_password}'""")

    def revoke_all_tables_from_user(self, user: str, debug=False):
        """
        Revokes all table permissions from a user.

        Args:
            user (str): The username.
            debug (bool): If True, logs the SQL commands. Default is False.
        """
        self.hook.run_sql(f"""
            DECLARE @UserName NVARCHAR(128) = '{user}'
            DECLARE @sql NVARCHAR(MAX) = ''
            
            -- Generate REVOKE statements for each table the user has permissions on
            SELECT @sql += 'REVOKE ALL ON [' + s.name + '].[' + t.name + '] FROM [' + @UserName + '];' + CHAR(13)
            FROM sys.tables AS t
            INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
            INNER JOIN sys.database_permissions AS p ON t.object_id = p.major_id
            WHERE p.grantee_principal_id = USER_ID(@UserName)
            
            -- Execute the generated REVOKE statements
            EXEC sp_executesql @sql
        """, log_sql=debug)

    def revoke_all_views_from_user(self, user: str, debug=False):
        """
        Revokes all table permissions from a user.

        Args:
            user (str): The username.
            debug (bool): If True, logs the SQL commands. Default is False.
        """
        self.hook.run_sql(f"""
            DECLARE @UserName NVARCHAR(128) = '{user}'
            DECLARE @sql NVARCHAR(MAX) = ''

            -- Generate REVOKE statements for each table the user has permissions on
            SELECT @sql += 'REVOKE ALL ON [' + s.name + '].[' + t.name + '] FROM [' + @UserName + '];' + CHAR(13)
            FROM sys.views AS t
            INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
            INNER JOIN sys.database_permissions AS p ON t.object_id = p.major_id
            WHERE p.grantee_principal_id = USER_ID(@UserName)

            -- Execute the generated REVOKE statements
            EXEC sp_executesql @sql
        """, log_sql=debug)


    def grant_table_to_user(self, table_name: str | list, user_or_schema: str, permissions: list = ("SELECT",)):
        """
        Grants permissions on one or more tables to a user or schema.

        Args:
            table_name (str | list): The table name or list of table names.
            user_or_schema (str): The user or schema name.
            permissions (list): List of permissions to grant. Default is ["SELECT"].
        """
        sql = ""
        if isinstance(table_name, str):
            sql = f"GRANT {', '.join(permissions)} ON {table_name} TO [{user_or_schema}]"
        elif isinstance(table_name, list):
            list_sql = []
            for m in table_name:
                list_sql += [f"GRANT {', '.join(permissions)} ON {m} TO [{user_or_schema}];"]
            sql = "\n".join(list_sql)

        self.hook.run_sql(sql, log_sql=True)


if __name__ == "__main__":
    # unit test
    ...
