from cores.hooks.mssql import SQLServerHook
from cores.utils.configs import FrameworkConfigs as cfg

sql_create_table = """
CREATE TABLE CONFIG.TBL_REPORT_USAGE_LOG (
    LOG_DTTM DATETIME DEFAULT GETDATE(),
    LOG_GUI  VARCHAR(100),
    REPORT_ID VARCHAR(100),
    LOGIN_USER VARCHAR(100),
    CLIENT_NAME VARCHAR(1000),
    START_DTTM DATETIME,
    END_DTTM DATETIME,
    QUERY_STATUS VARCHAR(100),
    DURATION BIGINT,
    SQL_TEXT VARCHAR(8000),
)
"""

sql = f"""
CREATE OR ALTER PROCEDURE DM.QUERY_DYNAMIC(@sql NVARCHAR(4000), @report_id VARCHAR(50))
AS
BEGIN
    /*
    {sql_create_table}
    */
    DECLARE @LOGIN_USER VARCHAR(100) = ORIGINAL_LOGIN();
    DECLARE @CLIENT_NAME VARCHAR(1000) = HOST_NAME();
    DECLARE @GUID VARCHAR(100) = NEWID();

    INSERT INTO CONFIG.TBL_REPORT_USAGE_LOG (LOG_GUI, REPORT_ID, LOGIN_USER, CLIENT_NAME, START_DTTM, QUERY_STATUS, SQL_TEXT)
    VALUES (
        @GUID, @report_id, @LOGIN_USER, @CLIENT_NAME, getdate(), 'RUNNING', @sql
    )
    EXEC sp_executesql @sql;
    DECLARE @END_DTTM DATETIME = GETDATE();
    UPDATE CONFIG.TBL_REPORT_USAGE_LOG
       SET END_DTTM = @END_DTTM,
           QUERY_STATUS = 'FINISH',
           DURATION = DATEDIFF(SECOND, START_DTTM, @END_DTTM)
     WHERE LOG_GUI = @GUID
    ;
    RETURN;
END
"""

if __name__ == '__main__':
    hook = SQLServerHook(cfg.Hooks.MSSQL.new)

    # create log table
    hook.run_sql(sql=sql_create_table)

    # create procedure
    hook.run_sql(sql=sql)

