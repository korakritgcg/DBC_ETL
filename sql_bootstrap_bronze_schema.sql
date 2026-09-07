USE [DBC_PRD];
GO

IF DATABASE_PRINCIPAL_ID(N'sql_gcgapidb') IS NULL
BEGIN
    IF SUSER_ID(N'sql_gcgapidb') IS NULL
    BEGIN
        THROW 50000, 'Login sql_gcgapidb does not exist on this SQL Server.', 1;
    END

    CREATE USER [sql_gcgapidb] FOR LOGIN [sql_gcgapidb];
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.schemas
    WHERE name = N'bronze'
)
BEGIN
    EXEC(N'CREATE SCHEMA [bronze] AUTHORIZATION [dbo]');
END
GO

ALTER AUTHORIZATION ON SCHEMA::[bronze] TO [dbo];
GO

GRANT ALTER, SELECT, INSERT, UPDATE, DELETE ON SCHEMA::[bronze] TO [sql_gcgapidb];
GRANT CONTROL ON SCHEMA::[bronze] TO [sql_gcgapidb];
GRANT CREATE TABLE TO [sql_gcgapidb];
GO

EXECUTE AS USER = N'sql_gcgapidb';
SELECT
    USER_NAME() AS database_user,
    HAS_PERMS_BY_NAME(N'bronze', N'SCHEMA', N'ALTER') AS can_alter_bronze_schema,
    HAS_PERMS_BY_NAME(DB_NAME(), N'DATABASE', N'CREATE TABLE') AS can_create_table;
REVERT;
GO
