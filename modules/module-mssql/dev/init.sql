-- Create database (idempotent)
IF DB_ID('$(DATABASE)') IS NULL
BEGIN
    CREATE DATABASE [$(DATABASE)];
END
GO

-- Enable CDC at the database level (idempotent)
USE [$(DATABASE)];
IF (SELECT is_cdc_enabled FROM sys.databases WHERE name = '$(DATABASE)') = 0
BEGIN
    EXEC sys.sp_cdc_enable_db;
END
GO

-- Create a SQL login (server) if missing
USE [master];
IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = '$(DB_USER)')
BEGIN
    CREATE LOGIN [$(DB_USER)] WITH PASSWORD = '$(DB_USER_PASSWORD)', CHECK_POLICY = ON;
END
GO

-- Create DB user for the app DB if missing
USE [$(DATABASE)];
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = '$(DB_USER)')
BEGIN
    CREATE USER [$(DB_USER)] FOR LOGIN [$(DB_USER)];
END
GO

-- Required for PowerSync to access the sys.dm_db_log_stats DMV
USE [master];
GRANT VIEW SERVER PERFORMANCE STATE TO [$(DB_USER)];
GO

-- Required for PowerSync to access the sys.dm_db_log_stats DMV and the sys.dm_db_partition_stats DMV
USE [$(DATABASE)];
GRANT VIEW DATABASE PERFORMANCE STATE TO [$(DB_USER)];
GO

-- Create PowerSync checkpoints table
-- Powersync requires this table to ensure regular checkpoints appear in CDC
IF OBJECT_ID('dbo._powersync_checkpoints', 'U') IS NULL
BEGIN
    CREATE TABLE dbo._powersync_checkpoints (
        id INT IDENTITY PRIMARY KEY,
        last_updated DATETIME NOT NULL DEFAULT (GETDATE())
    );
END

GRANT INSERT, UPDATE ON dbo._powersync_checkpoints TO [$(DB_USER)];
GO

-- Enable CDC for the powersync checkpoints table
IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID(N'dbo._powersync_checkpoints'))
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'dbo',
        @source_name   = N'_powersync_checkpoints',
        @role_name     = N'cdc_reader',
        @supports_net_changes = 0;
END
GO

-- Wait until capture job exists - usually takes a few seconds after enabling CDC on a table for the first time
DECLARE @tries int = 10;
WHILE @tries > 0 AND NOT EXISTS (SELECT 1 FROM msdb.dbo.cdc_jobs WHERE job_type = N'capture')
BEGIN
    WAITFOR DELAY '00:00:01';
    SET @tries -= 1;
END;

-- Set the CDC capture job polling interval to 1 second (default is 5 seconds)
EXEC sys.sp_cdc_change_job @job_type = N'capture', @pollinginterval = 1;
GO

/* -----------------------------------------------------------
   Create demo lists and todos tables and enables CDC on them.
   CDC must be enabled per table to actually capture changes.
------------------------------------------------------------*/
IF OBJECT_ID('dbo.lists', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.lists (
        id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID(),
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        name NVARCHAR(MAX) NOT NULL,
        owner_id UNIQUEIDENTIFIER NOT NULL,
        CONSTRAINT PK_lists PRIMARY KEY (id)
    );
END

GRANT INSERT, UPDATE, DELETE ON dbo.lists TO [$(DB_USER)];
GO

IF OBJECT_ID('dbo.todos', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.todos (
        id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID(),
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        completed_at DATETIME2 NULL,
        description NVARCHAR(MAX) NOT NULL,
        completed BIT NOT NULL DEFAULT 0,
        created_by UNIQUEIDENTIFIER NULL,
        completed_by UNIQUEIDENTIFIER NULL,
        list_id UNIQUEIDENTIFIER NOT NULL,
        CONSTRAINT PK_todos PRIMARY KEY (id),
        CONSTRAINT FK_todos_lists FOREIGN KEY (list_id) REFERENCES dbo.lists(id) ON DELETE CASCADE
    );
END

GRANT INSERT, UPDATE, DELETE ON dbo.todos TO [$(DB_USER)];
GO

-- Enable CDC for dbo.lists (idempotent guard)
IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID(N'dbo.lists'))
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'dbo',
        @source_name   = N'lists',
        @role_name     = N'cdc_reader',
        @supports_net_changes = 0;
END
GO

-- Enable CDC for dbo.todos (idempotent guard)
IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID(N'dbo.todos'))
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'dbo',
        @source_name   = N'todos',
        @role_name     = N'cdc_reader',
        @supports_net_changes = 0;
END
GO

-- Grant minimal rights to read CDC data
IF IS_ROLEMEMBER('db_datareader', '$(DB_USER)') = 0
BEGIN
    ALTER ROLE db_datareader ADD MEMBER [$(DB_USER)];
END

IF IS_ROLEMEMBER('cdc_reader', '$(DB_USER)') = 0
BEGIN
    ALTER ROLE cdc_reader ADD MEMBER [$(DB_USER)];
END
GO

-- Add demo data
IF NOT EXISTS (SELECT 1 FROM dbo.lists)
BEGIN
INSERT INTO dbo.lists (id, name, owner_id)
VALUES (NEWID(), 'Do a demo', NEWID());
END
GO