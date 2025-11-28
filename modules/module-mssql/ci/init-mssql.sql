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