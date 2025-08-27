-- Create database (idempotent)
DECLARE @db sysname = '$(APP_DB)';
IF DB_ID(@db) IS NULL
BEGIN
  DECLARE @sql nvarchar(max) = N'CREATE DATABASE [' + @db + N'];';
EXEC(@sql);
END
GO

-- Enable CLR (idempotent, needed for CDC net changes update-mask optimization)
IF (SELECT CAST(value_in_use AS INT) FROM sys.configurations WHERE name = 'clr enabled') = 0
BEGIN
  EXEC sp_configure 'show advanced options', 1;
  RECONFIGURE;
  EXEC sp_configure 'clr enabled', 1;
  RECONFIGURE;
END
GO

-- Enable CDC at the database level (idempotent)
DECLARE @db sysname = '$(APP_DB)';
DECLARE @cmd nvarchar(max) = N'USE [' + @db + N'];
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = ''' + @db + N''' AND is_cdc_enabled = 0)
    EXEC sys.sp_cdc_enable_db;';
EXEC(@cmd);
GO

-- Create a SQL login (server) and user (db), then grant CDC read access
-- Note: 'cdc_reader' role is auto-created when CDC is enabled on the DB.
DECLARE @db sysname = '$(APP_DB)';
DECLARE @login sysname = '$(APP_LOGIN)';
DECLARE @password nvarchar(128) = '$(APP_PASSWORD)';
-- Create login if missing
IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = @login)
BEGIN
  DECLARE @mklogin nvarchar(max) = N'CREATE LOGIN [' + @login + N'] WITH PASSWORD = ''' + @password + N''', CHECK_POLICY = ON;';
EXEC(@mklogin);
END;

-- Create user in DB if missing
DECLARE @mkuser nvarchar(max) = N'USE [' + @db + N'];
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = ''' + @login + N''')
  CREATE USER [' + @login + N'] FOR LOGIN [' + @login + N'];';
EXEC(@mkuser);
GO
/* -----------------------------------------------------------
   OPTIONAL: enable CDC for specific tables.
   You must enable CDC per table to actually capture changes.
   Example below creates a demo table and enables CDC on it.
------------------------------------------------------------*/

DECLARE @db sysname = '$(APP_DB)';
EXEC(N'USE [' + @db + N'];
IF OBJECT_ID(''dbo.lists'', ''U'') IS NULL
BEGIN
  CREATE TABLE dbo.lists (
    id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID(), -- GUID (36 characters),
    created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    name NVARCHAR(MAX) NOT NULL,
    owner_id UNIQUEIDENTIFIER NOT NULL,
    CONSTRAINT PK_lists PRIMARY KEY (id)
  );
END;
');


EXEC(N'USE [' + @db + N'];
IF OBJECT_ID(''dbo.todos'', ''U'') IS NULL
BEGIN
  CREATE TABLE dbo.todos (
    id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID(), -- GUID (36 characters)
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
END;
');
GO

-- Enable CDC for dbo.lists (idempotent guard)
DECLARE @db sysname = '$(APP_DB)';
DECLARE @login sysname = '$(APP_LOGIN)';
DECLARE @enableListsTable nvarchar(max) = N'USE [' + @db + N'];
IF NOT EXISTS (
    SELECT 1
    FROM cdc.change_tables
    WHERE source_object_id = OBJECT_ID(N''dbo.lists'')
)
BEGIN
  EXEC sys.sp_cdc_enable_table
    @source_schema = N''dbo'',
    @source_name   = N''lists'',
    @role_name     = N''cdc_reader'',
    @supports_net_changes = 1;
END;';
EXEC(@enableListsTable);

-- Enable CDC for dbo.todos (idempotent guard)
DECLARE @enableTodosTable nvarchar(max) = N'USE [' + @db + N'];
IF NOT EXISTS (
    SELECT 1
    FROM cdc.change_tables
    WHERE source_object_id = OBJECT_ID(N''dbo.todos'')
)
BEGIN
  EXEC sys.sp_cdc_enable_table
    @source_schema = N''dbo'',
    @source_name   = N''todos'',
    @role_name     = N''cdc_reader'',
    @supports_net_changes = 1;
END;';
EXEC(@enableTodosTable);

-- Grant minimal rights to read CDC data:
--   1) read access to base tables (db_datareader)
--   2) membership in cdc_reader (allows selecting from CDC change tables & functions)
DECLARE @grant nvarchar(max) = N'USE [' + @db + N'];
IF NOT EXISTS (SELECT 1 FROM sys.database_role_members rm
               JOIN sys.database_principals r ON rm.role_principal_id = r.principal_id AND r.name = ''db_datareader''
               JOIN sys.database_principals u ON rm.member_principal_id = u.principal_id AND u.name = ''' + @login + N''')
  ALTER ROLE db_datareader ADD MEMBER [' + @login + N'];

IF NOT EXISTS (SELECT 1 FROM sys.database_role_members rm
               JOIN sys.database_principals r ON rm.role_principal_id = r.principal_id AND r.name = ''cdc_reader''
               JOIN sys.database_principals u ON rm.member_principal_id = u.principal_id AND u.name = ''' + @login + N''')
  ALTER ROLE cdc_reader ADD MEMBER [' + @login + N'];';
EXEC(@grant);
GO

DECLARE @db sysname = '$(APP_DB)';
EXEC(N'USE [' + @db + N'];
BEGIN
  INSERT INTO dbo.lists (id, name, owner_id)
  VALUES (NEWID(), ''Do a demo'', NEWID());
END;
');
GO