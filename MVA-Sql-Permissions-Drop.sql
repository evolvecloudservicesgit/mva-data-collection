------------------------------------------------------------------------------
-- Clean up script for mva_user
------------------------------------------------------------------------------

USE [msdb];
GO
IF EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'mva_user')
	DROP USER [mva_user];

EXEC sp_MSforeachdb '
USE [?];
IF DB_ID(''?'') > 4  -- skip system DBs: master, model, msdb, tempdb
BEGIN
    IF EXISTS (SELECT 1 FROM sys.database_principals WHERE name = ''mva_user'')
        DROP USER [mva_user];
END
';


USE [master]
GO
DECLARE @kill varchar(8000) = '';
SELECT @kill=@kill+'kill '+convert(varchar(5),spid)+';'
    FROM [master].[dbo].[sysprocesses] 
WHERE loginame='mva_user';
EXEC (@kill);

IF EXISTS (SELECT 1 FROM [syslogins] WHERE name = 'mva_user')
	DROP LOGIN [mva_user]
GO
