USE [master]
GO
IF NOT EXISTS (SELECT 1 FROM [syslogins] WHERE [name] = 'mva_user')
	CREATE LOGIN [mva_user] WITH PASSWORD=N'password', DEFAULT_DATABASE=[master], DEFAULT_LANGUAGE=[us_english], CHECK_EXPIRATION=OFF, CHECK_POLICY=OFF
Go

GRANT CREATE ANY DATABASE TO [mva_user];
GRANT VIEW SERVER STATE TO [mva_user];

IF EXISTS (SELECT 1 FROM [sys].[databases] WHERE [name] = 'rdsadmin') 
BEGIN
	USE [msdb];
	
	IF NOT EXISTS (SELECT 1 FROM [sys].[database_principals] WHERE [name] = 'mva_user')
		CREATE USER [mva_user] FOR LOGIN [mva_user];

	GRANT SELECT ON [dbo].[sysjobs] TO [mva_user];
	EXEC sp_addrolemember 'SQLAgentOperatorRole', [mva_user];

	EXEC sp_MSforeachdb '
	USE [?];
	IF DB_ID(''?'') > 5 -- skip system DBs: master, model, msdb, tempdb, rdsadmin
	BEGIN
		IF NOT EXISTS (SELECT 1 FROM [sys].[database_principals] WHERE [name] = ''mva_user'')
			CREATE USER [mva_user] FOR LOGIN [mva_user];

		GRANT SELECT ON [sys].[objects] TO [mva_user];
		GRANT VIEW DEFINITION TO [mva_user];

		IF DB_NAME() LIKE (''ReportServer%'') AND DB_NAME() NOT LIKE (''ReportServer%TempDB'')
		BEGIN
			GRANT SELECT ON [dbo].[DataSource] TO [mva_user];
			GRANT SELECT ON [dbo].[Catalog] TO [mva_user];
			GRANT SELECT ON [dbo].[DataSets] TO [mva_user];
			GRANT SELECT ON [dbo].[ActiveSubscriptions] TO [mva_user];
			GRANT SELECT ON [dbo].[Subscriptions] TO [mva_user];
			GRANT SELECT ON [dbo].[ReportSchedule] TO [mva_user];
			GRANT SELECT ON [dbo].[DataSource] TO [mva_user];
		END

		IF DB_NAME() LIKE (''SSISDB'')
		BEGIN
			GRANT SELECT ON [internal].[folders] TO [mva_user];
			GRANT SELECT ON [internal].[projects] TO [mva_user];
			GRANT SELECT ON [internal].[packages] TO [mva_user];
			GRANT SELECT ON [internal].[environments] TO [mva_user];
		END
	END
	';
END
ELSE 
BEGIN
	USE [msdb];
	
	IF NOT EXISTS (SELECT 1 FROM [sys].[database_principals] WHERE [name] = 'mva_user')
		CREATE USER [mva_user] FOR LOGIN [mva_user];

	GRANT SELECT ON [dbo].[sysjobs] TO [mva_user];
	GRANT EXECUTE ON [dbo].[sp_add_job] TO [mva_user];
	GRANT EXECUTE ON [dbo].[sp_add_jobstep] TO [mva_user];
	GRANT EXECUTE ON [dbo].[sp_update_job] TO [mva_user];
	GRANT EXECUTE ON [dbo].[sp_add_jobschedule] TO [mva_user];
	GRANT EXECUTE ON [dbo].[sp_add_jobserver] TO [mva_user];
	GRANT EXECUTE ON [dbo].[sp_delete_job] TO [mva_user];
	GRANT SELECT ON [dbo].[sysschedules] TO [mva_user];
	GRANT SELECT ON [dbo].[sysjobschedules] TO [mva_user];
	GRANT SELECT ON [dbo].[sysjobsteps] TO [mva_user];
	GRANT SELECT ON [dbo].[syscategories] TO [mva_user];

	EXEC sp_MSforeachdb '
	USE [?];
	IF DB_ID(''?'') > 4 -- skip system DBs: master, model, msdb, tempdb
	BEGIN
		IF NOT EXISTS (SELECT 1 FROM [sys].[database_principals] WHERE [name] = ''mva_user'')
			CREATE USER [mva_user] FOR LOGIN [mva_user];

		GRANT SELECT ON [sys].[objects] TO [mva_user];
		GRANT VIEW DEFINITION TO [mva_user];

		IF DB_NAME() LIKE (''ReportServer%'') AND DB_NAME() NOT LIKE (''ReportServer%TempDB'')
		BEGIN
			GRANT SELECT ON [dbo].[DataSource] TO [mva_user];
			GRANT SELECT ON [dbo].[Catalog] TO [mva_user];
			GRANT SELECT ON [dbo].[DataSets] TO [mva_user];
			GRANT SELECT ON [dbo].[ActiveSubscriptions] TO [mva_user];
			GRANT SELECT ON [dbo].[Subscriptions] TO [mva_user];
			GRANT SELECT ON [dbo].[ReportSchedule] TO [mva_user];
			GRANT SELECT ON [dbo].[DataSource] TO [mva_user];
		END

		IF DB_NAME() LIKE (''SSISDB'')
		BEGIN
			GRANT SELECT ON [internal].[folders] TO [mva_user];
			GRANT SELECT ON [internal].[projects] TO [mva_user];
			GRANT SELECT ON [internal].[packages] TO [mva_user];
			GRANT SELECT ON [internal].[environments] TO [mva_user];
		END
	END
	';
END
