# PowerShell script to collect EC2, RDS, and CloudWatch configuration and utilization data

## Export Paths

**Log Files**
```
<InstallPath>\Export\$datestamp\Log\*.log
```

**CSV Export**
```
<InstallPath>\Export\Connections\*ServerName*\*.csv
<InstallPath>\Export\$datestamp\*ServerName*\*.csv
```

**Zip Files**
```
<InstallPath>\Export\*.zip
```

## Parameters

**-ValidateResourcesOnly** {default is false}
- This will validate AWS object and SQL Connectivity only

**-CollectTsqlData** {default is false}
- This executes all Tsql collection scripts and outputs to individual CSV files

**-CollectCloudWatchData** {default is false}
- This executes all CLI calls to collect CloudWatch data and export to CSV files

**-CollectConnectionsOnly** {default is false}
- This creates the CollectConnections Agent Job and runs a data collection against the [Connections] table the Agent Job CollectConnections is compiling

**-ExportDacPacs** {default is false}
- This will create a schema only .dacpac for all user databases

**-ExportDDL** {default is false}
- This will script the schema of all user databases to a readable .sql file (tables, indexes, constraints, views, procedures, functions, triggers, security and more). The T-SQL used is generated for the SQL Server version of each instance. Items that cannot be scripted are listed in the file on lines starting with `-- !!` and are logged as warnings SQL Server Agent jobs (steps, schedules, operators, proxies, categories) are also scripted, one SQLAgentJobs file per instance.

**-ExportDdlThreads** {default is 4}
- The number of servers whose DDL is exported at the same time (1 - 32). Messages from the parallel workers are written to the log by the main thread.

**-ExportDdlDatabaseThreads** {default is 4}
- The number of databases exported at the same time within ONE server (1 - 16). With the default of 1 an instance never runs more than one extraction at a time. Raise it only for instances that can take the extra catalog load; the most work in flight is -ExportDdlThreads x -ExportDdlDatabaseThreads.

**-CollectTsqlThreads** {default is 4}
- The number of servers whose T-SQL data (-CollectTsqlData) is collected at the same time (1 - 32). The two performance counter samples (60 seconds apart) are taken from every server at once, one before and one after this collection, so they do not hold a server thread.

**-CollectTsqlDatabaseThreads** {default is 4}
- The number of databases collected at the same time within ONE server (1 - 16). With the default of 1 an instance never runs more than one collection query set at a time.

**-CloudWatchThreads** {default is 4}
- The number of AWS CloudWatch calls made at the same time (1 - 16). Each call is an aws CLI process.

**-CloudWatchQueriesPerCall** {default is 500}
- How many metric series are requested in one CloudWatch `get-metric-data` call (1 - 500; fewer are used automatically when the answer would be very large). A few calls replace the thousands of per-metric, per-day `get-metric-statistics` calls used before. `0` uses the older per-day calls. `get-metric-data` needs the IAM permission `cloudwatch:GetMetricData` (now in MVA-AWS-Policy.json); if it is missing, the script collects those metrics with the older calls by itself and logs a warning.

**-ExportPath** {default is ''}
- This will override the default directory path where .CSV and .Zip will be written

**-AWSProfile** {default is ''}
- This will set the AWS Profile for Authenticating to your AWS account

**-UseSSOLogin** {default is false}
- This will use your SSO login for Authenticating to your AWS account **Preview**

**-SqlUser**
- User for SQL Authentication

**-SqlPassword**
- Password for SQL Authentication

**-SqlServerConnectionTimeout** {default is 5 seconds}
- This may need to be adjusted for your workload

**-SqlServerQueryTimeout** {default is 300 seconds}
- This may need to be adjusted for your workload

**-IncludeAllMsgs** {default is false}
- Verbose Mode

**-UsePublicIPs** {default is false}
- Uses Public facing DNS Names

**-CleanUpEnvironment** {default is false}
- This will remove:
  - Agent Job CollectConnections
  - Table [MVA-Data-Collection].[dbo].[Connections]
  - Database [MVA-Data-Collection]
  - SQLPackage Executable
  - `<InstallPath>\Export` Directory Structure

## Example Script Executions

```powershell
./MVA-Data-Collection.ps1 -ValidateResourcesOnly

./MVA-Data-Collection.ps1 -CollectTsqlData -CollectCloudWatchData -ExportDacPacs:$false

./MVA-Data-Collection.ps1 -CollectTsqlData -CollectCloudWatchData -ExportDDL -SqlUser 'myusername' -SqlPassword 'mypassword'

./MVA-Data-Collection.ps1 -CollectCloudWatchData -ExportPath 'C:\Temp\'

./MVA-Data-Collection.ps1 -CollectTsqlData -UseSSOLogin -AWSProfile 'MyProfileName'

./MVA-Data-Collection.ps1 -CollectConnectionsOnly 

./MVA-Data-Collection.ps1 -CleanUpEnvironment
```