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

./MVA-Data-Collection.ps1 -CollectTsqlData -CollectCloudWatchData -ExportDacPacs -SqlUser 'myusername' -SqlPassword 'mypassword'

./MVA-Data-Collection.ps1 -CollectCloudWatchData -ExportPath 'C:\Temp\'

./MVA-Data-Collection.ps1 -CollectTsqlData -UseSSOLogin -AWSProfile 'MyProfileName'

./MVA-Data-Collection.ps1 -CollectConnectionsOnly 

./MVA-Data-Collection.ps1 -CleanUpEnvironment
```