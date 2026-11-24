
##########################################################################################################################
#
# Author:  Phil Ekins - Director of Database Services
# Website: www.evolvecloudservices.com
# Email:   pekins@evolvecloudservices.com
#
# Version: 1.0.27
#
# Copyright © 2025 Evolve Cloud Services, LLC. or its affiliates. All Rights Reserved.
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING 
# BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND 
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, 
# DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, 
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# Export Paths:
#   Log Files  - <InstallPath>\Export\$datestamp\Log\*.log"
#   CSV Export - <InstallPath>\Export\Connections\*ServerName*\*.csv"
#              - <InstallPath>\Export\$datestamp\*ServerName*\*.csv""
#   Zip Files  - <InstallPath>\Export\*.zip
#
# Parameters:
#   -ValidateResourcesOnly {default is false}
#     This will validate AWS object and SQL Connectivity only
#   -CollectTsqlData {default is false}
#     This executes all Tsql collection scripts and outputs to individual CSV files
#   -CollectCloudWatchData {default is false}
#     This executes all CLI calls to collect CloudWatch data and export to CSV files
#   -CollectConnectionsOnly {default is false}
#     This creates the CollectConnections Agent Job and runs a data collection against the 
#     [Connections] table the Agent Job CollectConnections is compiling
#   -AMAOnly {default is false}
#     This executes the AMA Only data collection
#   -ExportDacPacs {default is false}
#     This will create a schema only .dacpac for all user databases
#   -ExportDDL {default is false}
#     This will script the schema of all user databases to a readable .sql file (tables, indexes, constraints,
#     views, procedures, functions, triggers, security, etc.) using T-SQL catalog queries generated for the
#     SQL Server version of each instance (see ExtractDDLScript). Items that cannot be scripted are listed
#     in the .sql file on lines starting with  -- !!  and are written to the log as warnings.
#     SQL Server Agent jobs are scripted too (one SQLAgentJobs file per instance)
#   -ExportDdlThreads {default is 4}
#     Number of servers whose DDL is exported at the same time (1 - 32)
#   -ExportDdlDatabaseThreads {default is 4}
#     Number of databases exported at the same time within ONE server (1 - 16). The default of 1 means an instance never
#     runs more than one extraction at a time; raise it only for instances that can take the extra catalog load
#   -CollectTsqlThreads {default is 4}
#     Number of servers whose T-SQL data is collected at the same time (1 - 32)
#   -CollectTsqlDatabaseThreads {default is 4}
#     Number of databases collected at the same time within ONE server (1 - 16). The 60 second performance counter
#     sampling always runs alongside the other queries of its server
#   -CloudWatchThreads {default is 4}
#     Number of AWS CloudWatch calls made at the same time (1 - 16); each call is an aws CLI process
#   -CloudWatchQueriesPerCall {default is 500}
#     Metric series requested per CloudWatch get-metric-data call (1 - 500; fewer are used automatically when the answer would be
#     very large). 0 uses the older get-metric-statistics call per metric per day. get-metric-data needs the IAM permission
#     cloudwatch:GetMetricData (see MVA-AWS-Policy.json); without it the script falls back to the older calls by itself
#   -ExportPath {default is ''}
#     This will override the default directory path where .CSV and .Zip will be written
#   -AWSProfile {default is ''}
#     This will set the AWS Profile for Authenticating to your AWS account
#   -UseSSOLogin {default is false}
#     This will use your SSO login for Authenticating to your AWS account ** Preview **
#   -SqlUser
#     User for SQL Authentication
#   -SqlPassword
#     Password for SQL Authentication
#   -SqlServerConnectionTimeout {default is 30 seconds}
#     This may need to be adjusted for your workload
#   -SqlServerQueryTimeout {default is 300 seconds}
#     This may need to be adjusted for your workload
#   -IncludeAllMsgs {default is false}
#     Verbose Mode
#   -UsePublicIPs {default is false}
#     Uses Public facing DNS Names
#   -CleanUpEnvironment {default is false}
#     This will remove
#         . Agent Job CollectConnections
#         . Table [MVA-Data-Collection].[dbo].[Connections]
#         . Databse [MVA-Data-Collection]
#         . SQLPackage Executable
#         . <InstallPath>\Export Directory Structure
#
# Example Script Executions:
#   ./MVA-Data-Collection.ps1 -ValidateResourcesOnly
#   ./MVA-Data-Collection.ps1 -CollectTsqlData -CollectCloudWatchData -ExportDacPacs:$false
#   ./MVA-Data-Collection.ps1 -CollectTsqlData -CollectCloudWatchData -ExportDDL -SqlUser 'myusername' -SqlPassword 'mypassword'
#   ./MVA-Data-Collection.ps1 -CollectCloudWatchData -ExportPath 'C:\Temp\'
#   ./MVA-Data-Collection.ps1 -CollectTsqlData -UseSSOLogin -AWSProfile 'MyProfileName'
#   ./MVA-Data-Collection.ps1 -CollectConnectionsOnly 
#   ./MVA-Data-Collection.ps1 -AMAOnly -SqlUser 'myusername' -SqlPassword 'mypassword'
#   ./MVA-Data-Collection.ps1 -CleanUpEnvironment 
#
##########################################################################################################################

param( 
    [Parameter(Mandatory=$false)] [switch] $CollectConnectionsOnly = $false,
    [Parameter(Mandatory=$false)] [switch] $ExportDacPacs = $false,
    [Parameter(Mandatory=$false)] [switch] $ExportDDL = $false,
    [Parameter(Mandatory=$false)] [int]    $ExportDdlThreads = 4,
    [Parameter(Mandatory=$false)] [int]    $ExportDdlDatabaseThreads = 4,
    [Parameter(Mandatory=$false)] [int]    $CollectTsqlThreads = 4,
    [Parameter(Mandatory=$false)] [int]    $CollectTsqlDatabaseThreads = 4,
    [Parameter(Mandatory=$false)] [int]    $CloudWatchThreads = 4,
    [Parameter(Mandatory=$false)] [int]    $CloudWatchQueriesPerCall = 500,
    [Parameter(Mandatory=$false)] [switch] $CollectCloudWatchData = $false,
    [Parameter(Mandatory=$false)] [switch] $CollectTsqlData = $false,
    [Parameter(Mandatory=$false)] [switch] $AMAOnly = $false,    
    [Parameter(Mandatory=$false)] [switch] $CleanUpEnvironment = $false,
    [Parameter(Mandatory=$false)] [int]    $SqlServerConnectionTimeout = 30,   
    [Parameter(Mandatory=$false)] [int]    $SqlServerQueryTimeout = 300,      
    [Parameter(Mandatory=$false)] [int]    $CloudWatchCollectionPeriod = 30, 
    [Parameter(Mandatory=$false)] [switch] $IncludeAllMsgs = $false,
    [Parameter(Mandatory=$false)] [switch] $ValidateResourcesOnly = $false,
    [Parameter(Mandatory=$false)] [String] $AWSProfile = '',   
    [Parameter(Mandatory=$false)] [switch] $UseSSOLogin = $false,  
    [Parameter(Mandatory=$false)] [String] $SqlUser = '',
    [Parameter(Mandatory=$false)] [String] $SqlPassword = '',
    [Parameter(Mandatory=$false)] [String] $ExportPath = '',
    [Parameter(Mandatory=$false)] [String] $FileNameDelimiter = '~~~~',
    [Parameter(Mandatory=$false)] [switch] $UsePublicIPs = $False,
    [Parameter(Mandatory=$false)] [switch] $DebugMode = $false
)
     
Function GetVersion()
{
    TRY {
     
        $Version = "1.0.27"

        Return $Version 
    } CATCH {
        IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
        LogActivity "** ERROR: GetVersion() : $ErrorMsg" $True
        Exit_Script -ErrorRaised $True
    }
}

Function LogActivity()
{
    Param ([string]$ErrorInfo, [bool]$Display)

    TRY {
        $timeStamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss.fff"

        IF (!([string]::IsNullOrWhiteSpace($global:LogFile))) {
            Add-content $global:LogFile -value "$timeStamp :  $ErrorInfo"
        } Else {
            $OverrideDisplay = $True 
        }

        IF ($Display -or $IncludeAllMsgs -or $OverrideDisplay) { WriteLogConsole $ErrorInfo $Display }
        Start-Sleep -Milliseconds 50
    } CATCH {
        IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
        throw "** ERROR: Error writing to log file : $ErrorInfo : $ErrorMsg"
    }
}

Function WriteLogConsole()
{
    Param ([string]$ErrorInfo, [bool]$Display)

    IF ( $ErrorInfo -like '** ERROR:*' ) {
        $ForegroundColor = 'Yellow'
    } ElseIf ( $ErrorInfo -like '** INFO:*' ) {
        IF ( $IncludeAllMsgs -and $Display ) {
            $ForegroundColor = 'Green'
        } ELSEIF ( $IncludeAllMsgs -and (!($Display)) ) {
            $ForegroundColor = 'White'
        } ELSE {
            $ForegroundColor = 'Green'
        }
    } Else {
        $ForegroundColor = 'White'
    }
    Write-Host $ErrorInfo -ForegroundColor $ForegroundColor
}

Function LogActivityBatch()
{
    Param ([object[]]$Messages)

    IF ($Null -eq $Messages -or $Messages.Count -eq 0) { Return }
    $Current = ''
    TRY {
        $lines = New-Object 'System.Collections.Generic.List[string]'
        ForEach ($m in $Messages) {
            $stamp = $(IF ([string]::IsNullOrWhiteSpace([string]$m.Stamp)) { Get-Date -Format "yyyy-MM-dd HH:mm:ss.fff" } Else { [string]$m.Stamp })
            $lines.Add("$stamp :  $($m.Text)")
        }
        $NoFile = [string]::IsNullOrWhiteSpace($global:LogFile)
        IF (-not $NoFile) { Add-Content -Path $global:LogFile -Value $lines.ToArray() }
        ForEach ($m in $Messages) {
            $Current = [string]$m.Text
            IF ([bool]$m.Display -or $IncludeAllMsgs -or $NoFile) { WriteLogConsole ([string]$m.Text) ([bool]$m.Display) }
        }
    } CATCH {
        IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
        throw "** ERROR: Error writing to log file : $Current : $ErrorMsg"
    }
}

Function ExportData()
{
    Param ( [string]$server,
            [string]$tableDesc,
            [String]$sql,
            [String]$database = 'master',
            [bool]$Append = $False,
            [switch]$PassThru
    )
    $ok = $False
    TRY {
        $Delimiter = '|'

        $FileId = $tableDesc.split('~')[0]
        $tableDesc = $tableDesc.split('~')[1].replace("[","").Replace("]","").replace(".","_")
        $sql = $sql.replace("@@@@",$database)

        TRY {
            $path = FormatString -InputString $("$ExportPath\$FileId$FileNameDelimiter$database$FileNameDelimiter$tableDesc$FileNameDelimiter$datestamp.csv")
            IF ($PassThru) {
                ## parallel workers: a failed query must not end the run from inside a worker (Exit_Script cannot do that there) - it is reported to the caller instead
                $rows = @(Invoke-Sql -ServerInstance $server -Database $database -Query $sql -RaiseError $False)
                IF ($rows.Count -eq 1 -and $rows[0] -is [string]) { Throw $rows[0] }
                $rows | Export-Csv -Path ($Path) -Delimiter $Delimiter -NoTypeInformation -Append:$Append
            } Else {
                (Invoke-Sql -ServerInstance $server -Database $database -Query $sql) | Export-Csv -Path ($Path) -Delimiter $Delimiter -NoTypeInformation -Append:$Append
            }
            LogActivity "** INFO: Exported $FileId$FileNameDelimiter$database$FileNameDelimiter$tableDesc : $server" $False
            $ok = $True
        } CATCH {
            IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
            LogActivity "** ERROR: Collecting $tableDesc : $database : $server : $ErrorMsg" $True
        }
    } CATCH {
        IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
        LogActivity "** ERROR: ExportData() : $ErrorMsg" $True
        Exit_Script -ErrorRaised $True
    }
    IF ($PassThru) { Return $ok }
}

Function CleanUpEnvironment()
{ 
    TRY {
        ## Collect Confirmation
        $global:Confirmation = Read-Host "Delete All MVA Data Collection Resources: (Y/N)?"
        IF ( (!($global:Confirmation -eq 'Y')) -or (!($global:Confirmation -eq 'y')) ) {
            LogActivity "** INFO: MVA Data Collection Resource Deletion Cancelled" $True
            $global:Confirmation = $False
            Exit_Script -ErrorRaised $False
        } ELSE {
            LogActivity "** INFO: MVA Data Collection Resource Deletion Confirmed" $True
        }

        ## Remove SqlPackage DacPac Media
        $SqlpackagePath = FormatString -InputString $("$ScriptRoot\Dacpac\")
        Set-Location -Path $env:userprofile

        IF (test-path $SqlpackagePath) {
            Remove-Item -Path $SqlpackagePath -Recurse -Force
        }

        ## Remove Connections Job and Table
        $sql = "IF EXISTS (SELECT 1 FROM [master].[dbo].[sysdatabases] WHERE name = 'MVA-Data-Collection') 
                    DROP DATABASE [MVA-Data-Collection]
                IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WHERE name = 'CollectConnections') 
                    EXEC msdb.dbo.sp_delete_job @job_name= N'CollectConnections'"

        ForEach ($Server in $global:AllServers) {
            IF (!([string]::IsNullOrWhiteSpace($Server))) {
                TRY {
                    Invoke-Sql -ServerInstance $Server -Database 'master' -Query $sql
                    LogActivity "** INFO: MVA Data Collection Objects removed : $Server" $False
                } CATCH {
                    IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
                    LogActivity "** ERROR: Unable to remove MVA Data Collection Objects : $Server" $True
                    LogActivity "** ERROR: Unable to remove MVA Data Collection Objects : $Server : $ErrorMsg" $False
                }
            }
        }

        ## Remove Directories and CSV files - retains *MVA-Export-ALL-*.zip files
        $AllItems = Get-ChildItem -Path ( $(FormatString -InputString "$ExportPath\Export")) -Recurse
        $Files = $AllItems | Where-Object { -not $_.PSIsContainer }
        $Directories = $AllItems | Where-Object { $_.PSIsContainer }

        $FilesToDelete = $Files | Where-Object { $_.Name -notlike "*MVA-Export-ALL-*.zip" }
        foreach ($File in $FilesToDelete) {
            try {
                Remove-Item -Path $File.FullName -Force
            }
            catch {
                IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
                LogActivity "** ERROR: Failed to delete file: $($File.FullName) : $ErrorMsg" $False
            }
        }

        $DirectoriesToCheck = $Directories | Sort-Object FullName -Descending
        foreach ($Dir in $DirectoriesToCheck) {
            try {
                $RemainingItems = Get-ChildItem -Path $Dir.FullName -Force
                
                if ($RemainingItems.Count -eq 0) {
                    Remove-Item -Path $Dir.FullName -Force
                }
                elseif (($RemainingItems | Where-Object { -not $_.PSIsContainer -and $_.Extension -ne ".zip" }).Count -eq 0) {
                    $HasZipFiles = ($RemainingItems | Where-Object { -not $_.PSIsContainer -and $_.Extension -eq ".zip" }).Count -gt 0
                    if (-not $HasZipFiles) {
                        $SubItems = Get-ChildItem -Path $Dir.FullName -Force
                        if ($SubItems.Count -eq 0) {
                            Remove-Item -Path $Dir.FullName -Force
                        }
                    }
                }
            }
            catch {
                IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
                LogActivity "** ERROR: Failed to process directory: $($Dir.FullName) : $ErrorMsg" $False                
            }
        }
    } CATCH {
        IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
        LogActivity "** ERROR: CleanUpEnvironment() : $ErrorMsg" $True
        Exit_Script -ErrorRaised $True
    }
}

Function LoadTSqlArray()
{
    Param ([string]$SqlVersion)
    TRY {
        ## Instance Specific Queries
        $global:TsqlInstance = @{}   
        
        ## 00 ConnectionInfo
        IF (($SqlVersion).SubString(0,2) -in("10","11","12","13","14","15","16","17")) {  
            $global:TsqlInstance.Add("00.002~ConnectionInfo","SELECT DISTINCT @@SERVERNAME AS SQLInstance, ec.client_net_address as ClientAddress, es.[program_name] as ProgramName, es.[host_name] as HostName, 
                    es.login_name as LoginName, DB_NAME(er.database_id) AS database_name, COUNT(ec.session_id) AS ConnectionCount,getdate() as collect_date 
                FROM sys.dm_exec_sessions es
                    JOIN sys.dm_exec_connections ec ON es.session_id = ec.session_id
                    LEFT JOIN sys.dm_exec_requests er ON es.session_id = er.session_id
                WHERE es.is_user_process = 1
                GROUP BY ec.client_net_address, es.[program_name], es.[host_name], es.login_name, DB_NAME(er.database_id) 
                ORDER BY ec.client_net_address, es.[program_name] OPTION (RECOMPILE);")
        }

        ## 01 SqlVersionInfo
        IF (($SqlVersion).SubString(0,2) -in("10","11","12","13","14","15","16","17")) {  
            $global:TsqlInstance.Add("01.001~SqlVersionInfo","SELECT @@SERVERNAME AS [SQLInstance], @@VERSION AS [Version]")            
        }

        ## 02 configurations
        IF (($SqlVersion).SubString(0,2) -in("10","11","12","13","14","15","16","17")) {  
            $global:TsqlInstance.Add("02.001~[sys].[configurations]","SELECT @@SERVERNAME AS SQLInstance, * FROM [master].[sys].[configurations]")               
        }

        ## 03 databases
        IF (($SqlVersion).SubString(0,2) -in("10")) {  
            $global:TsqlInstance.Add("03.001~[sys].[databases]","SELECT @@SERVERNAME AS [SQLInstance], 
            	[name],
                [database_id],
                [source_database_id],
                [owner_sid],
                [create_date],
                [compatibility_level],
                [collation_name],
                [user_access],
                [user_access_desc],
                [is_read_only],
                [is_auto_close_on],
                [is_auto_shrink_on],
                [state],
                [state_desc],
                [is_in_standby],
                [is_cleanly_shutdown],
                [is_supplemental_logging_enabled],
                [snapshot_isolation_state],
                [snapshot_isolation_state_desc],
                [is_read_committed_snapshot_on],
                [recovery_model],
                [recovery_model_desc],
                [page_verify_option],
                [page_verify_option_desc],
                [is_auto_create_stats_on],
                '' AS [is_auto_create_stats_incremental_on],
                [is_auto_update_stats_on],
                [is_auto_update_stats_async_on],
                [is_ansi_null_default_on],
                [is_ansi_nulls_on],
                [is_ansi_padding_on],
                [is_ansi_warnings_on],
                [is_arithabort_on],
                [is_concat_null_yields_null_on],
                [is_numeric_roundabort_on],
                [is_quoted_identifier_on],
                [is_recursive_triggers_on],
                [is_cursor_close_on_commit_on],
                [is_local_cursor_default],
                [is_fulltext_enabled],
                [is_trustworthy_on],
                [is_db_chaining_on],
                [is_parameterization_forced],
                [is_master_key_encrypted_by_server],
                '' AS [is_query_store_on],
                [is_published],
                [is_subscribed],
                [is_merge_published],
                [is_distributor],
                [is_sync_with_backup],
                [service_broker_guid],
                [is_broker_enabled],
                [log_reuse_wait],
                [log_reuse_wait_desc],
                [is_date_correlation_on],
                [is_cdc_enabled],
                [is_encrypted],
                [is_honor_broker_priority_on],
                '' AS [replica_id], 
                '' AS [group_database_id], 
                '' AS [resource_pool_id],
                '' AS [default_language_lcid], 
                '' AS [default_language_name], 
                '' AS [default_fulltext_language_lcid], 
                '' AS [default_fulltext_language_name], 
                '' AS [is_nested_triggers_on], 
                '' AS [is_transform_noise_words_on], 
                '' AS [two_digit_year_cutoff], 
                '' AS [containment], 
                '' AS [containment_desc], 
                '' AS [target_recovery_time_in_seconds], 
                '' AS [delayed_durability], 
                '' AS [delayed_durability_desc], 
                '' AS [is_memory_optimized_elevate_to_snapshot_on],  
                '' AS [is_federation_member], 
                '' AS [is_remote_data_archive_enabled], 
                '' AS [is_mixed_page_allocation_on], 
                '' AS [is_temporal_history_retention_enabled], 
                '' AS [catalog_collation_type], 
                '' AS [catalog_collation_type_desc], 
                '' AS [physical_database_name],
                '' AS [is_result_set_caching_on], 
                '' AS [is_accelerated_database_recovery_on], 
                '' AS [is_tempdb_spill_to_remote_store], 
                '' AS [is_stale_page_detection_on], 
                '' AS [is_memory_optimized_enabled],    
                '' AS [is_data_retention_enabled],
                '' AS [is_ledger_on], 
                '' AS [is_change_feed_enabled] FROM [master].[sys].[databases]")    
        } ELSEIF (($SqlVersion).SubString(0,2) -in("11")) {  
            $global:TsqlInstance.Add("03.002~[sys].[databases]","SELECT @@SERVERNAME AS [SQLInstance], 
            	[name],
                [database_id],
                [source_database_id],
                [owner_sid],
                [create_date],
                [compatibility_level],
                [collation_name],
                [user_access],
                [user_access_desc],
                [is_read_only],
                [is_auto_close_on],
                [is_auto_shrink_on],
                [state],
                [state_desc],
                [is_in_standby],
                [is_cleanly_shutdown],
                [is_supplemental_logging_enabled],
                [snapshot_isolation_state],
                [snapshot_isolation_state_desc],
                [is_read_committed_snapshot_on],
                [recovery_model],
                [recovery_model_desc],
                [page_verify_option],
                [page_verify_option_desc],
                [is_auto_create_stats_on],
                '' AS [is_auto_create_stats_incremental_on],
                [is_auto_update_stats_on],
                [is_auto_update_stats_async_on],
                [is_ansi_null_default_on],
                [is_ansi_nulls_on],
                [is_ansi_padding_on],
                [is_ansi_warnings_on],
                [is_arithabort_on],
                [is_concat_null_yields_null_on],
                [is_numeric_roundabort_on],
                [is_quoted_identifier_on],
                [is_recursive_triggers_on],
                [is_cursor_close_on_commit_on],
                [is_local_cursor_default],
                [is_fulltext_enabled],
                [is_trustworthy_on],
                [is_db_chaining_on],
                [is_parameterization_forced],
                [is_master_key_encrypted_by_server],
                '' AS [is_query_store_on],
                [is_published],
                [is_subscribed],
                [is_merge_published],
                [is_distributor],
                [is_sync_with_backup],
                [service_broker_guid],
                [is_broker_enabled],
                [log_reuse_wait],
                [log_reuse_wait_desc],
                [is_date_correlation_on],
                [is_cdc_enabled],
                [is_encrypted],
                [is_honor_broker_priority_on],
                [replica_id], 
                [group_database_id], 
                '' AS [resource_pool_id],
                [default_language_lcid], 
                [default_language_name], 
                [default_fulltext_language_lcid], 
                [default_fulltext_language_name], 
                [is_nested_triggers_on], 
                [is_transform_noise_words_on], 
                [two_digit_year_cutoff], 
                [containment], 
                [containment_desc], 
                [target_recovery_time_in_seconds],             
                '' AS [delayed_durability], 
                '' AS [delayed_durability_desc], 
                '' AS [is_memory_optimized_elevate_to_snapshot_on],  
                '' AS [is_federation_member], 
                '' AS [is_remote_data_archive_enabled], 
                '' AS [is_mixed_page_allocation_on], 
                '' AS [is_temporal_history_retention_enabled], 
                '' AS [catalog_collation_type], 
                '' AS [catalog_collation_type_desc], 
                '' AS [physical_database_name],
                '' AS [is_result_set_caching_on], 
                '' AS [is_accelerated_database_recovery_on], 
                '' AS [is_tempdb_spill_to_remote_store], 
                '' AS [is_stale_page_detection_on], 
                '' AS [is_memory_optimized_enabled],    
                '' AS [is_ledger_on], 
                '' AS [is_change_feed_enabled] FROM [master].[sys].[databases]")  
        } ELSEIF (($SqlVersion).SubString(0,2) -in("12")) {  
            $global:TsqlInstance.Add("03.003~[sys].[databases]","SELECT @@SERVERNAME AS [SQLInstance], 
            	[name],
                [database_id],
                [source_database_id],
                [owner_sid],
                [create_date],
                [compatibility_level],
                [collation_name],
                [user_access],
                [user_access_desc],
                [is_read_only],
                [is_auto_close_on],
                [is_auto_shrink_on],
                [state],
                [state_desc],
                [is_in_standby],
                [is_cleanly_shutdown],
                [is_supplemental_logging_enabled],
                [snapshot_isolation_state],
                [snapshot_isolation_state_desc],
                [is_read_committed_snapshot_on],
                [recovery_model],
                [recovery_model_desc],
                [page_verify_option],
                [page_verify_option_desc],
                [is_auto_create_stats_on],
                [is_auto_create_stats_incremental_on],
                [is_auto_update_stats_on],
                [is_auto_update_stats_async_on],
                [is_ansi_null_default_on],
                [is_ansi_nulls_on],
                [is_ansi_padding_on],
                [is_ansi_warnings_on],
                [is_arithabort_on],
                [is_concat_null_yields_null_on],
                [is_numeric_roundabort_on],
                [is_quoted_identifier_on],
                [is_recursive_triggers_on],
                [is_cursor_close_on_commit_on],
                [is_local_cursor_default],
                [is_fulltext_enabled],
                [is_trustworthy_on],
                [is_db_chaining_on],
                [is_parameterization_forced],
                [is_master_key_encrypted_by_server],
                [is_query_store_on],
                [is_published],
                [is_subscribed],
                [is_merge_published],
                [is_distributor],
                [is_sync_with_backup],
                [service_broker_guid],
                [is_broker_enabled],
                [log_reuse_wait],
                [log_reuse_wait_desc],
                [is_date_correlation_on],
                [is_cdc_enabled],
                [is_encrypted],
                [is_honor_broker_priority_on],
                [replica_id], 
                [group_database_id], 
                [resource_pool_id],
                [default_language_lcid], 
                [default_language_name], 
                [default_fulltext_language_lcid], 
                [default_fulltext_language_name], 
                [is_nested_triggers_on], 
                [is_transform_noise_words_on], 
                [two_digit_year_cutoff], 
                [containment], 
                [containment_desc], 
                [target_recovery_time_in_seconds],             
                [delayed_durability], 
                [delayed_durability_desc], 
                [is_memory_optimized_elevate_to_snapshot_on],  
                '' AS [is_federation_member],             
                '' AS [is_remote_data_archive_enabled], 
                '' AS [is_mixed_page_allocation_on], 
                '' AS [is_temporal_history_retention_enabled], 
                '' AS [catalog_collation_type], 
                '' AS [catalog_collation_type_desc], 
                '' AS [is_result_set_caching_on], 
                '' AS [is_accelerated_database_recovery_on], 
                '' AS [is_tempdb_spill_to_remote_store], 
                '' AS [is_stale_page_detection_on], 
                '' AS [is_memory_optimized_enabled],    
                '' AS [is_ledger_on], 
                '' AS [is_change_feed_enabled] FROM [master].[sys].[databases]")     
        } ELSEIF (($SqlVersion).SubString(0,2) -in("13")) {  
            $global:TsqlInstance.Add("03.004~[sys].[databases]","SELECT @@SERVERNAME AS [SQLInstance], 
            	[name],
                [database_id],
                [source_database_id],
                [owner_sid],
                [create_date],
                [compatibility_level],
                [collation_name],
                [user_access],
                [user_access_desc],
                [is_read_only],
                [is_auto_close_on],
                [is_auto_shrink_on],
                [state],
                [state_desc],
                [is_in_standby],
                [is_cleanly_shutdown],
                [is_supplemental_logging_enabled],
                [snapshot_isolation_state],
                [snapshot_isolation_state_desc],
                [is_read_committed_snapshot_on],
                [recovery_model],
                [recovery_model_desc],
                [page_verify_option],
                [page_verify_option_desc],
                [is_auto_create_stats_on],
                [is_auto_create_stats_incremental_on],
                [is_auto_update_stats_on],
                [is_auto_update_stats_async_on],
                [is_ansi_null_default_on],
                [is_ansi_nulls_on],
                [is_ansi_padding_on],
                [is_ansi_warnings_on],
                [is_arithabort_on],
                [is_concat_null_yields_null_on],
                [is_numeric_roundabort_on],
                [is_quoted_identifier_on],
                [is_recursive_triggers_on],
                [is_cursor_close_on_commit_on],
                [is_local_cursor_default],
                [is_fulltext_enabled],
                [is_trustworthy_on],
                [is_db_chaining_on],
                [is_parameterization_forced],
                [is_master_key_encrypted_by_server],
                [is_query_store_on],
                [is_published],
                [is_subscribed],
                [is_merge_published],
                [is_distributor],
                [is_sync_with_backup],
                [service_broker_guid],
                [is_broker_enabled],
                [log_reuse_wait],
                [log_reuse_wait_desc],
                [is_date_correlation_on],
                [is_cdc_enabled],
                [is_encrypted],
                [is_honor_broker_priority_on],
                [replica_id], 
                [group_database_id], 
                [resource_pool_id],
                [default_language_lcid], 
                [default_language_name], 
                [default_fulltext_language_lcid], 
                [default_fulltext_language_name], 
                [is_nested_triggers_on], 
                [is_transform_noise_words_on], 
                [two_digit_year_cutoff], 
                [containment], 
                [containment_desc], 
                [target_recovery_time_in_seconds],             
                [delayed_durability], 
                [delayed_durability_desc], 
                [is_memory_optimized_elevate_to_snapshot_on],  
                [is_federation_member],             
                [is_remote_data_archive_enabled], 
                [is_mixed_page_allocation_on], 
                '' AS [is_temporal_history_retention_enabled],             
                '' AS [catalog_collation_type], 
                '' AS [catalog_collation_type_desc], 
                '' AS [is_result_set_caching_on], 
                '' AS [is_accelerated_database_recovery_on], 
                '' AS [is_tempdb_spill_to_remote_store], 
                '' AS [is_stale_page_detection_on], 
                '' AS [is_memory_optimized_enabled],    
                '' AS [is_ledger_on], 
                '' AS [is_change_feed_enabled] FROM [master].[sys].[databases]")
        } ELSEIF (($SqlVersion).SubString(0,2) -in("14")) {  
            $global:TsqlInstance.Add("03.005~[sys].[databases]","SELECT @@SERVERNAME AS [SQLInstance], 
            	[name],
                [database_id],
                [source_database_id],
                [owner_sid],
                [create_date],
                [compatibility_level],
                [collation_name],
                [user_access],
                [user_access_desc],
                [is_read_only],
                [is_auto_close_on],
                [is_auto_shrink_on],
                [state],
                [state_desc],
                [is_in_standby],
                [is_cleanly_shutdown],
                [is_supplemental_logging_enabled],
                [snapshot_isolation_state],
                [snapshot_isolation_state_desc],
                [is_read_committed_snapshot_on],
                [recovery_model],
                [recovery_model_desc],
                [page_verify_option],
                [page_verify_option_desc],
                [is_auto_create_stats_on],
                [is_auto_create_stats_incremental_on],
                [is_auto_update_stats_on],
                [is_auto_update_stats_async_on],
                [is_ansi_null_default_on],
                [is_ansi_nulls_on],
                [is_ansi_padding_on],
                [is_ansi_warnings_on],
                [is_arithabort_on],
                [is_concat_null_yields_null_on],
                [is_numeric_roundabort_on],
                [is_quoted_identifier_on],
                [is_recursive_triggers_on],
                [is_cursor_close_on_commit_on],
                [is_local_cursor_default],
                [is_fulltext_enabled],
                [is_trustworthy_on],
                [is_db_chaining_on],
                [is_parameterization_forced],
                [is_master_key_encrypted_by_server],
                [is_query_store_on],
                [is_published],
                [is_subscribed],
                [is_merge_published],
                [is_distributor],
                [is_sync_with_backup],
                [service_broker_guid],
                [is_broker_enabled],
                [log_reuse_wait],
                [log_reuse_wait_desc],
                [is_date_correlation_on],
                [is_cdc_enabled],
                [is_encrypted],
                [is_honor_broker_priority_on],
                [replica_id], 
                [group_database_id], 
                [resource_pool_id],
                [default_language_lcid], 
                [default_language_name], 
                [default_fulltext_language_lcid], 
                [default_fulltext_language_name], 
                [is_nested_triggers_on], 
                [is_transform_noise_words_on], 
                [two_digit_year_cutoff], 
                [containment], 
                [containment_desc], 
                [target_recovery_time_in_seconds],             
                [delayed_durability], 
                [delayed_durability_desc], 
                [is_memory_optimized_elevate_to_snapshot_on],  
                [is_federation_member],             
                [is_remote_data_archive_enabled], 
                [is_mixed_page_allocation_on], 
                [is_temporal_history_retention_enabled],             
                '' AS [catalog_collation_type], 
                '' AS [catalog_collation_type_desc], 
                '' AS [is_result_set_caching_on],             
                '' AS [is_accelerated_database_recovery_on], 
                '' AS [is_tempdb_spill_to_remote_store], 
                '' AS [is_stale_page_detection_on], 
                '' AS [is_memory_optimized_enabled],    
                '' AS [is_ledger_on], 
                '' AS [is_change_feed_enabled] FROM [master].[sys].[databases]")
        } ELSEIF (($SqlVersion).SubString(0,2) -in("15")) {  
            $global:TsqlInstance.Add("03.006~[sys].[databases]","SELECT @@SERVERNAME AS [SQLInstance], 
            	[name],
                [database_id],
                [source_database_id],
                [owner_sid],
                [create_date],
                [compatibility_level],
                [collation_name],
                [user_access],
                [user_access_desc],
                [is_read_only],
                [is_auto_close_on],
                [is_auto_shrink_on],
                [state],
                [state_desc],
                [is_in_standby],
                [is_cleanly_shutdown],
                [is_supplemental_logging_enabled],
                [snapshot_isolation_state],
                [snapshot_isolation_state_desc],
                [is_read_committed_snapshot_on],
                [recovery_model],
                [recovery_model_desc],
                [page_verify_option],
                [page_verify_option_desc],
                [is_auto_create_stats_on],
                [is_auto_create_stats_incremental_on],
                [is_auto_update_stats_on],
                [is_auto_update_stats_async_on],
                [is_ansi_null_default_on],
                [is_ansi_nulls_on],
                [is_ansi_padding_on],
                [is_ansi_warnings_on],
                [is_arithabort_on],
                [is_concat_null_yields_null_on],
                [is_numeric_roundabort_on],
                [is_quoted_identifier_on],
                [is_recursive_triggers_on],
                [is_cursor_close_on_commit_on],
                [is_local_cursor_default],
                [is_fulltext_enabled],
                [is_trustworthy_on],
                [is_db_chaining_on],
                [is_parameterization_forced],
                [is_master_key_encrypted_by_server],
                [is_query_store_on],
                [is_published],
                [is_subscribed],
                [is_merge_published],
                [is_distributor],
                [is_sync_with_backup],
                [service_broker_guid],
                [is_broker_enabled],
                [log_reuse_wait],
                [log_reuse_wait_desc],
                [is_date_correlation_on],
                [is_cdc_enabled],
                [is_encrypted],
                [is_honor_broker_priority_on],
                [replica_id], 
                [group_database_id], 
                [resource_pool_id],
                [default_language_lcid], 
                [default_language_name], 
                [default_fulltext_language_lcid], 
                [default_fulltext_language_name], 
                [is_nested_triggers_on], 
                [is_transform_noise_words_on], 
                [two_digit_year_cutoff], 
                [containment], 
                [containment_desc], 
                [target_recovery_time_in_seconds],             
                [delayed_durability], 
                [delayed_durability_desc], 
                [is_memory_optimized_elevate_to_snapshot_on],  
                [is_federation_member],             
                [is_remote_data_archive_enabled], 
                [is_mixed_page_allocation_on], 
                [is_temporal_history_retention_enabled],             
                [catalog_collation_type], 
                [catalog_collation_type_desc], 
                [is_result_set_caching_on],             
                [is_accelerated_database_recovery_on], 
                [is_tempdb_spill_to_remote_store], 
                [is_stale_page_detection_on], 
                [is_memory_optimized_enabled],             
                '' AS [is_ledger_on], 
                '' AS [is_change_feed_enabled] FROM [master].[sys].[databases]")
        } ELSEIF (($SqlVersion).SubString(0,2) -in("16")) {  
            $global:TsqlInstance.Add("03.007~[sys].[databases]","SELECT @@SERVERNAME AS [SQLInstance], 
            	[name],
                [database_id],
                [source_database_id],
                [owner_sid],
                [create_date],
                [compatibility_level],
                [collation_name],
                [user_access],
                [user_access_desc],
                [is_read_only],
                [is_auto_close_on],
                [is_auto_shrink_on],
                [state],
                [state_desc],
                [is_in_standby],
                [is_cleanly_shutdown],
                [is_supplemental_logging_enabled],
                [snapshot_isolation_state],
                [snapshot_isolation_state_desc],
                [is_read_committed_snapshot_on],
                [recovery_model],
                [recovery_model_desc],
                [page_verify_option],
                [page_verify_option_desc],
                [is_auto_create_stats_on],
                [is_auto_create_stats_incremental_on],
                [is_auto_update_stats_on],
                [is_auto_update_stats_async_on],
                [is_ansi_null_default_on],
                [is_ansi_nulls_on],
                [is_ansi_padding_on],
                [is_ansi_warnings_on],
                [is_arithabort_on],
                [is_concat_null_yields_null_on],
                [is_numeric_roundabort_on],
                [is_quoted_identifier_on],
                [is_recursive_triggers_on],
                [is_cursor_close_on_commit_on],
                [is_local_cursor_default],
                [is_fulltext_enabled],
                [is_trustworthy_on],
                [is_db_chaining_on],
                [is_parameterization_forced],
                [is_master_key_encrypted_by_server],
                [is_query_store_on],
                [is_published],
                [is_subscribed],
                [is_merge_published],
                [is_distributor],
                [is_sync_with_backup],
                [service_broker_guid],
                [is_broker_enabled],
                [log_reuse_wait],
                [log_reuse_wait_desc],
                [is_date_correlation_on],
                [is_cdc_enabled],
                [is_encrypted],
                [is_honor_broker_priority_on],
                [replica_id], 
                [group_database_id], 
                [resource_pool_id],
                [default_language_lcid], 
                [default_language_name], 
                [default_fulltext_language_lcid], 
                [default_fulltext_language_name], 
                [is_nested_triggers_on], 
                [is_transform_noise_words_on], 
                [two_digit_year_cutoff], 
                [containment], 
                [containment_desc], 
                [target_recovery_time_in_seconds],             
                [delayed_durability], 
                [delayed_durability_desc], 
                [is_memory_optimized_elevate_to_snapshot_on],  
                [is_federation_member],             
                [is_remote_data_archive_enabled], 
                [is_mixed_page_allocation_on], 
                [is_temporal_history_retention_enabled],             
                [catalog_collation_type], 
                [catalog_collation_type_desc], 
                [is_result_set_caching_on],             
                [is_accelerated_database_recovery_on], 
                [is_tempdb_spill_to_remote_store], 
                [is_stale_page_detection_on], 
                [is_memory_optimized_enabled],             
                [is_ledger_on], 
                [is_change_feed_enabled],
                '' AS [is_data_lake_replication_enabled],
                '' AS [is_event_stream_enabled],
                '' AS [data_compaction],
                '' AS [data_compaction_desc],
                '' AS [data_lake_log_publishing],
                '' AS [data_lake_log_publishing_desc],
                '' AS [is_vorder_enabled],
                '' AS [is_proactive_statistics_refresh_on],
                '' AS [is_optimized_locking_on] FROM [master].[sys].[databases]")
        } ELSEIF (($SqlVersion).SubString(0,2) -in("17")) {  
            $global:TsqlInstance.Add("03.008~[sys].[databases]","SELECT @@SERVERNAME AS [SQLInstance], * FROM [master].[sys].[databases]")
        }

        ## 04 master_files
        IF (($SqlVersion).SubString(0,2) -in("10","11","12","13","14","15","16","17")) {  
            $global:TsqlInstance.Add("04.001~[sys].[master_files]","SELECT @@SERVERNAME AS SQLInstance, [name] FROM [master].[sys].[master_files]")     
        }

        ## 5 DatabaseIOInfo
        IF (($SqlVersion).SubString(0,2) -in("10","11","12","13","14","15","16","17")) {  
            $global:TsqlInstance.Add("05.001~DatabaseIOInfo","WITH DBIO AS (SELECT DB_NAME(IVFS.database_id) AS db_name, 
                CASE WHEN MF.type = 1 THEN 'log' ELSE 'data' END AS file_type, MF.name As file_name, SUM(IVFS.num_of_bytes_read + IVFS.num_of_bytes_written) AS io,
                SUM(IVFS.io_stall) AS io_stall FROM sys.dm_io_virtual_file_stats(NULL, NULL) AS IVFS
                INNER JOIN sys.master_files AS MF ON IVFS.database_id = MF.database_id AND IVFS.file_id = MF.file_id
                GROUP BY DB_NAME(IVFS.database_id), MF.[type], MF.name) 
                SELECT @@SERVERNAME AS SQLInstance, GetDate(), db_name, file_type, file_name, CAST(1. * io / (1024 * 1024) AS DECIMAL(12, 2)) AS io_mb,
                CAST(io_stall / 1000. AS DECIMAL(12, 2)) AS io_stall_s, CAST(100. * io_stall / SUM(io_stall) OVER() AS DECIMAL(10, 2)) AS io_stall_pct,
                ROW_NUMBER() OVER(ORDER BY io_stall DESC) AS rn FROM DBIO ORDER BY io_stall DESC")      
        }  

        ## 6 availability_replicas
        IF (($SqlVersion).SubString(0,2) -in("12")) {  
            $global:TsqlInstance.Add("06.001~[sys].[availability_replicas]","SELECT @@SERVERNAME AS SQLInstance, *,
                '' AS [seeding_mode], 
                '' AS [seeding_mode_desc],
                '' AS [read_write_routing_url]                            
                FROM [master].[sys].[availability_replicas]")    
        } ELSEIf (($Sqlversion).SubString(0,2) -in("13","14")) {  
            $global:TsqlInstance.Add("06.002~[sys].[availability_replicas]","SELECT @@SERVERNAME AS SQLInstance, *,
                '' AS [read_write_routing_url] 
                FROM [master].[sys].[availability_replicas]")
        } ELSEIf (($Sqlversion).SubString(0,2) -in("15","16","17")) {  
            $global:TsqlInstance.Add("06.003~[sys].[availability_replicas]","SELECT @@SERVERNAME AS SQLInstance, * FROM [master].[sys].[availability_replicas]")
        }

        ## 7 availability_read_only_routing_lists
        IF (($SqlVersion).SubString(0,2) -in("12","13","14","15","16","17")) {  
            $global:TsqlInstance.Add("07.001~[sys].[availability_read_only_routing_lists]","SELECT @@SERVERNAME AS SQLInstance, * FROM [master].[sys].[availability_read_only_routing_lists]")        
        } 

        ## 8 dm_hadr_availability_replica_cluster_nodes
        If (($Sqlversion).SubString(0,2) -in("12","13","14","15","16","17")) {  
            $global:TsqlInstance.Add("08.001~[sys].[dm_hadr_availability_replica_cluster_nodes]","SELECT @@SERVERNAME AS SQLInstance, * FROM [master].[sys].[dm_hadr_availability_replica_cluster_nodes]")
        }        

        ## 9 dm_hadr_availability_replica_cluster_states
        If (($Sqlversion).SubString(0,2) -in("12","13","14","15","16","17")) {  
            $global:TsqlInstance.Add("09.001~[sys].[dm_hadr_availability_replica_cluster_states]","SELECT @@SERVERNAME AS SQLInstance, * FROM [master].[sys].[dm_hadr_availability_replica_cluster_states]")
        }

        ## 10 dm_hadr_database_replica_states
        IF (($SqlVersion).SubString(0,2) -in("12")) {  
            $global:TsqlInstance.Add("10.001~[sys].[dm_hadr_database_replica_states]","SELECT @@SERVERNAME AS SQLInstance, *,
                '' AS [secondary_lag_seconds], 
                '' AS [quorum_commit_lsn],
                '' AS [quorum_commit_time]             
                FROM [master].[sys].[dm_hadr_database_replica_states]")
        } ELSEIf (($Sqlversion).SubString(0,2) -in("13","14")) {  
            $global:TsqlInstance.Add("10.002~[sys].[dm_hadr_database_replica_states]","SELECT @@SERVERNAME AS SQLInstance, *,
                '' AS [quorum_commit_lsn],
                '' AS [quorum_commit_time]             
                FROM [master].[sys].[dm_hadr_database_replica_states]")                
        } ELSEIf (($Sqlversion).SubString(0,2) -in("15","16","17")) {  
            $global:TsqlInstance.Add("10.003~[sys].[dm_hadr_database_replica_states]","SELECT @@SERVERNAME AS SQLInstance, * FROM [master].[sys].[dm_hadr_database_replica_states]") 
        }

        ## 11 availability_databases_cluster
        If (($Sqlversion).SubString(0,2) -in("12","13","14","15","16","17")) {  
            $global:TsqlInstance.Add("11.001~[sys].[availability_databases_cluster]","SELECT @@SERVERNAME AS SQLInstance, * FROM [master].[sys].[availability_databases_cluster]")   
        }        

        ## 12 availability_groups
        IF (($SqlVersion).SubString(0,2) -in("12")) {  
            $global:TsqlInstance.Add("12.001~[sys].[availability_groups]","SELECT @@SERVERNAME AS SQLInstance, *,
                '' AS [version], 
                '' AS [basic_features],
                '' AS [dtc_support],   
                '' AS [db_failover], 
                '' AS [is_distributed],
                '' AS [cluster_type],                               
                '' AS [cluster_type_desc], 
                '' AS [required_synchronized_secondaries_to_commit],
                '' AS [sequence_number],   
                '' AS [is_contained] FROM [master].[sys].[availability_groups]")                   
        } ELSEIF (($SqlVersion).SubString(0,2) -in("13")) {  
            $global:TsqlInstance.Add("12.002~[sys].[availability_groups]","SELECT @@SERVERNAME AS SQLInstance, *,
                '' AS [cluster_type],                               
                '' AS [cluster_type_desc], 
                '' AS [required_synchronized_secondaries_to_commit],
                '' AS [sequence_number],   
                '' AS [is_contained] FROM [master].[sys].[availability_groups]")    
        } ELSEIF (($SqlVersion).SubString(0,2) -in("14")) {  
            $global:TsqlInstance.Add("12.003~[sys].[availability_groups]","SELECT @@SERVERNAME AS SQLInstance, *,
                '' AS [is_contained] FROM [master].[sys].[availability_groups]")  
        } ELSEIf (($Sqlversion).SubString(0,2) -in("15","16","17")) {    
            $global:TsqlInstance.Add("12.004~[sys].[availability_groups]","SELECT @@SERVERNAME AS SQLInstance, * FROM [master].[sys].[availability_groups]")  
        }

        ## 13 dm_hadr_availability_replica_states
        If (($Sqlversion).SubString(0,2) -in("12","13")) {  
            $global:TsqlInstance.Add("13.002~[sys].[dm_hadr_availability_replica_states]","SELECT @@SERVERNAME AS SQLInstance, *,
                '' AS [write_lease_remaining_ticks],                               
                '' AS [current_configuration_commit_start_time_utc]
            FROM [master].[sys].[dm_hadr_availability_replica_states]")   
        } ELSEIF (($SqlVersion).SubString(0,2) -in("14")) {  
            $global:TsqlInstance.Add("13.001~[sys].[dm_hadr_availability_replica_states]","SELECT @@SERVERNAME AS SQLInstance, *,                          
                '' AS [current_configuration_commit_start_time_utc]
            FROM [master].[sys].[dm_hadr_availability_replica_states]")   
        } ELSEIf (($Sqlversion).SubString(0,2) -in("15","16","17")) {  
            $global:TsqlInstance.Add("13.003~[sys].[dm_hadr_availability_replica_states]","SELECT @@SERVERNAME AS SQLInstance, * FROM [master].[sys].[dm_hadr_availability_replica_states]")  
        }
        
        ## 14 database_mirroring
        IF (($SqlVersion).SubString(0,2) -in("10","11","12","13","14","15","16","17")) {  
            $global:TsqlInstance.Add("14.001~[sys].[database_mirroring]","SELECT @@SERVERNAME AS SQLInstance, * FROM [master].[sys].[database_mirroring]")
        }        

        ## 15 server_audits
        IF (($SqlVersion).SubString(0,2) -in("10")) {  
            $global:TsqlInstance.Add("15.001~[sys].[server_audits]","SELECT @@SERVERNAME AS SQLInstance, *, '' AS [predicate], '' AS [is_operator_audit] FROM [master].[sys].[server_audits]")
        } ELSEIf (($Sqlversion).SubString(0,2) -in("11","12","13","14","15")) {  
            $global:TsqlInstance.Add("15.003~[sys].[server_audits]","SELECT @@SERVERNAME AS SQLInstance, *, '' AS [is_operator_audit] FROM [master].[sys].[server_audits]")
        } ELSEIF (($SqlVersion).SubString(0,2) -in("16","17")) {  
            $global:TsqlInstance.Add("15.002~[sys].[server_audits]","SELECT @@SERVERNAME AS SQLInstance, * FROM [master].[sys].[server_audits]")
        }    

        ## 16 server_audit_specifications
        If (($Sqlversion).SubString(0,2) -in("10","11","12","13","14","15")) {  
            $global:TsqlInstance.Add("16.002~[sys].[server_audit_specifications]","SELECT @@SERVERNAME AS SQLInstance, *, '' AS [is_session_context_enabled], '' AS [session_context_keys] FROM [master].[sys].[server_audit_specifications]")
        } ELSEIF (($SqlVersion).SubString(0,2) -in("16","17")) {  
            $global:TsqlInstance.Add("16.001~[sys].[server_audit_specifications]","SELECT @@SERVERNAME AS SQLInstance, * FROM [master].[sys].[server_audit_specifications]")
        }   
   
        ## 17 server_audit_specification_details
        IF (($SqlVersion).SubString(0,2) -in("10","11","12","13","14","15","16","17")) {  
            $global:TsqlInstance.Add("17.001~[sys].[server_audit_specification_details]","SELECT @@SERVERNAME AS SQLInstance, * FROM [master].[sys].[server_audit_specification_details]")
        }   

        ## 40 linkedservers
        IF (($SqlVersion).SubString(0,2) -in("10","11","12","13","14","15","16","17")) {  
            $global:TsqlInstance.Add("40.001~[linkedservers]","EXEC [sp_linkedservers]")
        }  

        ## 44 sys.dm_os_performance_counters
        IF (($SqlVersion).SubString(0,2) -in("10","11","12","13","14","15","16","17")) {  
            $global:TsqlInstance.Add("44.001~[sys_dm_os_performance_counters]","SELECT @@SERVERNAME AS SQLInstance, *, CONVERT(varchar(23), GETDATE(), 121) AS inserted_stamp FROM [sys].[dm_os_performance_counters]")   
        }       
        
        ## 45 trace_flags
        IF (($SqlVersion).SubString(0,2) -in("10","11","12","13","14","15","16","17")) {  
            $global:TsqlInstance.Add("45.001~[trace_flags]","CREATE TABLE #mva_TraceStatus (TraceFlag INT, Status INT, Global INT, Session INT);
                INSERT INTO #mva_TraceStatus (TraceFlag, Status, Global, Session) EXEC ('DBCC TRACESTATUS(-1) WITH NO_INFOMSGS');
                SELECT @@SERVERNAME AS SQLInstance, * FROM #mva_TraceStatus;")   
        }     
        
        ## 46 sys.dm_os_process_memory
        IF (($SqlVersion).SubString(0,2) -in("10","11","12","13","14","15","16","17")) {  
            $global:TsqlInstance.Add("46.001~[sys_dm_os_process_memory]","SELECT @@SERVERNAME AS SQLInstance, * FROM [sys].[dm_os_process_memory]")   
        }     
        
        ## 47 sql_agent_job_info
        IF (($SqlVersion).SubString(0,2) -in("10","11","12","13","14","15","16","17")) {  
            $global:TsqlInstance.Add("47.001~[sql_agent_job_info]","IF NOT EXISTS (SELECT 1 FROM [master].[sys].[databases] WHERE [name] = 'rdsadmin')
                BEGIN
                    SELECT @@SERVERNAME AS SQLInstance,j.job_id,j.name AS JobName,j.enabled AS JobEnabled,j.name AS Category,s.step_id,s.step_name,
                        s.subsystem,s.command,sc.name AS ScheduleName,sc.enabled AS ScheduleEnabled,sc.freq_type,sc.freq_interval,sc.freq_subday_type,
                        sc.freq_subday_interval,sc.freq_relative_interval,sc.freq_recurrence_factor,sc.active_start_date,sc.active_end_date,
                        sc.active_start_time,sc.active_end_time 
                    FROM [msdb].[dbo].[sysjobs] j
                        LEFT JOIN msdb.dbo.syscategories c ON j.category_id = c.category_id
                        LEFT JOIN msdb.dbo.sysjobsteps s ON j.job_id = s.job_id
                        LEFT JOIN msdb.dbo.sysjobschedules js ON j.job_id = js.job_id
                        LEFT JOIN msdb.dbo.sysschedules sc ON js.schedule_id = sc.schedule_id
                END
            ELSE
                BEGIN
                    SELECT @@SERVERNAME AS SQLInstance,j.job_id,j.name AS JobName,j.enabled AS JobEnabled,j.name AS Category,
                        0 AS step_id,
                        '' AS step_name,
                        '' AS subsystem,
                        '' AS command ,
                        '' AS  ScheduleName,
                        0 AS ScheduleEnabled,
                        0 AS freq_type,
                        0 AS freq_interval,
                        0 AS freq_subday_type,
                        0 AS freq_subday_interval,
                        0 AS freq_relative_interval,
                        0 AS freq_recurrence_factor,
                        '' AS  active_start_date,
                        '' AS  active_end_date,
                        '' AS  active_start_time,
                        '' AS  active_end_time 
			        FROM [msdb].[dbo].[sysjobs] j
                END")   
        }    
        
        ## 48 sys.dm_os_sys_info
        IF (($SqlVersion).SubString(0,2) -in("10")) {  
            $global:TsqlInstance.Add("48.001~[sys_dm_os_sys_info]","
            SELECT @@SERVERNAME AS SQLInstance, 
                [cpu_ticks],
                [ms_ticks],
                [cpu_count],
                [hyperthread_ratio],
                [physical_memory_in_bytes] AS [physical_memory_kb],
                [virtual_memory_in_bytes] AS [virtual_memory_kb],
                [bpool_committed] AS [committed_kb],
                [bpool_commit_target] AS [committed_target_kb],
                [bpool_visible] AS [visible_target_kb],
                [stack_size_in_bytes],
                [os_quantum],
                [os_error_mode],
                [os_priority_class],
                [max_workers_count],
                [scheduler_count],
                [scheduler_total_count],
                [deadlock_monitor_serial_number],
                [sqlserver_start_time_ms_ticks],
                [sqlserver_start_time],
                '' AS [affinity_type],
                '' AS [affinity_type_desc],
                '' AS [process_kernel_time_ms],
                '' AS [process_user_time_ms],
                '' AS [time_source],
                '' AS [time_source_desc],
                '' AS [virtual_machine_type],
                '' AS [virtual_machine_type_desc],
                '' AS [softnuma_configuration],
                '' AS [softnuma_configuration_desc],
                '' AS [process_physical_affinity],
                '' AS [sql_memory_model],
                '' AS [sql_memory_model_desc],
                '' AS [socket_count],
                '' AS [cores_per_socket],
                '' AS [numa_node_count],
                '' AS [container_type],
                '' AS [container_type_desc]
            FROM [sys].[dm_os_sys_info]")   
        } ElseIF (($SqlVersion).SubString(0,2) -in("11")) {  
            $global:TsqlInstance.Add("48.002~[sys_dm_os_sys_info]","
            SELECT @@SERVERNAME AS SQLInstance, 
                [cpu_ticks],
                [ms_ticks],
                [cpu_count],
                [hyperthread_ratio],
                [physical_memory_kb],
                [virtual_memory_kb],
                [committed_kb],
                [committed_target_kb],
                [visible_target_kb],
                [stack_size_in_bytes],
                [os_quantum],
                [os_error_mode],
                [os_priority_class],
                [max_workers_count],
                [scheduler_count],
                [scheduler_total_count],
                [deadlock_monitor_serial_number],
                [sqlserver_start_time_ms_ticks],
                [sqlserver_start_time],
                [affinity_type],
                [affinity_type_desc],
                [process_kernel_time_ms],
                [process_user_time_ms],
                [time_source],
                [time_source_desc],
                [virtual_machine_type],
                [virtual_machine_type_desc],
                '' AS [softnuma_configuration],
                '' AS [softnuma_configuration_desc],
                '' AS [process_physical_affinity],
                [sql_memory_model],
                [sql_memory_model_desc],
                '' AS [socket_count],
                '' AS [cores_per_socket],
                '' AS [numa_node_count],
                '' AS [container_type],
                '' AS [container_type_desc]
            FROM [sys].[dm_os_sys_info]")
        } ElseIF (($SqlVersion).SubString(0,2) -in("12")) {  
            $global:TsqlInstance.Add("48.003~[sys_dm_os_sys_info]","
            SELECT @@SERVERNAME AS SQLInstance, 
                [cpu_ticks],
                [ms_ticks],
                [cpu_count],
                [hyperthread_ratio],
                [physical_memory_kb],
                [virtual_memory_kb],
                [committed_kb],
                [committed_target_kb],
                [visible_target_kb],
                [stack_size_in_bytes],
                [os_quantum],
                [os_error_mode],
                [os_priority_class],
                [max_workers_count],
                [scheduler_count],
                [scheduler_total_count],
                [deadlock_monitor_serial_number],
                [sqlserver_start_time_ms_ticks],
                [sqlserver_start_time],
                [affinity_type],
                [affinity_type_desc],
                [process_kernel_time_ms],
                [process_user_time_ms],
                [time_source],
                [time_source_desc],
                [virtual_machine_type],
                [virtual_machine_type_desc],
                '' AS [softnuma_configuration],
                '' AS [softnuma_configuration_desc],
                '' AS [process_physical_affinity],
                [sql_memory_model],
                [sql_memory_model_desc],
                [socket_count],
                [cores_per_socket],
                [numa_node_count],
                '' AS [container_type],
                '' AS [container_type_desc]
            FROM [sys].[dm_os_sys_info]")
        } ELSEIf (($Sqlversion).SubString(0,2) -in("13")) {  
            $global:TsqlInstance.Add("48.004~[sys_dm_os_sys_info]","
            SELECT @@SERVERNAME AS SQLInstance, 
                [cpu_ticks],
                [ms_ticks],
                [cpu_count],
                [hyperthread_ratio],
                [physical_memory_kb],
                [virtual_memory_kb],
                [committed_kb],
                [committed_target_kb],
                [visible_target_kb],
                [stack_size_in_bytes],
                [os_quantum],
                [os_error_mode],
                [os_priority_class],
                [max_workers_count],
                [scheduler_count],
                [scheduler_total_count],
                [deadlock_monitor_serial_number],
                [sqlserver_start_time_ms_ticks],
                [sqlserver_start_time],
                [affinity_type],
                [affinity_type_desc],
                [process_kernel_time_ms],
                [process_user_time_ms],
                [time_source],
                [time_source_desc],
                [virtual_machine_type],
                [virtual_machine_type_desc],
                [softnuma_configuration],
                [softnuma_configuration_desc],
                '' AS [process_physical_affinity],
                [sql_memory_model],
                [sql_memory_model_desc],
                [socket_count],
                [cores_per_socket],
                [numa_node_count],
                '' AS [container_type],
                '' AS [container_type_desc]
            FROM [sys].[dm_os_sys_info]")
        } ELSEIF (($SqlVersion).SubString(0,2) -in("14","15","16","17")) {  
            $global:TsqlInstance.Add("48.005~[sys_dm_os_sys_info]","
            SELECT @@SERVERNAME AS SQLInstance, * FROM [sys].[dm_os_sys_info]")
        }    

        ## 50 sys.dm_os_volume_stats
        IF (($SqlVersion).SubString(0,2) -in("11","12","13","14","15","16","17")) {  
            $global:TsqlInstance.Add("50.001~[sys_dm_os_volume_stats]","SELECT DISTINCT @@SERVERNAME AS SQLInstance, 
                    DB_NAME(mf.database_id) AS dbName,
                    mf.type_desc AS file_type,
                    mf.physical_name,
                    vs.volume_mount_point,
                    vs.logical_volume_name,
                    CAST(vs.total_bytes / 1073741824.0 AS decimal(18,2)) AS volume_total_gb,
                    CAST(vs.available_bytes / 1073741824.0 AS decimal(18,2)) AS volume_free_gb,
                    CAST(
                        100.0 * vs.available_bytes / NULLIF(vs.total_bytes, 0)
                        AS decimal(6,2)
                    ) AS volume_free_percent
                FROM sys.master_files AS mf
                CROSS APPLY sys.dm_os_volume_stats(mf.database_id, mf.file_id) AS vs")   
        } 

        #############################
        ## Database Specific Queries
        $global:TsqlDatabase = @{}

        ## 18 DatabaseSizeInfo
        IF (($SqlVersion).SubString(0,2) -in("10","11","12","13","14","15","16","17")) {  
            $global:TsqlDatabase.Add("18.001~DatabaseSizeInfo","SELECT @@ServerName, '@@@@' AS dbName, name AS [File Name] , physical_name AS [Physical Name],
                size/128.0 AS [Total Size in MB], size/128.0 - CAST(FILEPROPERTY(name, 'SpaceUsed') AS int)/128.0 AS [Available Space In MB], [file_id] 
                FROM [@@@@].sys.database_files")    
        }     

        ## 19 database_audit_specifications
        If (($Sqlversion).SubString(0,2) -in("10","11","12","13","14","15")) {  
            $global:TsqlDatabase.Add("19.001~[sys].[database_audit_specifications]","SELECT @@SERVERNAME AS SQLInstance, '@@@@' AS dbName,*,
                '' AS [is_session_context_enabled], 
                '' AS [session_context_keys] FROM [sys].[database_audit_specifications]")
        } ELSEIF (($SqlVersion).SubString(0,2) -in("16","17")) {  
            $global:TsqlDatabase.Add("19.001~[sys].[database_audit_specifications]","SELECT @@SERVERNAME AS SQLInstance, '@@@@' AS dbName,* FROM [sys].[database_audit_specifications]")
        }
                
        ## 20 database_audit_specification_details
        IF (($SqlVersion).SubString(0,2) -in("10","11","12","13","14","15","16","17")) {                                              
            $global:TsqlDatabase.Add("20.001~[sys].[database_audit_specification_details]","SELECT @@SERVERNAME AS SQLInstance,  '@@@@' AS dbName,* FROM [sys].[database_audit_specification_details]")     
        }     

        ## 21 IndexInfoNonClustered
        IF (($SqlVersion).SubString(0,2) -in("10","11","12","13","14","15","16","17")) {  
            $global:TsqlDatabase.Add("21.001~IndexInfoNonClustered","SELECT @@SERVERNAME AS SQLInstance, '@@@@' AS dbName, [t].[name] AS [Table], [i].[name] AS [Index], [p].[partition_number] AS [Partition], [p].[data_compression_desc] AS [Compression]
                FROM [sys].[partitions] AS [p]
                INNER JOIN sys.tables AS [t] ON [t].[object_id] = [p].[object_id]
                INNER JOIN sys.indexes AS [i] ON [i].[object_id] = [p].[object_id] AND i.index_id = p.index_id
                WHERE [p].[index_id] not in (0,1) AND [p].[data_compression_desc] != 'NONE'")    
        }    

        ## 22 IndexInfoHeapClustered
        IF (($SqlVersion).SubString(0,2) -in("10","11","12","13","14","15","16","17")) {  
            $global:TsqlDatabase.Add("22.001~IndexInfoHeapClustered","SELECT @@SERVERNAME AS SQLInstance, '@@@@' AS dbName, [t].[name] AS [Table], [i].[name] AS [Index], [p].[partition_number] AS [Partition], [p].[data_compression_desc] AS [Compression]
                FROM [sys].[partitions] AS [p]
                INNER JOIN sys.tables AS [t] ON [t].[object_id] = [p].[object_id]
                INNER JOIN sys.indexes AS [i] ON [i].[object_id] = [p].[object_id]
                WHERE [p].[index_id] in (0,1) AND [p].[data_compression_desc] != 'NONE'")  
        }      

        ## 23 partition_functions
        IF (($SqlVersion).SubString(0,2) -in("10")) {  
            $global:TsqlDatabase.Add("23.001~[sys].[partition_functions]","SELECT @@SERVERNAME AS SQLInstance, '@@@@' AS dbName, 
                [name],[function_id],[type],[type_desc],[fanout],[boundary_value_on_right],'' AS [is_system],[create_date],[modify_date] FROM [sys].[partition_functions]")
        } ELSEIF (($SqlVersion).SubString(0,2) -in("11","12","13","14","15","16","17")) {  
            $global:TsqlDatabase.Add("23.001~[sys].[partition_functions]","SELECT @@SERVERNAME AS SQLInstance, '@@@@' AS dbName, * FROM [sys].[partition_functions]")
        }

        ## 24 partition_range_values
        IF (($SqlVersion).SubString(0,2) -in("10","11","12","13","14","15","16","17")) {  
            $global:TsqlDatabase.Add("24.001~[sys].[partition_range_values]","SELECT @@SERVERNAME AS SQLInstance, '@@@@' AS dbName,  * FROM [sys].[partition_range_values]")
        }
        
        ## 25 partition_parameters
        IF (($SqlVersion).SubString(0,2) -in("10","11","12","13","14","15","16","17")) {  
            $global:TsqlDatabase.Add("25.001~[sys].[partition_parameters]","SELECT @@SERVERNAME AS SQLInstance, '@@@@' AS dbName,  * FROM [sys].[partition_parameters]")
        }

        ## 26 partition_schemes
        IF (($SqlVersion).SubString(0,2) -in("10")) {                  
            $global:TsqlDatabase.Add("26.001~[sys].[partition_schemes]","SELECT @@SERVERNAME AS SQLInstance, '@@@@' AS dbName,  
                [name],[data_space_id],[type],[type_desc],[is_default],'' AS [is_system],[function_id] FROM [sys].[partition_schemes]")
        } ELSEIF (($SqlVersion).SubString(0,2) -in("11","12","13","14","15","16","17")) {  
            $global:TsqlDatabase.Add("26.001~[sys].[partition_schemes]","SELECT @@SERVERNAME AS SQLInstance, '@@@@' AS dbName,  * FROM [sys].[partition_schemes]")
        }

        ## 27 partitions
        If (($Sqlversion).SubString(0,2) -in("10","11","12","13","14","15")) {  
            $global:TsqlDatabase.Add("27.001~[sys].[partitions]","SELECT @@SERVERNAME AS SQLInstance, '@@@@' AS dbName,  *, '' AS [xml_compression], '' AS [xml_compression_desc] FROM [sys].[partitions]")
        } ELSEIF (($SqlVersion).SubString(0,2) -in("16","17")) {  
            $global:TsqlDatabase.Add("27.001~[sys].[partitions]","SELECT @@SERVERNAME AS SQLInstance, '@@@@' AS dbName FROM [sys].[partitions]")
        }
   
        ## 28 objects
        IF (($SqlVersion).SubString(0,2) -in("10","11","12","13","14","15","16","17")) {  
            $global:TsqlDatabase.Add("28.001~[sys].[objects]","SELECT @@SERVERNAME AS SQLInstance, '@@@@' AS dbName,  * FROM [sys].[objects]")   
        }      
        
        ## 41 missing_indexes
        IF (($SqlVersion).SubString(0,2) -in("10","11","12","13","14","15","16","17")) {  
            $global:TsqlDatabase.Add("41.001~[missing_indexes]","SELECT @@SERVERNAME AS SQLInstance, '@@@@' AS dbName, 
                d.index_handle, DB_NAME(database_id) AS db_name, object_id, equality_columns, inequality_columns, included_columns, statement, s.avg_total_user_cost , s.avg_user_impact, s.last_user_seek,s.unique_compiles 
                FROM sys.dm_db_missing_index_group_stats s ,sys.dm_db_missing_index_groups g,sys.dm_db_missing_index_details d
                WHERE s.group_handle = g.index_group_handle and d.index_handle = g.index_handle")   
        }      

        ## 42 index_read_write_stats
        IF (($SqlVersion).SubString(0,2) -in("10","11","12","13","14","15","16","17")) {  
            $global:TsqlDatabase.Add("42.001~[index_read_write_stats]","SELECT @@SERVERNAME AS SQLInstance, '@@@@' AS dbName, 
                OBJECT_NAME(s.[object_id]) AS [ObjectName], i.name AS [IndexName], i.index_id, s.user_updates AS [Writes], user_seeks + user_scans + user_lookups AS [Reads], i.type_desc AS [IndexType], i.fill_factor AS [FillFactor]
                FROM sys.dm_db_index_usage_stats AS s
                INNER JOIN sys.indexes AS i ON s.[object_id] = i.[object_id] 
                WHERE OBJECTPROPERTY(s.[object_id],'IsUserTable') = 1 AND i.index_id = s.index_id AND s.database_id = DB_ID()
                OPTION (RECOMPILE);")   
        }    
        
        ## 43 underutilized_NC_indexes
        IF (($SqlVersion).SubString(0,2) -in("10","11","12","13","14","15","16","17")) {  
            $global:TsqlDatabase.Add("43.001~[underutilized_NC_indexes]","SELECT @@SERVERNAME AS SQLInstance, '@@@@' AS dbName, 
                OBJECT_NAME(s.[object_id]) AS [Table Name], i.name AS [Index Name], i.type_desc AS IndexType, i.index_id, user_updates AS [Total Writes], user_seeks + user_scans + user_lookups AS [Total Reads],user_updates - (user_seeks + user_scans + user_lookups) AS [Difference]
                FROM sys.dm_db_index_usage_stats AS s WITH (NOLOCK)
                INNER JOIN sys.indexes AS i WITH (NOLOCK) ON s.[object_id] = i.[object_id] AND i.index_id = s.index_id
                WHERE OBJECTPROPERTY(s.[object_id],'IsUserTable') = 1 AND s.database_id = DB_ID() AND user_updates > (user_seeks + user_scans + user_lookups) AND i.index_id > 1
                OPTION (RECOMPILE);")   
        }    
        
        ## 49 sys_database_files
        IF (($SqlVersion).SubString(0,2) -in("10","11","12","13")) {  
            $global:TsqlDatabase.Add("49.001~[sys_database_files]","SELECT @@SERVERNAME AS SQLInstance, '@@@@' AS dbName, 
                    [file_id],
                    [file_guid],
                    [type],
                    [type_desc],
                    [data_space_id],
                    [name],
                    [physical_name],
                    [state],
                    [state_desc],
                    [size],
                    [max_size],
                    [growth],
                    [is_media_read_only],
                    [is_read_only],
                    [is_sparse],
                    [is_percent_growth],
                    [is_name_reserved],
                    NULL AS [is_persistent_log_buffer],
                    [create_lsn],
                    [drop_lsn],
                    [read_only_lsn],
                    [read_write_lsn],
                    [differential_base_lsn],
                    [differential_base_guid],
                    [differential_base_time],
                    [redo_start_lsn],
                    [redo_start_fork_guid],
                    [redo_target_lsn],
                    [redo_target_fork_guid],
                    [backup_lsn]
                FROM [sys].[database_files];")   
        } ELSEIF (($SqlVersion).SubString(0,2) -in("14","15","16","17")) {  
            $global:TsqlDatabase.Add("49.002~[sys_database_files]","SELECT @@SERVERNAME AS SQLInstance, '@@@@' AS dbName,
                    [file_id],
                    [file_guid],
                    [type],
                    [type_desc],
                    [data_space_id],
                    [name],
                    [physical_name],
                    [state],
                    [state_desc],
                    [size],
                    [max_size],
                    [growth],
                    [is_media_read_only],
                    [is_read_only],
                    [is_sparse],
                    [is_percent_growth],
                    [is_name_reserved],
                    [is_persistent_log_buffer],
                    [create_lsn],
                    [drop_lsn],
                    [read_only_lsn],
                    [read_write_lsn],
                    [differential_base_lsn],
                    [differential_base_guid],
                    [differential_base_time],
                    [redo_start_lsn],
                    [redo_start_fork_guid],
                    [redo_target_lsn],
                    [redo_target_fork_guid],
                    [backup_lsn]
                FROM [sys].[database_files];")  
        } 
        
        ## Transactional Replication Specific Queries
        $global:TsqlReplicatedDatabase = @{}

        IF (($SqlVersion).SubString(0,2) -in("10","11","12","13","14","15","16","17")) {  
            $global:TsqlReplicatedDatabase.Add("29.001~[sys].[ReplObjects]","SELECT DISTINCT @@SERVERNAME AS SQLInstance, '@@@@' AS dbName,  sp.name AS PublicationName, sa.name AS ArticleName, o.name AS ObjectName, ss.srvname AS SubscriberServerName, 
                ss.dest_db AS SubscriberDBName FROM dbo.sysarticles sa JOIN dbo.syspublications sp on sa.pubid = sp.pubid
                LEFT OUTER JOIN dbo.syssubscriptions ss on ss.artid = sa.artid JOIN sys.objects o ON sa.objid = o.object_id")
        }

        ## SSRS Specific Queries
        $global:TsqlSsrsDatabases = @{}

        IF (($SqlVersion).SubString(0,2) -in("10","11","12","13","14","15","16","17")) {  
            $global:TsqlSsrsDatabases.Add("30.001~[dbo].[Catalog]","SELECT @@SERVERNAME AS SQLInstance, * FROM [dbo].[Catalog]")
            $global:TsqlSsrsDatabases.Add("31.001~[dbo].[DataSets]","SELECT @@SERVERNAME AS SQLInstance, * FROM [dbo].[DataSets]")
            $global:TsqlSsrsDatabases.Add("32.001~[dbo].[ActiveSubscriptions]","SELECT @@SERVERNAME AS SQLInstance, * FROM [dbo].[ActiveSubscriptions]")
            $global:TsqlSsrsDatabases.Add("33.001~[dbo].[Subscriptions]","SELECT @@SERVERNAME AS SQLInstance, * FROM [dbo].[Subscriptions]")
            $global:TsqlSsrsDatabases.Add("34.001~[dbo].[ReportSchedule]","SELECT @@SERVERNAME AS SQLInstance, * FROM [dbo].[ReportSchedule]")
            $global:TsqlSsrsDatabases.Add("35.001~[dbo].[DataSource]","SELECT @@SERVERNAME AS SQLInstance, * FROM [dbo].[DataSource]")
        }

        ## SSIS Specific Queries
        $global:TsqlSsisDatabases = @{}

        IF (($SqlVersion).SubString(0,2) -in("10","11","12","13","14","15","16","17")) {  
            $global:TsqlSsisDatabases.Add("36.001~[internal].[folders]","SELECT @@SERVERNAME AS SQLInstance, * FROM [internal].[folders]")
            $global:TsqlSsisDatabases.Add("37.001~[internal].[projects]","SELECT @@SERVERNAME AS SQLInstance, * FROM [internal].[projects]")
            $global:TsqlSsisDatabases.Add("38.001~[internal].[packages]","SELECT @@SERVERNAME AS SQLInstance, * FROM [internal].[packages]")
            $global:TsqlSsisDatabases.Add("39.001~[internal].[environments]","SELECT @@SERVERNAME AS SQLInstance, * FROM [internal].[environments]")
        }
    } CATCH {
        IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
        LogActivity "** ERROR: LoadTSqlArray() : $ErrorMsg" $True
        Exit_Script -ErrorRaised $True
    }
}

Function CollectConnectionInfoOnly()
{
    TRY {
        ForEach ($Server in $global:AllServers)
        {
            IF (!([string]::IsNullOrWhiteSpace($Server))) { 
                $FormattedServer = ( FormatServerName $Server )
                IF ( HasSqlAgent $Server ) {
                        			        
                    $sql = "BEGIN TRANSACTION
                    DECLARE @ReturnCode INT
                    SELECT @ReturnCode = 0

		            IF (SELECT 1 FROM [msdb].[dbo].[sysjobs] WHERE name = 'CollectConnections') IS NULL
		            BEGIN

				        DECLARE @jobId BINARY(16)
				        EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'CollectConnections', 
						        @enabled=1, 
						        @notify_level_eventlog=0, 
						        @notify_level_email=0, 
						        @notify_level_netsend=0, 
						        @notify_level_page=0, 
						        @delete_level=0, 
						        @description=N'No description available.', 
						        @category_name=N'[Uncategorized (Local)]', 
						        --@owner_login_name=N'sa', 
								@job_id = @jobId OUTPUT
				        IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

				        EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Collect Connections', 
						        @step_id=1, 
						        @cmdexec_success_code=0, 
						        @on_success_action=1, 
						        @on_success_step_id=0, 
						        @on_fail_action=2, 
						        @on_fail_step_id=0, 
						        @retry_attempts=0, 
						        @retry_interval=0, 
						        @os_run_priority=0, @subsystem=N'TSQL', 
						        @command=N'IF OBJECT_ID(N''dbo.Connections'', N''U'') IS NULL
					        CREATE TABLE [MVA-Data-Collection].[dbo].[Connections](
                                [SQLInstance] [nvarchar](128) NULL,
                                [ClientAddress] [nvarchar](48) NULL,
                                [ProgramName] [nvarchar](128) NULL,
                                [HostName] [nvarchar](128) NULL,
                                [LoginName] [nvarchar](128) NULL,
                                [database_name] [nvarchar](128) NULL,
                                [ConnectionCount] [int] NULL,
                                [collect_date] [datetime] NULL
					        ) ON [PRIMARY];

				        INSERT INTO [MVA-Data-Collection].[dbo].[Connections] 
				        SELECT DISTINCT @@SERVERNAME AS SQLInstance, ec.client_net_address as ClientAddress, es.[program_name] as ProgramName, es.[host_name] as HostName, 
                            es.login_name as LoginName, DB_NAME(er.database_id) AS database_name, COUNT(ec.session_id) AS ConnectionCount,getdate() as collect_date 
                        FROM sys.dm_exec_sessions es
                            JOIN sys.dm_exec_connections ec ON es.session_id = ec.session_id
                            LEFT JOIN sys.dm_exec_requests er ON es.session_id = er.session_id
                        WHERE es.is_user_process = 1
                        GROUP BY ec.client_net_address, es.[program_name], es.[host_name], es.login_name, DB_NAME(er.database_id) 
                        ORDER BY ec.client_net_address, es.[program_name] OPTION (RECOMPILE);', 
						        @database_name=N'master', 
						        @flags=0
				        IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

				        EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
				        IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

				        EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'Every 5 Mins', 
						        @enabled=1, 
						        @freq_type=4, 
						        @freq_interval=1, 
						        @freq_subday_type=4, 
						        @freq_subday_interval=5, 
						        @freq_relative_interval=0, 
						        @freq_recurrence_factor=0, 
						        @active_start_date=20231004, 
						        @active_end_date=99991231, 
						        @active_start_time=0, 
						        @active_end_time=235959
				        IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

				        EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
				        IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
			        END

			        COMMIT TRANSACTION
			        GOTO EndSave

			        QuitWithRollback:
				        IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
			        EndSave:

                    IF NOT EXISTS (SELECT 1 FROM [MVA-Data-Collection].[dbo].[sysobjects] WHERE name = 'Connections') 
                    BEGIN
	                    CREATE TABLE [MVA-Data-Collection].[dbo].[Connections](
                            [SQLInstance] [nvarchar](128) NULL,
                            [ClientAddress] [nvarchar](48) NULL,
                            [ProgramName] [nvarchar](128) NULL,
                            [HostName] [nvarchar](128) NULL,
                            [LoginName] [nvarchar](128) NULL,
                            [database_name] [nvarchar](128) NULL,
                            [ConnectionCount] [int] NULL,
                            [collect_date] [datetime] NULL
	                    ) ON [PRIMARY];
                    END

				    INSERT INTO [MVA-Data-Collection].[dbo].[Connections] 
                    SELECT DISTINCT @@SERVERNAME AS SQLInstance, ec.client_net_address as ClientAddress, es.[program_name] as ProgramName, es.[host_name] as HostName, 
                        es.login_name as LoginName, DB_NAME(er.database_id) AS database_name, COUNT(ec.session_id) AS ConnectionCount,getdate() as collect_date 
                    FROM sys.dm_exec_sessions es
                        JOIN sys.dm_exec_connections ec ON es.session_id = ec.session_id
                        LEFT JOIN sys.dm_exec_requests er ON es.session_id = er.session_id
                    WHERE es.is_user_process = 1
                    GROUP BY ec.client_net_address, es.[program_name], es.[host_name], es.login_name, DB_NAME(er.database_id) 
                    ORDER BY ec.client_net_address, es.[program_name] OPTION (RECOMPILE);
                    "
                        
                    Invoke-Sql -ServerInstance $Server -Database 'master' -Query "IF NOT EXISTS (SELECT 1 FROM [master].[dbo].[sysdatabases] WHERE name = 'MVA-Data-Collection') CREATE DATABASE [MVA-Data-Collection];"

                    TRY {
                        (Invoke-Sql -ServerInstance $Server -Database 'msdb' -Query $sql) 
                    } CATCH {
                        IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
                        Invoke-Sql -ServerInstance $Server -Database 'msdb' -Query "IF EXISTS (SELECT 1 FROM [master].[dbo].[sysdatabases] WHERE name = 'MVA-Data-Collection') DROP DATABASE [MVA-Data-Collection];"
                        LogActivity "** ERROR: Configuring database connection collections job : $ErrorMsg" $True
                    }                        

                    $ExportPath = FormatString -InputString $("$ScriptRoot\Export\Connections\$FormattedServer\")
                    IF (!(test-path $ExportPath)) { New-Item -ItemType Directory -Force -Path $ExportPath | Out-Null }

                    TRY {
                        ExportData $Server "00.002~ConnectionInfo" "SELECT * FROM [MVA-Data-Collection].[dbo].[Connections]" 
                    } CATCH {
                        IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
                        LogActivity "** ERROR: Collecting [dbo].[Connections] : $ErrorMsg" $True
                    }
                } Else {
                    $ExportPath = FormatString -InputString $("$ScriptRoot\Export\$datestamp\$FormattedServer\")
                    IF (!(test-path $ExportPath)) { New-Item -ItemType Directory -Force -Path $ExportPath | Out-Null }
                    LogActivity "** INFO: $Server is running Express Edition : No Agent Job can be created" $True
                    ExportData $Server "00.002~ConnectionInfo" $global:TsqlInstance.'00.002~ConnectionInfo' 
                }
            }
        }
        Compress-Archive -Path $(FormatString -InputString $("$ScriptRoot\Export\Connections\")) -DestinationPath $(FormatString -InputString $("$ScriptRoot\Export\$datestamp\$Customer-MVA-Export-Connections.zip")) -update
        Exit_Script -ErrorRaised $False
    } CATCH {
        IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
        LogActivity "** ERROR: CollectConnectionInfoOnly() : $ErrorMsg" $True
        Exit_Script -ErrorRaised $True
    }
}

Function ValidateRegions()
{
    Param ([System.Array]$Regions)
    TRY {
        $Pass = $True
        ForEach ($region in $Regions) {
            IF (!([string]::IsNullOrWhiteSpace($region))) { 
                TRY {
                    $output = ''
                    $output = aws ec2 describe-regions --filters "Name=region-name,Values=$region" --query 'Regions[0].[RegionName]' 2>$null
                    IF (!([string]::IsNullOrWhiteSpace($output))) { 
                        IF ($ValidateResourcesOnly) { LogActivity "** INFO: Region $region has been validated" $True } ELSE { LogActivity "** INFO: Region $region has been validated" $False }
                    } Else {
                        $Pass = $False
                        LogActivity "** ERROR: Region $region has failed validation" $True
                    }
                } CATCH {
                    IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
                    LogActivity "** ERROR: Call to Get-EC2Region -RegionName $region had errors : $ErrorMsg" $True
                    $Pass = $False
                }
            }
        }
    } CATCH {
        IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
        LogActivity "** ERROR: ValidateRegions() : $ErrorMsg" $True
        $Pass = $False
    }
    Return $Pass    
} 

Function ValidateEC2Info
{
    Param ([System.Array]$EC2InstanceIds)
    TRY {
        $Pass = $True
        ForEach ($instance in $EC2InstanceIds) {
            IF (!([string]::IsNullOrWhiteSpace($instance))) { 
                TRY {
                    $output = ''
                    ForEach ($region in $Regions) {
                        IF (!([string]::IsNullOrWhiteSpace($region))) {
                            IF ([string]::IsNullOrWhiteSpace($output)) {
                                TRY {
                                    $output = aws ec2 describe-instances --instance-ids $instance --region $region 2>$null | ConvertFrom-JSON
                                } CATCH {
                                    ## Ignore
                                }
                            }
                        }
                    }
                    IF ( (!([string]::IsNullOrWhiteSpace($output))) -and ( $output.Reservations.Instances.InstanceId -eq $instance )) { 
                        IF ($Output.Reservations.Instances.State.Name -ne 'running') {
                            $Pass = $False
                            LogActivity "** ERROR: Instance $instance is Not running" $True
                        } Else {
                            IF ($ValidateResourcesOnly) {
                                LogActivity "** INFO: Instance $instance has been validated" $True 
                            } ELSE { 
                                LogActivity "** INFO: Instance $instance has been validated" $False 
                            }
                        }
                    } Else {
                        $Pass = $False
                        LogActivity "** ERROR: Instance $instance has failed validation" $True
                    }
                } CATCH {
                    IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
                    LogActivity "** ERROR: Call to describe-instances --instance-ids $instance had errors : $ErrorMsg" $True
                    $Pass = $False
                }

                TRY {
                    IF ($Output.Reservations.Instances.State.Name -eq 'running') {
                        ## Locate Private/Public IP for InstanceIDs
                        IF ($UsePublicIPs) {
                            $DnsName = $output.Reservations.Instances.PublicDnsName  
                            LogActivity "** INFO: Instance $instance Public DNS Name Found: $DnsName" $False
                        } Else {
                            $DnsName = $output.Reservations.Instances.PrivateDnsName 
                            LogActivity "** INFO: Instance $instance Private DNS Name Found: $DnsName" $False
                        }
                    
                        IF ($output.Reservations.Instances.tags.Key -eq "Name") {
                            $name = $output.Reservations.Instances.tags | Where-Object { $_.Key -eq "Name" } | Select-Object -expand Value
                        } Else { 
                            $name = ''
                        }
                        $InstanceIdDetails.Add($instance, "$DnsName|$name")
                    }
                } CATCH {
                    IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
                    LogActivity "** ERROR: Unable to Identify IP addresses associated with Instances : $ErrorMsg" $True
                    $Pass = $False
                }
            }
        }
    } CATCH {
        IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
        LogActivity "** ERROR: ValidateEC2Info() : $ErrorMsg" $True
        $Pass = $False
    }
    Return $Pass
} 

Function ValidateRdsInfo()
{
    Param ([System.Array]$RdsInstances)
    TRY {
        $Pass = $True
        ForEach ($rds in $RdsInstances) {
            IF (!([string]::IsNullOrWhiteSpace($rds))) { 
                TRY {
                    $output = ''
                    ForEach ($region in $Regions) {
                        IF (!([string]::IsNullOrWhiteSpace($region))) {
                            IF ([string]::IsNullOrWhiteSpace($output)) {
                                $output = aws rds describe-db-instances --db-instance-identifier $rds --region $region 2>$null
                            }
                        }
                    }
                    IF ( (!([string]::IsNullOrWhiteSpace($output))) -and ( (($output | ConvertFrom-JSON).DBInstances.DBInstanceIdentifier) -eq $rds )) { 
                        $DBInstanceIdentifier = ($output | ConvertFrom-JSON).DBInstances.DBInstanceIdentifier
                        $RdsEndPoint = ($output | ConvertFrom-JSON).DBInstances.Endpoint.Address
                        $RdsPort = ($output | ConvertFrom-JSON).DBInstances.Endpoint.Port  

                        IF ( ($output | ConvertFrom-JSON).DBInstances.Engine -like "sqlserver*") { 
                            # $DBInstanceIdentifier = ($output | ConvertFrom-JSON).DBInstances.DBInstanceIdentifier
                            # $RdsEndPoint = ($output | ConvertFrom-JSON).DBInstances.Endpoint.Address
                            # $RdsPort = ($output | ConvertFrom-JSON).DBInstances.Endpoint.Port
                            IF (!([string]::IsNullOrWhiteSpace($RdsEndPoint))) {
                                $RdsDetails.Add($DBInstanceIdentifier, "$RdsEndPoint|$RdsPort")
                                IF ($ValidateResourcesOnly) { LogActivity "** INFO: RDS SQL Server Node $rds has been validated" $True } ELSE { LogActivity "** INFO: RDS SQL Server Node $rds has been validated" $False }
                            } Else {
                                $Pass = $False
                                LogActivity "** ERROR: RDS Node $rds has failed validation : No Endpoint detected" $True                                
                            }
                        } ElseIF ( ($output | ConvertFrom-JSON).DBInstances.Engine -like "mariadb*") {
                            $RdsDetails.Add($DBInstanceIdentifier, "$RdsEndPoint|$RdsPort")
                            IF ($ValidateResourcesOnly) { LogActivity "** INFO: RDS mariaDB Node $rds has been validated" $True } ELSE { LogActivity "** INFO: RDS MariaDB Node $rds has been validated" $False }                            
                        } Else {
                            $Pass = $False
                            LogActivity "** ERROR: RDS Node $rds is not a SQL Server Instance and has failed validation" $True
                        }
                    } Else {
                        $Pass = $False
                        LogActivity "** ERROR: RDS Node $rds has failed validation" $True
                    }
                } CATCH {
                    IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
                    LogActivity "** ERROR: Call to describe-db-instances --db-instance-identifier $rds had errors : $ErrorMsg" $True
                    $Pass = $False
                }
            }
        }
    } CATCH {
        IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
        LogActivity "** ERROR: ValidateRdsInfo() : $ErrorMsg" $True
        $Pass = $False
    }
    Return $Pass    
} 

Function ValidateFsxInfo()
{
    Param ([System.Array]$FSxFileSystems)
    TRY {
        $Pass = $True
        ForEach ($fsx in $FSxFileSystems) {
            IF (!([string]::IsNullOrWhiteSpace($fsx))) { 
                TRY {
                    $output = ''
                    ForEach ($region in $Regions) {
                        IF (!([string]::IsNullOrWhiteSpace($region))) {
                            IF ([string]::IsNullOrWhiteSpace($output)) {
                                $output = aws fsx describe-file-systems --file-system-ids $fsx --region $region 2>$null
                            }
                        }
                    }
                    IF ( (!([string]::IsNullOrWhiteSpace($output))) -and ( (($output | ConvertFrom-JSON).FileSystems.FileSystemId) -eq $fsx )) { 
                        IF ($ValidateResourcesOnly) { LogActivity "** INFO: FSx FileSystem $fsx has been validated" $True } ELSE { LogActivity "** INFO: FSx FileSystem $fsx has been validated" $False }
                    } Else {
                        $Pass = $False
                        LogActivity "** ERROR: FSx FileSystem $fsx has failed validation" $True
                    }
                } CATCH {
                    IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
                    LogActivity "** ERROR: Call to describe-file-systems --file-system-ids $fsx had errors : $ErrorMsg" $True
                    $Pass = $False
                }
            }
        }
    } CATCH {
        IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
        LogActivity "** ERROR: ValidateFsxInfo() : $ErrorMsg" $True
        $Pass = $False
    }
    Return $Pass    
} 

Function ValidateServerConnectivity()
{
    Param (
        [System.Array]$EC2InstanceIds,
        [System.Array]$EC2Servers,
        [System.Array]$RdsInstances
    )
    TRY {
        $Pass = $True
        $sql = "SELECT TOP 1 name FROM [master].[sys].[databases] WHERE name = 'master'"

        ForEach ($item in $EC2InstanceIds) {
            IF (!([string]::IsNullOrWhiteSpace($item))) {
                $output = ''
                $namePass = $False
                $ipPass = $False
                TRY {
                    ## Try AWS DNS first, then name
                    $output = (Invoke-Sql -ServerInstance ($InstanceIdDetails."$item").split('|')[0]  -Database 'master' -Query $sql -Timeout 15 -RaiseError $False) 
                    IF ($output.name -eq 'master') {
                        $ipPass = $True
                    } Else {
                        $output = (Invoke-Sql -ServerInstance ($InstanceIdDetails."$item").split('|')[1]  -Database 'master' -Query $sql -Timeout 15 -RaiseError $False) 
                        IF ($output.name -eq 'master') {
                            $namePass = $True
                        } 
                    }
                        
                    IF ($namePass -or $ipPass) { 
                        IF ($ValidateResourcesOnly) { LogActivity "** INFO: EC2 InstanceId $item SQL Connectivity has been validated" $True } ELSE { LogActivity "** INFO: EC2 InstanceId $item SQL Connectivity has been validated" $False }          
                        IF ($ipPass) { 
                            $global:AllServers += ($InstanceIdDetails."$item").split('|')[0] 
                            $global:AllServersByIp += GetServerIP -Server ($InstanceIdDetails."$item").split('|')[0]
                        } Else { 
                            $global:AllServers += ($InstanceIdDetails."$item").split('|')[1]
                            $global:AllServersByIp += GetServerIP -Server ($InstanceIdDetails."$item").split('|')[1]
                        }
                    } Else {
                        $Pass = $False
                        LogActivity "** ERROR: Unable to Connect to SQL Instance $item : SQL Connectivity has failed validation" $True
                        LogActivity "** ERROR: Unable to Connect to SQL Instance $item : SQL Connectivity has failed validation : $error" $False
                    }                            
                } CATCH {
                    $Pass = $False
                    IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
                    LogActivity "** ERROR: Unable to Connect to SQL Instance $item : SQL Connectivity has failed validation" $True
                    LogActivity "** ERROR: Unable to Connect to SQL Instance $item : SQL Connectivity has failed validation : $ErrorMsg" $False
                }
            }
        }

        ForEach ($item in $RdsInstances) {
            IF (!([string]::IsNullOrWhiteSpace($item))) {
                $output = ''
                TRY {
                    $output = (Invoke-Sql -ServerInstance $RdsDetails."$item".split("|")[0] -Database 'master' -Query $sql -Timeout 15) 
                    IF (!([string]::IsNullOrWhiteSpace($output))) { 
                        IF ($ValidateResourcesOnly) { LogActivity "** INFO: RDS Instance $item SQL Connectivity has been validated" $True } ELSE { LogActivity "** INFO: RDS Instance $item SQL Connectivity has been validated" $False }
                        $global:AllServers += $RdsDetails."$item".split("|")[0]
                        $global:AllServersByIp += GetServerIP -Server $RdsDetails."$item".split("|")[0]
                    } Else {
                        $Pass = $False
                        LogActivity "** ERROR: Unable to Connect to SQL Instance $item : SQL Connectivity has failed validation" $True
                        LogActivity "** ERROR: Unable to Connect to SQL Instance $item : SQL Connectivity has failed validation : $error" $False
                    }
                } CATCH {
                    $Pass = $False
                    IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
                    LogActivity "** ERROR: Unable to Connect to SQL Instance $item : SQL Connectivity has failed validation" $True
                    LogActivity "** ERROR: Unable to Connect to SQL Instance $item : SQL Connectivity has failed validation : $ErrorMsg" $False
                }
            }
        }

        ForEach ($item in $EC2Servers) {
            IF (!([string]::IsNullOrWhiteSpace($item))) {
                $output = ''
                
                TRY {
                    $output = (Invoke-Sql -ServerInstance $item  -Database 'master' -Query $sql -Timeout 15) 
                    IF (!([string]::IsNullOrWhiteSpace($output))) { 
                        IF ( ( $global:AllServersByIp -contains (GetServerIP -Server $item))  -and (!($item -like '*\*' ))  ) {     
                            LogActivity "** INFO: Duplicate Entry Found: Skipping SQL Data Collection for $item" $True  
                        } ELSE {
                            IF ($ValidateResourcesOnly) { LogActivity "** INFO: EC2 Server $item SQL Connectivity has been validated" $True } ELSE { LogActivity "** INFO: EC2 Server $item SQL Connectivity has been validated" $False }
                            $global:AllServers += $item
                            $global:AllServersByIp += GetServerIP -Server $item
                        }     
                    } Else {
                        $Pass = $False
                        LogActivity "** ERROR: Unable to Connect to SQL Instance $item : SQL Connectivity has failed validation" $True
                        LogActivity "** ERROR: Unable to Connect to SQL Instance $item : SQL Connectivity has failed validation : $error" $False
                    }
                } CATCH {
                    $Pass = $False
                    IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
                    LogActivity "** ERROR: Unable to Connect to SQL Instance $item : SQL Connectivity has failed validation" $True
                    LogActivity "** ERROR: Unable to Connect to SQL Instance $item : SQL Connectivity has failed validation : $ErrorMsg" $False
                }
            }
        }
    } CATCH {
        IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
        LogActivity "** ERROR: ValidateServerConnectivity() : $ErrorMsg" $True
        $Pass = $False
    }
    Return $Pass
} 

Function GetServerIP
{
    Param ([String]$Server)
    TRY {
        $Server = $Server.split(",")[0].split("\")[0]

        $DNSResult = [System.Net.Dns]::GetHostAddresses($Server)
                
        IF ($DNSResult.Count -gt 0) {
            # Get the first IPv4 address if available, otherwise first address
            $IPv4Address = $DNSResult | Where-Object { $_.AddressFamily -eq 'InterNetwork' -and ($_.IPAddressToString -notlike '169.254*') } | Select-Object -First 1
            IF ($IPv4Address) {
                $PreferredIP = $IPv4Address.IPAddressToString
            } ELSE {
                $PreferredIP = $DNSResult[0].IPAddressToString
            }
        }
        LogActivity "** INFO: DNS Lookup for $Server : $PreferredIP" $False
        Return $PreferredIP
    } CATCH {
        IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
        LogActivity "** ERROR: GetServerIP() : $ErrorMsg" $True
        Exit_Script -ErrorRaised $True
    }
}

Function Exit_Script
{
    Param ([Bool]$ErrorRaised)
    TRY {
        IF ($ErrorRaised -eq $True) {
            LogActivity "** ERROR: Errors were detected! Data Collection is Incomplete." $True 
            LogActivity "** ERROR: Please send Error Log file $global:LogFile to mva@evolvecloudservices.com" $True 
            LogActivity "** INFO: ***********************************END OF LOGS ************************************* " $False  
            LogActivity $error $False  
            Exit
        } Else {
            IF ( ($CollectConnectionsOnly) -or ($ExportDacPacs) -or ($ExportDDL) -or ($CollectCloudWatchData) -or ($CollectTsqlData) ) {
                Compress-Archive -Path $(FormatString -InputString $("$ScriptRoot\Export\$datestamp\")) -DestinationPath $(FormatString -InputString $("$ScriptRoot\Export\$Customer-MVA-Export-ALL-$datestamp.zip"))
                LogActivity "** INFO: Data Collection is Complete" $True
                LogActivity "** INFO: Please review and send zip file $(FormatString -InputString $("$ScriptRoot\Export\$Customer-MVA-Export-ALL-$datestamp.zip")) to mva@evolvecloudservices.com" $True
            } ElseIf ($CleanUpEnvironment -and $global:Confirmation) {
                LogActivity "** INFO: Environment CleanUp is Complete" $True
            } ElseIf ($ValidateResourcesOnly) {
                LogActivity "** INFO: Server Connectivity and Validation is Complete" $True            
            } Else {
                LogActivity "** INFO: No Data Collected" $True
            }
            Exit
        }
    } CATCH {
        IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
        LogActivity "** ERROR: Exit_Script() : $ErrorMsg" $True
        Exit
    }
}

Function Invoke-Sql
{
    param( 
        [Parameter(Mandatory=$false)] [String]  $ServerInstance,
        [Parameter(Mandatory=$false)] [String]  $Database = 'master',
        [Parameter(Mandatory=$false)] [String]  $Query,
        [Parameter(Mandatory=$false)] [Int]     $Timeout,
        [Parameter(Mandatory=$false)] [Boolean] $RaiseError = $True,
        [Parameter(Mandatory=$false)] [Int]     $CommandTimeout = 0
     )
    TRY {
        $table = $Null

        IF ($Timeout -gt 0) {
            $ConnectionTimeout = $Timeout
        } Else {
            $ConnectionTimeout = $SqlServerConnectionTimeout
        }

        $conn = New-Object System.Data.SqlClient.SqlConnection
        If ([string]::IsNullOrWhiteSpace($SqlUser)) {
            $conn.ConnectionString = "Server=$ServerInstance;Database=$Database;Integrated Security=true;TrustServerCertificate=True;Connection Timeout=$ConnectionTimeout"
        } Else {
            $conn.ConnectionString = "Server=$ServerInstance;Database=$Database;User ID='$SqlUser';Password='$SqlPassword';TrustServerCertificate=True;Connection Timeout=$ConnectionTimeout"
        }
        $conn.Open()

        $cmd = $conn.CreateCommand()
        IF ($CommandTimeout -gt 0) {
            $cmd.CommandTimeout = $CommandTimeout
        } Else {
            $cmd.CommandTimeout = $SqlServerQueryTimeout
        }
        $cmd.CommandText = $Query

        $adapter = New-Object System.Data.SqlClient.SqlDataAdapter $cmd
        $table = New-Object System.Data.DataTable
        $adapter.Fill($table) | Out-Null

        $conn.Close()

        Return $table
    } CATCH {
        IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
        IF ($ErrorMsg -eq 'SQLServerAgent is not currently running so it cannot be notified of this action.') {
            LogActivity "** INFO: SQL Agent is not running on $ServerInstance : CollectConnections Job will not run" $True
        } Else {
            IF ($RaiseError) {
                ## inside a parallel worker ($MvaLogQueue exists only there) Exit_Script cannot end the run: hand the failure to the caller, whose
                ## own error line reports it once, instead of logging it here and again for every function it passes through
                IF ($Null -ne $MvaLogQueue) { Throw "Invoke-Sql() : $ServerInstance : $ErrorMsg" }
                LogActivity "** ERROR: Invoke-Sql() : $ServerInstance : $ErrorMsg" $False
                Exit_Script -ErrorRaised $True
            } Else {
                LogActivity "** ERROR: Invoke-Sql() : $ServerInstance : $ErrorMsg" $False
                Return $ErrorMsg
            }
        }
    }
}

function ExtractDDLScript {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)] $SqlVersion,
        [ValidateSet('Batches', 'Lines')] [string] $OutputMode = 'Batches',
        [switch] $EmitSections
    )

    # ---------------------------------------------------------------- version
    $majorText = ([string]$SqlVersion).Trim().Split('.')[0]
    $v = 0
    if (-not [int]::TryParse($majorText, [ref]$v)) { throw "ExtractDDLScript: cannot read a major version from '$SqlVersion'" }
    if ($v -lt 10) { throw "ExtractDDLScript: SQL Server major version $v is not supported (minimum is 10 = SQL Server 2008)." }

    $sections = New-Object System.Collections.Generic.List[object]
    # Catalog "metadata" string columns (*_desc, permission_name, ...) use the fixed catalog collation, while names/definitions use the
    # database collation. Concatenating the two raises "Cannot resolve collation conflict", so force every such column to the
    # database default. (Helper temp tables are created WITH that collation too.)
    $collatePattern = '\b([A-Za-z_][A-Za-z0-9_]*)\.((?:[a-z_0-9]+_desc)|permission_name|tessellation_scheme|encryption_algorithm_name|audit_action_name|name)\b(?!\s+COLLATE)'
    function Add-Section([string]$Name, [string]$Sql) {
        $Sql = [regex]::Replace($Sql, $collatePattern, '$0 COLLATE DATABASE_DEFAULT')
        $sections.Add([pscustomobject]@{ Name = $Name; Sql = $Sql })
    }
    $tok = @{}                                                       # {{TOKEN}} -> text
    function Fill([string]$Sql) { foreach ($k in $tok.Keys) { $Sql = $Sql.Replace('{{' + $k + '}}', [string]$tok[$k]) }; return $Sql }
    function Gate([bool]$On, [string]$Text, [string]$Else = '') { if ($On) { return $Text } else { return $Else } }

    # ---------------------------------------------------------------- reusable T-SQL snippets
    # Rendered data type for sys.columns / sys.parameters / sys.partition_parameters rows (alias $a, types alias t, xsc for xml).
    function TypeExpr([string]$a, [bool]$xml = $true) {
        $x = ''
        if ($xml) {
            $x = "WHEN t.name = 'xml' AND $a.xml_collection_id <> 0 THEN '('+CASE WHEN $a.is_xml_document=1 THEN 'DOCUMENT ' ELSE 'CONTENT ' END+QUOTENAME(SCHEMA_NAME(xsc.schema_id))+'.'+QUOTENAME(xsc.name)+')'"
        }
        return @"
CASE WHEN t.is_user_defined = 1
       THEN QUOTENAME(SCHEMA_NAME(t.schema_id))+'.'+QUOTENAME(t.name)
       ELSE QUOTENAME(t.name) + CASE
            WHEN t.name IN ('varchar','char','varbinary','binary') THEN '('+CASE WHEN $a.max_length=-1 THEN 'max' ELSE CAST($a.max_length AS varchar(10)) END+')'
            WHEN t.name IN ('nvarchar','nchar') THEN '('+CASE WHEN $a.max_length=-1 THEN 'max' ELSE CAST($a.max_length/2 AS varchar(10)) END+')'
            WHEN t.name IN ('decimal','numeric') THEN '('+CAST($a.precision AS varchar(10))+','+CAST($a.scale AS varchar(10))+')'
            WHEN t.name IN ('datetime2','time','datetimeoffset') THEN '('+CAST($a.scale AS varchar(10))+')'
            $x
            ELSE '' END
  END
"@
    }
    # Index column lists (outer index alias $i, inner alias ixc).
    function KeyCols([string]$i = 'i', [bool]$sort = $true) {
        $s = ''; if ($sort) { $s = "+CASE WHEN ixc.is_descending_key=1 THEN ' DESC' ELSE ' ASC' END" }
        return "STUFF((SELECT ', '+QUOTENAME(COL_NAME(ixc.object_id,ixc.column_id))$s FROM sys.index_columns ixc WHERE ixc.object_id=$i.object_id AND ixc.index_id=$i.index_id AND ixc.is_included_column=0 ORDER BY ixc.key_ordinal FOR XML PATH(''),TYPE).value('.','nvarchar(max)'),1,2,'')"
    }
    function InclCols([string]$i = 'i') {
        return "STUFF((SELECT ', '+QUOTENAME(COL_NAME(ixc.object_id,ixc.column_id)) FROM sys.index_columns ixc WHERE ixc.object_id=$i.object_id AND ixc.index_id=$i.index_id AND ixc.is_included_column=1 ORDER BY ixc.index_column_id FOR XML PATH(''),TYPE).value('.','nvarchar(max)'),1,2,'')"
    }
    function AllCols([string]$i = 'i') {   # columnstore indexes report every column as 'included'
        return "STUFF((SELECT ', '+QUOTENAME(COL_NAME(ixc.object_id,ixc.column_id)) FROM sys.index_columns ixc WHERE ixc.object_id=$i.object_id AND ixc.index_id=$i.index_id ORDER BY ixc.index_column_id FOR XML PATH(''),TYPE).value('.','nvarchar(max)'),1,2,'')"
    }
    $QN_T = "QUOTENAME(SCHEMA_NAME(t.schema_id))+'.'+QUOTENAME(t.name)"
    $QN_O = "QUOTENAME(SCHEMA_NAME(o.schema_id))+'.'+QUOTENAME(o.name)"

    # ---------------------------------------------------------------- version tokens
    $tok['V']            = $v
    $tok['HASHJOIN']     = Gate ($v -ge 12) 'LEFT JOIN sys.hash_indexes hi ON hi.object_id=i.object_id AND hi.index_id=i.index_id'
    $tok['BUCKET']       = Gate ($v -ge 12) 'ISNULL(CAST(hi.bucket_count AS varchar(20)),''0'')' "'0'"
    $tok['OSK_IDX']    = Gate ($v -ge 15) "+', OPTIMIZE_FOR_SEQUENTIAL_KEY='+CASE WHEN i.optimize_for_sequential_key=1 THEN 'ON' ELSE 'OFF' END"
    $tok['KEYCOLS_I']    = KeyCols 'i' $true
    $tok['KEYCOLS_I_NS'] = KeyCols 'i' $false
    $tok['INCL_I']       = InclCols 'i'
    $tok['ALL_I']        = AllCols 'i'
    $tok['TYPE_C']       = TypeExpr 'c' $true
    $tok['TYPE_P']       = TypeExpr 'p' $true
    $tok['TYPE_PP']      = TypeExpr 'pp' $false
    $tok['QN_T']         = $QN_T
    $tok['QN_O']         = $QN_O

    # table-kind classification expressions (columns that do not exist on old versions collapse to 0)
    $tok['K_LEDGERHIST'] = Gate ($v -ge 16) "(t.ledger_type=1 OR t.name LIKE 'MSSQL[_]DroppedLedgerTable[_]%')" '1=0'
    $tok['K_EXTERNAL']   = Gate ($v -ge 13) 't.is_external=1' '1=0'
    $tok['K_FILETABLE']  = Gate ($v -ge 11) 't.is_filetable=1' '1=0'
    $tok['K_MEMOPT']     = Gate ($v -ge 12) 't.is_memory_optimized=1' '1=0'
    $tok['K_NODE']       = Gate ($v -ge 14) 't.is_node=1' '1=0'
    $tok['K_EDGE']       = Gate ($v -ge 14) 't.is_edge=1' '1=0'

    # column definition pieces
    $tok['COL_ENC']      = Gate ($v -ge 13) "+ CASE WHEN c.encryption_type IS NOT NULL THEN ' ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = '+QUOTENAME(cek.name)+', ENCRYPTION_TYPE = '+c.encryption_type_desc+', ALGORITHM = '''+c.encryption_algorithm_name+''')' ELSE '' END"
    $tok['COL_MASK']     = Gate ($v -ge 13) "+ ISNULL(' MASKED WITH (FUNCTION = '''+mc.masking_function+''')','')"
    $tok['COL_GEN']      = Gate ($v -ge 13) "+ CASE WHEN c.generated_always_type=1 THEN ' GENERATED ALWAYS AS ROW START' WHEN c.generated_always_type=2 THEN ' GENERATED ALWAYS AS ROW END' ELSE '' END + CASE WHEN c.generated_always_type IN (1,2) AND c.is_hidden=1 THEN ' HIDDEN' ELSE '' END"
    $tok['COL_JOINS']    = (Gate ($v -ge 13) "LEFT JOIN sys.masked_columns mc ON mc.object_id=c.object_id AND mc.column_id=c.column_id`r`n      LEFT JOIN sys.column_encryption_keys cek ON cek.column_encryption_key_id=c.column_encryption_key_id")
    $tok['COL_FILTER']   = Gate ($v -ge 14) 'AND c.graph_type IS NULL'
    if ($v -ge 16) { $tok['COL_FILTER'] = 'AND c.graph_type IS NULL AND c.generated_always_type NOT IN (7,8,9,10)' }   # ledger system columns are added by LEDGER = ON
    $tok['PERIOD']       = Gate ($v -ge 13) "+ ISNULL((SELECT ','+@nl+'    PERIOD FOR SYSTEM_TIME ('+QUOTENAME(COL_NAME(p.object_id,p.start_column_id))+', '+QUOTENAME(COL_NAME(p.object_id,p.end_column_id))+')' FROM sys.periods p WHERE p.object_id=t.object_id),'')"
    $tok['TORD']         = Gate ($v -ge 13) 'CASE WHEN t.temporal_type=2 THEN 5 ELSE 0 END' '0'
    $tok['HISTJOIN']     = Gate ($v -ge 13) 'LEFT JOIN sys.tables ht ON ht.object_id=t.history_table_id'
    $tok['OPT_MEMOPT']   = Gate ($v -ge 12) "CASE WHEN k.kind='memopt' THEN ', MEMORY_OPTIMIZED = ON, DURABILITY = '+t.durability_desc ELSE '' END +" ''
    $tok['OPT_TEMPORAL'] = Gate ($v -ge 13) "+ CASE WHEN t.temporal_type=2 THEN ', SYSTEM_VERSIONING = ON (HISTORY_TABLE = '+QUOTENAME(SCHEMA_NAME(ht.schema_id))+'.'+QUOTENAME(ht.name)+')' ELSE '' END"
    if ($v -ge 14) {   # 2017+: add HISTORY_RETENTION_PERIOD
        $tok['OPT_TEMPORAL'] = "+ CASE WHEN t.temporal_type=2 THEN ', SYSTEM_VERSIONING = ON (HISTORY_TABLE = '+QUOTENAME(SCHEMA_NAME(ht.schema_id))+'.'+QUOTENAME(ht.name)+CASE WHEN t.history_retention_period_unit_desc IS NOT NULL AND t.history_retention_period_unit_desc<>'INFINITE' THEN ', HISTORY_RETENTION_PERIOD = '+CAST(t.history_retention_period AS varchar(10))+' '+t.history_retention_period_unit_desc+'S' ELSE '' END+')' ELSE '' END"
    }
    # ledger tables (2022+): updatable = SYSTEM_VERSIONING + LEDGER, append-only = LEDGER (APPEND_ONLY = ON); the ledger view's last four columns carry the custom column names
    $tok['LEDGER_JOIN']  = Gate ($v -ge 16) "LEFT JOIN sys.views lvw ON lvw.object_id=t.ledger_view_id
OUTER APPLY (SELECT MAX(CASE q.rn WHEN 4 THEN q.cname END) AS tx, MAX(CASE q.rn WHEN 3 THEN q.cname END) AS sq, MAX(CASE q.rn WHEN 2 THEN q.cname END) AS op, MAX(CASE q.rn WHEN 1 THEN q.cname END) AS opd
             FROM (SELECT lc.name AS cname, ROW_NUMBER() OVER (ORDER BY lc.column_id DESC) AS rn FROM sys.columns lc WHERE lc.object_id=t.ledger_view_id) q) lvc"
    $tok['OPT_LEDGER']   = Gate ($v -ge 16) "+ CASE WHEN t.ledger_type IN (2,3) THEN CASE WHEN t.ledger_type=2 THEN ', SYSTEM_VERSIONING = ON (HISTORY_TABLE = '+QUOTENAME(SCHEMA_NAME(ht.schema_id))+'.'+QUOTENAME(ht.name)+')' ELSE '' END + ', LEDGER = ON ('+CASE WHEN t.ledger_type=3 THEN 'APPEND_ONLY = ON, ' ELSE '' END+'LEDGER_VIEW = '+QUOTENAME(SCHEMA_NAME(lvw.schema_id))+'.'+QUOTENAME(lvw.name)+' (TRANSACTION_ID_COLUMN_NAME = '+QUOTENAME(lvc.tx)+', SEQUENCE_NUMBER_COLUMN_NAME = '+QUOTENAME(lvc.sq)+', OPERATION_TYPE_COLUMN_NAME = '+QUOTENAME(lvc.op)+', OPERATION_TYPE_DESC_COLUMN_NAME = '+QUOTENAME(lvc.opd)+'))' ELSE '' END"
    $tok['HISTIDX_PH']   = Gate ($v -ge 13) "WHEN EXISTS (SELECT 1 FROM sys.tables hx WHERE hx.object_id=i.object_id AND hx.temporal_type=1) THEN 20"
    $tok['HISTIDX_ORD']  = Gate ($v -ge 13) "WHEN EXISTS (SELECT 1 FROM sys.tables hx WHERE hx.object_id=i.object_id AND hx.temporal_type=1) THEN 1"
    $tok['GRAPHIDX']     = Gate ($v -ge 14) "AND NOT EXISTS (SELECT 1 FROM sys.index_columns gx JOIN sys.columns gc ON gc.object_id=gx.object_id AND gc.column_id=gx.column_id WHERE gx.object_id=i.object_id AND gx.index_id=i.index_id AND gc.graph_type IS NOT NULL)"
    $tok['CS_ORDER']     = Gate ($v -ge 16) "+ CASE WHEN i.type=5 THEN ISNULL(' ORDER ('+STUFF((SELECT ', '+QUOTENAME(COL_NAME(ixc.object_id,ixc.column_id)) FROM sys.index_columns ixc WHERE ixc.object_id=i.object_id AND ixc.index_id=i.index_id AND ixc.column_store_order_ordinal>0 ORDER BY ixc.column_store_order_ordinal FOR XML PATH(''),TYPE).value('.','nvarchar(max)'),1,2,'')+')','') ELSE '' END"
    $tok['USER_TYPES']   = Gate ($v -ge 16) "'S','U','G','E','X'" "'S','U','G'"
    $tok['USER_ISCONTAINED_SQL'] = Gate ($v -ge 11) 'dp.authentication_type=2' '1=0'
    $tok['USER_NOTCONTAINED_SQL'] = Gate ($v -ge 11) 'AND dp.authentication_type<>2' ''
    $tok['USER_CLAUSE'] = if ($v -ge 16) {
        "CASE dp.authentication_type WHEN 0 THEN ' WITHOUT LOGIN' WHEN 1 THEN CASE WHEN sl.name IS NOT NULL THEN ' FOR LOGIN '+QUOTENAME(sl.name) ELSE ' WITHOUT LOGIN /* !! login sid '+CONVERT(varchar(100),dp.sid,1)+' not on this instance */' END WHEN 3 THEN '' WHEN 4 THEN ' FROM EXTERNAL PROVIDER' ELSE ' WITHOUT LOGIN' END"
    } elseif ($v -ge 11) {
        "CASE dp.authentication_type WHEN 0 THEN ' WITHOUT LOGIN' WHEN 1 THEN CASE WHEN sl.name IS NOT NULL THEN ' FOR LOGIN '+QUOTENAME(sl.name) ELSE ' WITHOUT LOGIN /* !! login sid '+CONVERT(varchar(100),dp.sid,1)+' not on this instance */' END WHEN 3 THEN '' ELSE ' WITHOUT LOGIN' END"
    } else {
        "CASE WHEN sl.name IS NOT NULL THEN ' FOR LOGIN '+QUOTENAME(sl.name) WHEN CONVERT(varchar(100),dp.sid,1) LIKE '0x0105000000000009030000%' THEN ' WITHOUT LOGIN' ELSE ' WITHOUT LOGIN /* !! login sid '+CONVERT(varchar(100),dp.sid,1)+' not on this instance */' END"
    }
    $tok['CERT_USER'] = if ($v -ge 11) {
        "'CREATE USER '+QUOTENAME(dp.name)+' FOR CERTIFICATE '+QUOTENAME(c.name)+ISNULL(' WITH DEFAULT_SCHEMA = '+QUOTENAME(dp.default_schema_name),'')+';'"
    } else {
        "'-- !! MANUAL: user '+QUOTENAME(dp.name)+' is mapped to a certificate that cannot be scripted on this version; recreate it by hand: CREATE USER '+QUOTENAME(dp.name)+' FOR CERTIFICATE <cert>;'"
    }
    $tok['ROLE_ADD'] = if ($v -ge 11) {
        "'ALTER ROLE '+QUOTENAME(r.name)+' ADD MEMBER '+QUOTENAME(m.name)+';'"
    } else {
        "'EXEC sys.sp_addrolemember N'''+REPLACE(r.name,'''','''''')+''', N'''+REPLACE(m.name,'''','''''')+''';'"
    }

    # =================================================================== SECTIONS
    # ---- helper-table population (prelude creates the empty tables) ----------------
    Add-Section 'prep: column types' (Fill @'
INSERT #coltype(object_id, column_id, typestr)
SELECT c.object_id, c.column_id, {{TYPE_C}}
FROM sys.columns c
JOIN sys.types t ON t.user_type_id = c.user_type_id
LEFT JOIN sys.xml_schema_collections xsc ON xsc.xml_collection_id = c.xml_collection_id;
'@)

    Add-Section 'prep: parameter types' (Fill @'
INSERT #partype(object_id, parameter_id, typestr)
SELECT p.object_id, p.parameter_id, {{TYPE_P}}
FROM sys.parameters p
JOIN sys.types t ON t.user_type_id = p.user_type_id
LEFT JOIN sys.xml_schema_collections xsc ON xsc.xml_collection_id = p.xml_collection_id
WHERE p.object_id > 0;
'@)

    Add-Section 'prep: table kinds' (Fill @'
INSERT #tbl(object_id, kind)
SELECT t.object_id,
  CASE WHEN {{K_LEDGERHIST}} THEN 'skip'
       WHEN {{K_EXTERNAL}}   THEN 'unsupported:external'
       WHEN {{K_FILETABLE}}  THEN 'unsupported:filetable'
       WHEN {{K_MEMOPT}}     THEN 'memopt'
       WHEN {{K_NODE}}       THEN 'node'
       WHEN {{K_EDGE}}       THEN 'edge'
       ELSE 'normal' END
FROM sys.tables t WHERE t.is_ms_shipped = 0;

INSERT #ddl(phase, ord, txt)
SELECT 1, 5, '-- !! NOT SCRIPTED ('+REPLACE(k.kind,'unsupported:','')+' table): '+{{QN_T}}
FROM #tbl k JOIN sys.tables t ON t.object_id = k.object_id WHERE k.kind LIKE 'unsupported:%';
'@)

    Add-Section 'prep: compression clauses' @'
SELECT p.object_id, p.index_id, p.data_compression_desc AS d,
       STUFF((SELECT ','+CAST(p2.partition_number AS varchar(10)) FROM sys.partitions p2
              WHERE p2.object_id=p.object_id AND p2.index_id=p.index_id AND p2.data_compression_desc=p.data_compression_desc
              ORDER BY p2.partition_number FOR XML PATH('')),1,1,'') AS parts
INTO #cmpd FROM sys.partitions p
JOIN sys.objects o ON o.object_id = p.object_id AND o.is_ms_shipped = 0
GROUP BY p.object_id, p.index_id, p.data_compression_desc;

INSERT #cmp(object_id, index_id, clause)
SELECT c.object_id, c.index_id,
  CASE WHEN c.nd = 1
       THEN CASE WHEN c.d IN ('NONE','COLUMNSTORE') THEN NULL ELSE 'DATA_COMPRESSION = '+c.d END
       ELSE STUFF((SELECT ', DATA_COMPRESSION = '+x.d+' ON PARTITIONS ('+x.parts+')' FROM #cmpd x
                   WHERE x.object_id=c.object_id AND x.index_id=c.index_id ORDER BY x.d FOR XML PATH('')),1,2,'') END
FROM (SELECT object_id, index_id, MIN(d) AS d, COUNT(*) AS nd FROM #cmpd GROUP BY object_id, index_id) c;
DROP TABLE #cmpd;
'@

    Add-Section 'prep: module dependency order' @'
INSERT #mod(object_id, type, rnk, early)
SELECT o.object_id, o.type, 1, 0
FROM sys.objects o WHERE o.is_ms_shipped = 0 AND o.type IN ('V','FN','IF','TF','FS','FT','AF');

INSERT #edge(src_id, dst_id)
SELECT DISTINCT d.referencing_id, d.referenced_id
FROM sys.sql_expression_dependencies d
JOIN #mod a ON a.object_id = d.referencing_id
JOIN #mod b ON b.object_id = d.referenced_id
WHERE d.referenced_id IS NOT NULL AND d.referencing_id <> d.referenced_id;

/* functions that a table needs at CREATE TABLE time (computed column / CHECK / DEFAULT) must exist before tables */
UPDATE m SET early = 1
FROM #mod m
WHERE m.type IN ('FN','IF','TF','FS','FT','AF')
  AND EXISTS (SELECT 1 FROM sys.sql_expression_dependencies d
              JOIN sys.objects ro ON ro.object_id = d.referencing_id
              WHERE d.referenced_id = m.object_id
                AND (ro.type = 'U' OR (ro.type IN ('C','D') AND ro.parent_object_id IN (SELECT object_id FROM sys.tables))));

DECLARE @n int = 1, @i int = 0;
WHILE @n > 0 AND @i < 100
BEGIN   /* closure: whatever an early function calls must be early too */
  UPDATE m SET early = 1 FROM #mod m
  WHERE m.early = 0 AND m.type IN ('FN','IF','TF','FS','FT','AF')
    AND EXISTS (SELECT 1 FROM #edge e JOIN #mod a ON a.object_id = e.src_id WHERE e.dst_id = m.object_id AND a.early = 1);
  SET @n = @@ROWCOUNT; SET @i = @i + 1;
END

SET @n = 1; SET @i = 0;
WHILE @n > 0 AND @i < 100
BEGIN   /* longest-path rank: a module is emitted after everything it depends on */
  UPDATE m SET rnk = x.r
  FROM #mod m
  CROSS APPLY (SELECT MAX(m2.rnk) + 1 AS r FROM #edge e JOIN #mod m2 ON m2.object_id = e.dst_id WHERE e.src_id = m.object_id) x
  WHERE x.r > m.rnk;
  SET @n = @@ROWCOUNT; SET @i = @i + 1;
END

INSERT #ddl(phase, ord, txt)
SELECT 1, 6, '-- !! ORDER CONFLICT: function '+QUOTENAME(SCHEMA_NAME(o.schema_id))+'.'+QUOTENAME(o.name)+' is needed by a table but is schema-bound to a table/view; replay will fail unless you reorder by hand'
FROM #mod m JOIN sys.objects o ON o.object_id = m.object_id
WHERE m.early = 1 AND EXISTS (SELECT 1 FROM sys.sql_expression_dependencies d JOIN sys.objects ro ON ro.object_id = d.referenced_id
                              WHERE d.referencing_id = m.object_id AND d.is_schema_bound_reference = 1 AND ro.type IN ('U','V'));
'@

    # ---- header -------------------------------------------------------------------
    Add-Section 'header' (Fill @'
INSERT #ddl(phase, ord, txt)
SELECT 1, 0, '-- Generated by mssql-schema-extract (script variant for SQL Server major version {{V}})'+@nl
  +'-- Source database: '+QUOTENAME(DB_NAME())+'   server '+CONVERT(nvarchar(128),SERVERPROPERTY('ProductVersion'))+' '+ISNULL(CONVERT(nvarchar(128),SERVERPROPERTY('Edition')),N'')+@nl
  +'-- Compatibility level '+CAST(d.compatibility_level AS varchar(5))+'   collation '+ISNULL(d.collation_name,N'?')+@nl
  +'-- Lines starting with  -- !!  are notes: not scripted (by design or unsupported), a manual step, or a section that failed. The last line counts the ones that need attention.'
FROM sys.databases d WHERE d.database_id = DB_ID();
'@)

    # ---- 3-6: principals needed before schemas -------------------------------------
    Add-Section 'users' (Fill @'
INSERT #ddl(phase, ord, txt)
SELECT 3, 0,
  CASE WHEN {{USER_ISCONTAINED_SQL}}
       THEN '-- !! MANUAL: contained database user '+QUOTENAME(dp.name)+' has a password that cannot be read. Create it by hand: CREATE USER '+QUOTENAME(dp.name)+' WITH PASSWORD = N''<set>'''+ISNULL(', DEFAULT_SCHEMA = '+QUOTENAME(dp.default_schema_name),'')+';'
       ELSE 'CREATE USER '+QUOTENAME(dp.name)+{{USER_CLAUSE}}+ISNULL(' WITH DEFAULT_SCHEMA = '+QUOTENAME(dp.default_schema_name),'')+';' END
FROM sys.database_principals dp
LEFT JOIN sys.server_principals sl ON sl.sid = dp.sid
WHERE dp.type IN ({{USER_TYPES}}) AND dp.principal_id > 4 AND dp.sid IS NOT NULL;

INSERT #ddl(phase, ord, txt)
SELECT 9, 1, CASE dp.type
    WHEN 'C' THEN {{CERT_USER}}
    ELSE '-- !! MANUAL: user '+QUOTENAME(dp.name)+' is mapped to an asymmetric key; recreate the key and then: CREATE USER '+QUOTENAME(dp.name)+' FOR ASYMMETRIC KEY <key>;' END
FROM sys.database_principals dp
LEFT JOIN sys.certificates c ON c.sid = dp.sid AND dp.type = 'C'
WHERE dp.type IN ('C','K') AND dp.principal_id > 4;
'@)

    Add-Section 'roles' @'
INSERT #ddl(phase, ord, txt)
SELECT CASE WHEN ow.type = 'R' THEN 5 ELSE 4 END, dp.principal_id,
       'CREATE ROLE '+QUOTENAME(dp.name)+' AUTHORIZATION '+QUOTENAME(ow.name)+';'
FROM sys.database_principals dp
JOIN sys.database_principals ow ON ow.principal_id = dp.owning_principal_id
WHERE dp.type = 'R' AND dp.is_fixed_role = 0 AND dp.name <> 'public';

INSERT #ddl(phase, ord, txt)
SELECT 6, 0, '-- !! MANUAL: application role '+QUOTENAME(dp.name)+' - its password cannot be read. Create it by hand: CREATE APPLICATION ROLE '+QUOTENAME(dp.name)+' WITH PASSWORD = N''<set>'''+ISNULL(', DEFAULT_SCHEMA = '+QUOTENAME(dp.default_schema_name),'')+';'
FROM sys.database_principals dp WHERE dp.type = 'A';
'@

    # ---- 7-8: database master key / certificates / keys (nothing secret is ever emitted) ----
    Add-Section 'keys and certificates' @'
INSERT #ddl(phase, ord, txt)
SELECT 7, 0, '-- !! MANUAL: the source database has a database master key; create it by hand: CREATE MASTER KEY ENCRYPTION BY PASSWORD = N''<set>'';'
FROM sys.symmetric_keys WHERE name = '##MS_DatabaseMasterKey##';

INSERT #ddl(phase, ord, txt)
SELECT 8, 1, '-- !! MANUAL: asymmetric key '+QUOTENAME(k.name)+' ('+k.algorithm_desc+') - key material is not scriptable; recreate or restore it by hand.'
FROM sys.asymmetric_keys k WHERE k.name NOT LIKE '##%';

INSERT #ddl(phase, ord, txt)
SELECT 8, 2, '-- !! MANUAL: symmetric key '+QUOTENAME(k.name)+' ('+k.algorithm_desc+') - key material is not scriptable; recreate or restore it by hand.'
FROM sys.symmetric_keys k WHERE k.name NOT LIKE '##%';
'@


    if ($v -ge 11) {
        Add-Section 'certificates' @'
INSERT #ddl(phase, ord, txt)
SELECT 8, 0, 'CREATE CERTIFICATE '+QUOTENAME(c.name)+' AUTHORIZATION '+QUOTENAME(USER_NAME(c.principal_id))
   +' FROM BINARY = '+CONVERT(varchar(max), CERTENCODED(c.certificate_id), 1)
   +CASE WHEN c.pvt_key_encryption_type <> 'NA' THEN ' /* !! public part only: the private key is not exportable here and must be restored by hand */' ELSE '' END
   +' ACTIVE FOR BEGIN_DIALOG = '+CASE WHEN c.is_active_for_begin_dialog = 1 THEN 'ON' ELSE 'OFF' END+';'
FROM sys.certificates c WHERE c.name NOT LIKE '##%';
'@
    } else {
        Add-Section 'certificates (not exportable on this version)' @'
INSERT #ddl(phase, ord, txt)
SELECT 8, 0, '-- !! MANUAL: certificate '+QUOTENAME(c.name)+' - CERTENCODED is not available on this SQL Server version, so the certificate cannot be scripted; restore it by hand.'
FROM sys.certificates c WHERE c.name NOT LIKE '##%';
'@
    }

    # ---- 8: change tracking (database level) ---------------------------------------
    Add-Section 'change tracking (database)' @'
INSERT #ddl(phase, ord, txt)
SELECT 8, 3, 'ALTER DATABASE CURRENT SET CHANGE_TRACKING = ON (CHANGE_RETENTION = '+CAST(ct.retention_period AS varchar(10))+' '+ct.retention_period_units_desc
   +', AUTO_CLEANUP = '+CASE WHEN ct.is_auto_cleanup_on = 1 THEN 'ON' ELSE 'OFF' END+');'
FROM sys.change_tracking_databases ct WHERE ct.database_id = DB_ID();
'@

    # ---- 9: Always Encrypted key metadata (must precede tables that use the keys) ---
    if ($v -ge 13) {
        $enclave = Gate ($v -ge 15) "+CASE WHEN cmk.allow_enclave_computations=1 THEN ', ENCLAVE_COMPUTATIONS (SIGNATURE = '+CONVERT(varchar(max),cmk.signature,1)+')' ELSE '' END"
        Add-Section 'column master / encryption keys' (@"
INSERT #ddl(phase, ord, txt)
SELECT 9, 0, 'CREATE COLUMN MASTER KEY '+QUOTENAME(cmk.name)+' WITH (KEY_STORE_PROVIDER_NAME = N'''+cmk.key_store_provider_name+''', KEY_PATH = N'''+cmk.key_path+''''$enclave+');'
FROM sys.column_master_keys cmk;

INSERT #ddl(phase, ord, txt)
SELECT 9, 2,
  CASE WHEN v.rn = 1 THEN 'CREATE COLUMN ENCRYPTION KEY '+QUOTENAME(cek.name)+' WITH VALUES ('
       ELSE 'ALTER COLUMN ENCRYPTION KEY '+QUOTENAME(cek.name)+' ADD VALUE (' END
  +'COLUMN_MASTER_KEY = '+QUOTENAME(cmk.name)+', ALGORITHM = '''+v.encryption_algorithm_name+''', ENCRYPTED_VALUE = '+CONVERT(varchar(max), v.encrypted_value, 1)+');'
FROM (SELECT cv.*, ROW_NUMBER() OVER (PARTITION BY cv.column_encryption_key_id ORDER BY cv.column_master_key_id) AS rn
      FROM sys.column_encryption_key_values cv) v
JOIN sys.column_encryption_keys cek ON cek.column_encryption_key_id = v.column_encryption_key_id
JOIN sys.column_master_keys cmk ON cmk.column_master_key_id = v.column_master_key_id;
"@)
    }

    # ---- 10: schemas ---------------------------------------------------------------
    Add-Section 'schemas' @'
INSERT #ddl(phase, ord, txt)
SELECT 10, 0, 'CREATE SCHEMA '+QUOTENAME(s.name)+' AUTHORIZATION '+QUOTENAME(dp.name)+';'
FROM sys.schemas s JOIN sys.database_principals dp ON dp.principal_id = s.principal_id
WHERE s.schema_id BETWEEN 5 AND 16383 AND s.name NOT IN ('dbo','guest','sys','INFORMATION_SCHEMA');
'@

    # ---- 11: filegroups + files (incl. memory-optimized and FILESTREAM) ------------
    Add-Section 'filegroups and files' @'
INSERT #ddl(phase, ord, txt)
SELECT 11, 0, 'ALTER DATABASE CURRENT ADD FILEGROUP '+QUOTENAME(fg.name)
   +CASE fg.type WHEN 'FX' THEN ' CONTAINS MEMORY_OPTIMIZED_DATA' WHEN 'FD' THEN ' CONTAINS FILESTREAM' ELSE '' END+';'
FROM sys.filegroups fg WHERE fg.name <> 'PRIMARY';

INSERT #ddl(phase, ord, txt)
SELECT 11, 1, 'ALTER DATABASE CURRENT ADD FILE (NAME='''+REPLACE(df.name,'''','''''')+''', FILENAME='''+REPLACE(df.physical_name,'''','''''')+''''
   +CASE WHEN fg.type IN ('FX','FD') THEN '' ELSE ', SIZE='+CAST(df.size*8 AS varchar(20))+'KB' END
   +') TO FILEGROUP '+QUOTENAME(fg.name)+';'
FROM sys.database_files df JOIN sys.filegroups fg ON fg.data_space_id = df.data_space_id
WHERE fg.name <> 'PRIMARY';

INSERT #ddl(phase, ord, txt)
SELECT 11, 2, 'ALTER DATABASE CURRENT MODIFY FILEGROUP '+QUOTENAME(fg.name)+' DEFAULT;'
FROM sys.filegroups fg WHERE fg.is_default = 1 AND fg.name <> 'PRIMARY' AND fg.type = 'FG';
'@

    # ---- 12-16: types, xml schema collections, sequences, partitioning --------------
    Add-Section 'alias types' @'
INSERT #ddl(phase, ord, txt)
SELECT 12, 0, 'CREATE TYPE '+QUOTENAME(SCHEMA_NAME(t.schema_id))+'.'+QUOTENAME(t.name)+' FROM '
          + QUOTENAME(bt.name) + CASE
              WHEN bt.name IN ('varchar','char','varbinary','binary') THEN '('+CASE WHEN t.max_length=-1 THEN 'max' ELSE CAST(t.max_length AS varchar(10)) END+')'
              WHEN bt.name IN ('nvarchar','nchar') THEN '('+CASE WHEN t.max_length=-1 THEN 'max' ELSE CAST(t.max_length/2 AS varchar(10)) END+')'
              WHEN bt.name IN ('decimal','numeric') THEN '('+CAST(t.precision AS varchar(10))+','+CAST(t.scale AS varchar(10))+')'
              WHEN bt.name IN ('datetime2','time','datetimeoffset') THEN '('+CAST(t.scale AS varchar(10))+')'
              ELSE '' END
          + CASE WHEN t.is_nullable=1 THEN ' NULL' ELSE ' NOT NULL' END + ';'
FROM sys.types t JOIN sys.types bt ON bt.user_type_id = t.system_type_id
WHERE t.is_user_defined = 1 AND t.is_table_type = 0 AND t.is_assembly_type = 0;
'@

    Add-Section 'xml schema collections' @'
INSERT #ddl(phase, ord, txt)
SELECT 13, 0, 'CREATE XML SCHEMA COLLECTION '+QUOTENAME(SCHEMA_NAME(xsc.schema_id))+'.'+QUOTENAME(xsc.name)+' AS N'''
          + REPLACE(CAST(XML_SCHEMA_NAMESPACE(SCHEMA_NAME(xsc.schema_id), xsc.name) AS nvarchar(max)),'''','''''')+''';'
FROM sys.xml_schema_collections xsc WHERE xsc.schema_id <> SCHEMA_ID('sys');
'@

    if ($v -ge 11) {
        Add-Section 'sequences' @'
INSERT #ddl(phase, ord, txt)
SELECT 14, 0, 'CREATE SEQUENCE '+QUOTENAME(SCHEMA_NAME(sq.schema_id))+'.'+QUOTENAME(sq.name)
          +' AS '+QUOTENAME(TYPE_NAME(sq.user_type_id))
          +' START WITH '+CONVERT(varchar(40), sq.start_value)
          +' INCREMENT BY '+CONVERT(varchar(40), sq.increment)
          +' MINVALUE '+CONVERT(varchar(40), sq.minimum_value)
          +' MAXVALUE '+CONVERT(varchar(40), sq.maximum_value)
          +CASE WHEN sq.is_cycling=1 THEN ' CYCLE' ELSE ' NO CYCLE' END
          +CASE WHEN sq.is_cached=1 THEN ' CACHE '+ISNULL(CAST(sq.cache_size AS varchar(20)),'') ELSE ' NO CACHE' END+';'
FROM sys.sequences sq WHERE sq.is_ms_shipped = 0;
'@
    }

    Add-Section 'partition functions and schemes' (Fill @'
INSERT #ddl(phase, ord, txt)
SELECT 15, 0, 'CREATE PARTITION FUNCTION '+QUOTENAME(pf.name)+' ('+pt.typestr+') AS RANGE '
          + CASE WHEN pf.boundary_value_on_right=1 THEN 'RIGHT' ELSE 'LEFT' END + ' FOR VALUES ('
          + ISNULL(STUFF((SELECT ', '''+CONVERT(nvarchar(100), rv.value, 126)+''''
                   FROM sys.partition_range_values rv
                   WHERE rv.function_id = pf.function_id ORDER BY rv.boundary_id
                   FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),1,2,''),'') + ');'
FROM sys.partition_functions pf
CROSS APPLY (SELECT TOP 1 {{TYPE_PP}} AS typestr
             FROM sys.partition_parameters pp JOIN sys.types t ON t.user_type_id = pp.user_type_id
             WHERE pp.function_id = pf.function_id) pt;

INSERT #ddl(phase, ord, txt)
SELECT 15, 1, 'CREATE PARTITION SCHEME '+QUOTENAME(ps.name)+' AS PARTITION '+QUOTENAME(pf.name)+' TO ('
          + STUFF((SELECT ', '+QUOTENAME(fg.name)
                   FROM sys.destination_data_spaces dds
                   JOIN sys.filegroups fg ON fg.data_space_id = dds.data_space_id
                   WHERE dds.partition_scheme_id = ps.data_space_id ORDER BY dds.destination_id
                   FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),1,2,'') + ');'
FROM sys.partition_schemes ps JOIN sys.partition_functions pf ON pf.function_id = ps.function_id;
'@)

    $ttMemopt = Gate ($v -ge 12) "+ CASE WHEN tt.is_memory_optimized=1 THEN ' WITH (MEMORY_OPTIMIZED = ON)' ELSE '' END"
    $ttHash   = Gate ($v -ge 12) 'LEFT JOIN sys.hash_indexes hi ON hi.object_id=i.object_id AND hi.index_id=i.index_id'
    $ttBucket = Gate ($v -ge 12) "ISNULL(CAST(hi.bucket_count AS varchar(20)),'0')" "'0'"
    Add-Section 'table types' (Fill (@"
INSERT #ddl(phase, ord, txt)
SELECT 16, 0, 'CREATE TYPE '+QUOTENAME(SCHEMA_NAME(tt.schema_id))+'.'+QUOTENAME(tt.name)+' AS TABLE ('+@nl
   + STUFF((SELECT ','+@nl+'    '+QUOTENAME(c.name)+' '
                   + CASE WHEN cc.definition IS NOT NULL
                          THEN 'AS '+cc.definition+CASE WHEN cc.is_persisted=1 THEN ' PERSISTED' ELSE '' END
                          ELSE ct.typestr
                   + CASE WHEN c.is_identity=1 THEN ' IDENTITY('+CAST(ic.seed_value AS varchar(20))+','+CAST(ic.increment_value AS varchar(20))+')' ELSE '' END
                   + CASE WHEN c.collation_name IS NOT NULL AND c.collation_name <> @dbcoll THEN ' COLLATE '+c.collation_name ELSE '' END
                   + CASE WHEN c.is_nullable=1 THEN ' NULL' ELSE ' NOT NULL' END
                   + ISNULL(' DEFAULT '+dc.definition,'') END
            FROM sys.columns c
            JOIN #coltype ct ON ct.object_id=c.object_id AND ct.column_id=c.column_id
            LEFT JOIN sys.computed_columns cc ON cc.object_id=c.object_id AND cc.column_id=c.column_id
            LEFT JOIN sys.identity_columns ic ON ic.object_id=c.object_id AND ic.column_id=c.column_id
            LEFT JOIN sys.default_constraints dc ON dc.parent_object_id=c.object_id AND dc.parent_column_id=c.column_id
            WHERE c.object_id = tt.type_table_object_id ORDER BY c.column_id
            FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),1,1,'')
   + ISNULL((SELECT ','+@nl+'    '+CASE WHEN i.is_primary_key=1 THEN 'PRIMARY KEY' WHEN i.is_unique_constraint=1 THEN 'UNIQUE' ELSE 'INDEX '+QUOTENAME(i.name) END
                   + CASE i.type WHEN 1 THEN ' CLUSTERED' WHEN 7 THEN ' NONCLUSTERED HASH' ELSE ' NONCLUSTERED' END
                   + ' ('+CASE WHEN i.type=7 THEN {{KEYCOLS_I_NS}} ELSE {{KEYCOLS_I}} END+')'
                   + CASE WHEN i.type=7 THEN ' WITH (BUCKET_COUNT = '+$ttBucket+')' ELSE '' END
             FROM sys.indexes i $ttHash
             WHERE i.object_id = tt.type_table_object_id AND i.index_id>0
             ORDER BY i.index_id FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),'')
   + ISNULL((SELECT ','+@nl+'    CHECK '+ck.definition FROM sys.check_constraints ck
             WHERE ck.parent_object_id = tt.type_table_object_id ORDER BY ck.object_id FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),'')
   + @nl+')' $ttMemopt + ';'
FROM sys.table_types tt WHERE tt.is_user_defined=1;
"@))

    # ---- 17-19: CLR assemblies, types, functions/aggregates --------------------------
    Add-Section 'CLR assemblies and types' @'
INSERT #ddl(phase, ord, txt)
SELECT 17, 0, 'CREATE ASSEMBLY '+QUOTENAME(a.name)+' AUTHORIZATION '+QUOTENAME(USER_NAME(a.principal_id))
  +' FROM '+CONVERT(varchar(max), af.content, 1)
  +' WITH PERMISSION_SET = '+CASE a.permission_set_desc WHEN 'SAFE_ACCESS' THEN 'SAFE' WHEN 'UNSAFE_ACCESS' THEN 'UNSAFE' ELSE 'EXTERNAL_ACCESS' END+';'
FROM sys.assemblies a
JOIN sys.assembly_files af ON af.assembly_id = a.assembly_id AND af.file_id = 1
WHERE a.is_user_defined = 1;

INSERT #ddl(phase, ord, txt)
SELECT 18, 0, 'CREATE TYPE '+QUOTENAME(SCHEMA_NAME(t.schema_id))+'.'+QUOTENAME(t.name)
  +' EXTERNAL NAME '+QUOTENAME(a.name)+'.'+QUOTENAME(at.assembly_class)+';'
FROM sys.assembly_types at
JOIN sys.types t ON t.user_type_id = at.user_type_id
JOIN sys.assemblies a ON a.assembly_id = at.assembly_id
WHERE a.is_user_defined = 1 AND t.is_user_defined = 1;
'@

    $clrWith = "CASE WHEN am.execute_as_principal_id IS NULL AND ISNULL(am.null_on_null_input,0)=0 THEN '' ELSE ' WITH '+STUFF(CASE WHEN am.execute_as_principal_id IS NULL THEN '' WHEN am.execute_as_principal_id=-2 THEN ', EXECUTE AS OWNER' ELSE ', EXECUTE AS '+QUOTENAME(USER_NAME(am.execute_as_principal_id)) END + CASE WHEN o.type IN ('FS','FT') AND am.null_on_null_input=1 THEN ', RETURNS NULL ON NULL INPUT' ELSE '' END,1,2,'') END"
    $clrParams = "ISNULL(STUFF((SELECT ', '+p.name+' '+pt.typestr+CASE WHEN p.is_output=1 THEN ' OUTPUT' ELSE '' END FROM sys.parameters p JOIN #partype pt ON pt.object_id=p.object_id AND pt.parameter_id=p.parameter_id WHERE p.object_id=o.object_id AND p.parameter_id>0 ORDER BY p.parameter_id FOR XML PATH(''),TYPE).value('.','nvarchar(max)'),1,2,''),'')"
    Add-Section 'CLR routines' (@"
/* scalar functions */
INSERT #ddl(phase, ord, txt)
SELECT 19, m.rnk, 'CREATE FUNCTION '+$QN_O+' ('+$clrParams+') RETURNS '+rt.typestr+$clrWith+' AS EXTERNAL NAME '+QUOTENAME(a.name)+'.'+QUOTENAME(am.assembly_class)+'.'+QUOTENAME(am.assembly_method)+';'
FROM #mod m JOIN sys.objects o ON o.object_id=m.object_id
JOIN sys.assembly_modules am ON am.object_id=o.object_id JOIN sys.assemblies a ON a.assembly_id=am.assembly_id
JOIN #partype rt ON rt.object_id=o.object_id AND rt.parameter_id=0
WHERE o.type='FS' AND a.is_user_defined=1;

/* table-valued functions */
INSERT #ddl(phase, ord, txt)
SELECT 19, m.rnk, 'CREATE FUNCTION '+$QN_O+' ('+$clrParams+') RETURNS TABLE ('
   +STUFF((SELECT ', '+QUOTENAME(c.name)+' '+ct.typestr FROM sys.columns c JOIN #coltype ct ON ct.object_id=c.object_id AND ct.column_id=c.column_id WHERE c.object_id=o.object_id ORDER BY c.column_id FOR XML PATH(''),TYPE).value('.','nvarchar(max)'),1,2,'')
   +')'+$clrWith+' AS EXTERNAL NAME '+QUOTENAME(a.name)+'.'+QUOTENAME(am.assembly_class)+'.'+QUOTENAME(am.assembly_method)+';'
FROM #mod m JOIN sys.objects o ON o.object_id=m.object_id
JOIN sys.assembly_modules am ON am.object_id=o.object_id JOIN sys.assemblies a ON a.assembly_id=am.assembly_id
WHERE o.type='FT' AND a.is_user_defined=1;

/* aggregates */
INSERT #ddl(phase, ord, txt)
SELECT 19, m.rnk, 'CREATE AGGREGATE '+$QN_O+' ('+$clrParams+') RETURNS '+rt.typestr+' EXTERNAL NAME '+QUOTENAME(a.name)+'.'+QUOTENAME(am.assembly_class)+';'
FROM #mod m JOIN sys.objects o ON o.object_id=m.object_id
JOIN sys.assembly_modules am ON am.object_id=o.object_id JOIN sys.assemblies a ON a.assembly_id=am.assembly_id
JOIN #partype rt ON rt.object_id=o.object_id AND rt.parameter_id=0
WHERE o.type='AF' AND a.is_user_defined=1;

/* procedures */
INSERT #ddl(phase, ord, txt)
SELECT 52, 0, 'CREATE PROCEDURE '+$QN_O+' '+$clrParams+$clrWith+' AS EXTERNAL NAME '+QUOTENAME(a.name)+'.'+QUOTENAME(am.assembly_class)+'.'+QUOTENAME(am.assembly_method)+';'
FROM sys.objects o JOIN sys.assembly_modules am ON am.object_id=o.object_id JOIN sys.assemblies a ON a.assembly_id=am.assembly_id
WHERE o.type='PC' AND o.is_ms_shipped=0 AND a.is_user_defined=1;

INSERT #ddl(phase, ord, txt)
SELECT 1, 7, '-- !! NOT SCRIPTED (CLR trigger): '+QUOTENAME(OBJECT_SCHEMA_NAME(o.object_id))+'.'+QUOTENAME(o.name)
FROM sys.objects o WHERE o.type='TA' AND o.is_ms_shipped=0;
"@)

    # ---- 20: TABLES -----------------------------------------------------------------
    Add-Section 'tables' (Fill @'
INSERT #ddl(phase, ord, txt)
SELECT 20, {{TORD}},
    CASE WHEN t.uses_ansi_nulls=0 THEN 'SET ANSI_NULLS OFF;'+@nl+'GO'+@nl ELSE '' END
  + CASE WHEN pad.off_cnt>0 AND pad.on_cnt=0 THEN 'SET ANSI_PADDING OFF;'+@nl+'GO'+@nl ELSE '' END
  + 'CREATE TABLE '+{{QN_T}}+' ('+@nl
  + STUFF((
      SELECT ','+@nl+'    '+QUOTENAME(c.name)+' '
        + CASE WHEN cc.definition IS NOT NULL
               THEN 'AS '+cc.definition + CASE WHEN cc.is_persisted=1 THEN ' PERSISTED' ELSE '' END
                    + CASE WHEN cc.is_persisted=1 AND c.is_nullable=0 THEN ' NOT NULL' ELSE '' END
               ELSE ct.typestr
                    + CASE WHEN c.is_filestream=1 THEN ' FILESTREAM' ELSE '' END
                    + CASE WHEN c.collation_name IS NOT NULL AND c.collation_name <> @dbcoll THEN ' COLLATE '+c.collation_name ELSE '' END
                    {{COL_ENC}}
                    + CASE WHEN c.is_sparse=1 THEN ' SPARSE' ELSE '' END
                    + CASE WHEN c.is_column_set=1 THEN ' COLUMN_SET FOR ALL_SPARSE_COLUMNS' ELSE '' END
                    + CASE WHEN c.is_identity=1 THEN ' IDENTITY('+CAST(ic.seed_value AS varchar(20))+','+CAST(ic.increment_value AS varchar(20))+')'
                                                   + CASE WHEN ic.is_not_for_replication=1 THEN ' NOT FOR REPLICATION' ELSE '' END ELSE '' END
                    + CASE WHEN c.is_rowguidcol=1 THEN ' ROWGUIDCOL' ELSE '' END
                    {{COL_MASK}}
                    {{COL_GEN}}
                    + CASE WHEN c.is_nullable=1 THEN ' NULL' ELSE ' NOT NULL' END
                    + ISNULL(' CONSTRAINT '+QUOTENAME(dc.name)+' DEFAULT '+dc.definition,'')
          END
      FROM sys.columns c
      JOIN #coltype ct ON ct.object_id=c.object_id AND ct.column_id=c.column_id
      LEFT JOIN sys.computed_columns cc ON cc.object_id=c.object_id AND cc.column_id=c.column_id
      LEFT JOIN sys.identity_columns ic ON ic.object_id=c.object_id AND ic.column_id=c.column_id
      LEFT JOIN sys.default_constraints dc ON dc.parent_object_id=c.object_id AND dc.parent_column_id=c.column_id
      {{COL_JOINS}}
      WHERE c.object_id=t.object_id {{COL_FILTER}} ORDER BY c.column_id
      FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),1,1,'')
  {{PERIOD}}
  /* PK / UNIQUE constraints (hash for memory-optimized) */
  + ISNULL((SELECT ','+@nl+'    CONSTRAINT '+QUOTENAME(kc.name)+' '+CASE WHEN kc.type='PK' THEN 'PRIMARY KEY' ELSE 'UNIQUE' END
                 + CASE i.type WHEN 1 THEN ' CLUSTERED' WHEN 7 THEN ' NONCLUSTERED HASH' ELSE ' NONCLUSTERED' END
                 + ' ('+CASE WHEN i.type=7 THEN {{KEYCOLS_I_NS}} ELSE {{KEYCOLS_I}} END+')'
                 + CASE WHEN i.type=7 THEN ' WITH (BUCKET_COUNT = '+{{BUCKET}}+')'
                        WHEN k.kind='memopt' THEN ''
                        ELSE ' WITH (PAD_INDEX='+CASE WHEN i.is_padded=1 THEN 'ON' ELSE 'OFF' END
                           + ', IGNORE_DUP_KEY='+CASE WHEN i.ignore_dup_key=1 THEN 'ON' ELSE 'OFF' END
                           + ', ALLOW_ROW_LOCKS='+CASE WHEN i.allow_row_locks=1 THEN 'ON' ELSE 'OFF' END
                           + ', ALLOW_PAGE_LOCKS='+CASE WHEN i.allow_page_locks=1 THEN 'ON' ELSE 'OFF' END
                           + CASE WHEN i.fill_factor>0 THEN ', FILLFACTOR='+CAST(i.fill_factor AS varchar(5)) ELSE '' END
                           {{OSK_IDX}}
                           + ISNULL(', '+kcmp.clause,'') + ')' END
                 + CASE WHEN k.kind='memopt' THEN ''
                        ELSE ' ON '+ CASE WHEN ps.name IS NOT NULL
                                          THEN QUOTENAME(ps.name)+'('+QUOTENAME(COL_NAME(t.object_id, pic.column_id))+')'
                                          ELSE QUOTENAME(fgi.name) END END
            FROM sys.key_constraints kc
            JOIN sys.indexes i ON i.object_id=kc.parent_object_id AND i.index_id=kc.unique_index_id
            {{HASHJOIN}}
            LEFT JOIN #cmp kcmp ON kcmp.object_id=i.object_id AND kcmp.index_id=i.index_id
            LEFT JOIN sys.partition_schemes ps ON ps.data_space_id=i.data_space_id
            LEFT JOIN sys.filegroups fgi ON fgi.data_space_id=i.data_space_id
            LEFT JOIN sys.index_columns pic ON pic.object_id=i.object_id AND pic.index_id=i.index_id AND pic.partition_ordinal=1
            WHERE kc.parent_object_id=t.object_id
            ORDER BY kc.type DESC, kc.name FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),'')
  /* memory-optimized tables carry their other indexes inline */
  + CASE WHEN k.kind='memopt' THEN ISNULL((SELECT ','+@nl+'    INDEX '+QUOTENAME(i.name)
                 + CASE i.type WHEN 7 THEN ' NONCLUSTERED HASH' WHEN 5 THEN ' CLUSTERED COLUMNSTORE' WHEN 6 THEN ' NONCLUSTERED COLUMNSTORE' ELSE ' NONCLUSTERED' END
                 + CASE WHEN i.type=5 THEN '' WHEN i.type=6 THEN ' ('+{{ALL_I}}+')' WHEN i.type=7 THEN ' ('+{{KEYCOLS_I_NS}}+')' ELSE ' ('+{{KEYCOLS_I}}+')' END
                 + CASE WHEN i.type=7 THEN ' WITH (BUCKET_COUNT = '+{{BUCKET}}+')' ELSE '' END
            FROM sys.indexes i
            {{HASHJOIN}}
            WHERE i.object_id=t.object_id AND i.index_id>0 AND i.is_primary_key=0 AND i.is_unique_constraint=0 AND i.type IN (2,5,6,7)
            ORDER BY i.name FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),'') ELSE '' END
  + @nl+')'
  + CASE k.kind WHEN 'node' THEN ' AS NODE' WHEN 'edge' THEN ' AS EDGE' ELSE '' END
  + CASE WHEN k.kind='memopt' THEN ''
         ELSE ' ON '+ CASE WHEN tps.name IS NOT NULL THEN QUOTENAME(tps.name)+'('+QUOTENAME(COL_NAME(t.object_id, tpic.column_id))+')' ELSE QUOTENAME(tfg.name) END
              + ISNULL(' TEXTIMAGE_ON '+QUOTENAME(lfg.name),'') END
  + ISNULL(' FILESTREAM_ON '+QUOTENAME(fsd.name),'')
  + CASE WHEN LEN(o.s)>0 THEN ' WITH ('+STUFF(o.s,1,2,'')+')' ELSE '' END
  + ';'
  + CASE WHEN t.uses_ansi_nulls=0 OR (pad.off_cnt>0 AND pad.on_cnt=0) THEN @nl+'GO'+@nl+'SET ANSI_NULLS ON;'+@nl+'SET ANSI_PADDING ON;' ELSE '' END
FROM sys.tables t
JOIN #tbl k ON k.object_id=t.object_id AND k.kind IN ('normal','memopt','node','edge')
LEFT JOIN sys.indexes ti ON ti.object_id=t.object_id AND ti.index_id IN (0,1)
LEFT JOIN sys.partition_schemes tps ON tps.data_space_id=ti.data_space_id
LEFT JOIN sys.filegroups tfg ON tfg.data_space_id=ti.data_space_id
LEFT JOIN sys.index_columns tpic ON tpic.object_id=ti.object_id AND tpic.index_id=ti.index_id AND tpic.partition_ordinal=1
LEFT JOIN sys.filegroups lfg ON lfg.data_space_id=t.lob_data_space_id AND lfg.data_space_id<>ti.data_space_id
LEFT JOIN sys.data_spaces fsd ON fsd.data_space_id=t.filestream_data_space_id
LEFT JOIN #cmp hc ON hc.object_id=t.object_id AND hc.index_id=0
{{HISTJOIN}}
{{LEDGER_JOIN}}
OUTER APPLY (SELECT SUM(CASE WHEN pc.is_ansi_padded=0 THEN 1 ELSE 0 END) AS off_cnt, SUM(CASE WHEN pc.is_ansi_padded=1 THEN 1 ELSE 0 END) AS on_cnt
             FROM sys.columns pc JOIN sys.types pty ON pty.user_type_id=pc.user_type_id
             WHERE pc.object_id=t.object_id AND pty.name IN ('char','varchar','binary','varbinary') AND pc.max_length<>-1) pad
OUTER APPLY (SELECT ISNULL( {{OPT_MEMOPT}} CASE WHEN k.kind<>'memopt' AND ti.index_id=0 AND hc.clause IS NOT NULL THEN ', '+hc.clause ELSE '' END
                            {{OPT_TEMPORAL}} {{OPT_LEDGER}} ,'') AS s) o;
'@)

    Add-Section 'table stubs for unsupported kinds' @'
INSERT #ddl(phase, ord, txt)
SELECT 20, 9, '-- !! NOT SCRIPTED ('+REPLACE(k.kind,'unsupported:','')+' table) '+QUOTENAME(SCHEMA_NAME(t.schema_id))+'.'+QUOTENAME(t.name)+' - create it by hand'
FROM #tbl k JOIN sys.tables t ON t.object_id = k.object_id WHERE k.kind LIKE 'unsupported:%';
'@

    Add-Section 'table options' @'
INSERT #ddl(phase, ord, txt)
SELECT 22, 0, 'ALTER TABLE '+QUOTENAME(SCHEMA_NAME(t.schema_id))+'.'+QUOTENAME(t.name)+' SET (LOCK_ESCALATION = '+t.lock_escalation_desc+');'
FROM sys.tables t JOIN #tbl k ON k.object_id=t.object_id AND k.kind IN ('normal','node','edge')
WHERE t.lock_escalation_desc <> 'TABLE';

INSERT #ddl(phase, ord, txt)
SELECT 22, 1, 'EXEC sys.sp_tableoption N'''+REPLACE(QUOTENAME(SCHEMA_NAME(t.schema_id))+'.'+QUOTENAME(t.name),'''','''''')+''', ''large value types out of row'', 1;'
FROM sys.tables t JOIN #tbl k ON k.object_id=t.object_id AND k.kind IN ('normal','node','edge')
WHERE t.large_value_types_out_of_row = 1;
'@

    # ---- 30-33: indexes, xml/spatial, statistics -----------------------------------
    Add-Section 'indexes' (Fill @'
INSERT #ddl(phase, ord, txt)
SELECT CASE WHEN o.type='V' THEN 56 {{HISTIDX_PH}} WHEN i.type IN (5,6) THEN 54 ELSE 30 END,
       CASE WHEN o.type='V' THEN 0 {{HISTIDX_ORD}} ELSE 0 END,
  'CREATE '+CASE WHEN i.is_unique=1 THEN 'UNIQUE ' ELSE '' END
  + CASE i.type WHEN 1 THEN 'CLUSTERED ' WHEN 2 THEN 'NONCLUSTERED ' WHEN 5 THEN 'CLUSTERED COLUMNSTORE ' WHEN 6 THEN 'NONCLUSTERED COLUMNSTORE ' ELSE 'NONCLUSTERED ' END
  + 'INDEX '+QUOTENAME(i.name)+' ON '+{{QN_O}}
  + CASE WHEN i.type=5 THEN ''
         WHEN i.type=6 THEN ' ('+ISNULL({{ALL_I}},'')+')'
         ELSE ' ('+ISNULL({{KEYCOLS_I}},'')+')' END
  + CASE WHEN i.type IN (1,2) THEN ISNULL(' INCLUDE ('+{{INCL_I}}+')','') ELSE '' END
  {{CS_ORDER}}
  + ISNULL(' WHERE '+i.filter_definition,'')
  + CASE WHEN i.type IN (1,2) THEN ' WITH (PAD_INDEX='+CASE WHEN i.is_padded=1 THEN 'ON' ELSE 'OFF' END
      + ', STATISTICS_NORECOMPUTE='+CASE WHEN st.no_recompute=1 THEN 'ON' ELSE 'OFF' END
      + ', IGNORE_DUP_KEY='+CASE WHEN i.ignore_dup_key=1 THEN 'ON' ELSE 'OFF' END
      + ', ALLOW_ROW_LOCKS='+CASE WHEN i.allow_row_locks=1 THEN 'ON' ELSE 'OFF' END
      + ', ALLOW_PAGE_LOCKS='+CASE WHEN i.allow_page_locks=1 THEN 'ON' ELSE 'OFF' END
      + CASE WHEN i.fill_factor>0 THEN ', FILLFACTOR='+CAST(i.fill_factor AS varchar(5)) ELSE '' END
      {{OSK_IDX}}
      + ISNULL(', '+cmp.clause,'') + ')'
         WHEN cmp.clause IS NOT NULL THEN ' WITH ('+cmp.clause+')'
         ELSE '' END
  + ' ON '+CASE WHEN ps.name IS NOT NULL THEN QUOTENAME(ps.name)+'('+QUOTENAME(COL_NAME(i.object_id, pic.column_id))+')' ELSE QUOTENAME(fg.name) END + ';'
FROM sys.indexes i
JOIN sys.objects o ON o.object_id=i.object_id
LEFT JOIN sys.stats st ON st.object_id=i.object_id AND st.stats_id=i.index_id
LEFT JOIN #cmp cmp ON cmp.object_id=i.object_id AND cmp.index_id=i.index_id
LEFT JOIN sys.partition_schemes ps ON ps.data_space_id=i.data_space_id
LEFT JOIN sys.filegroups fg ON fg.data_space_id=i.data_space_id
LEFT JOIN sys.index_columns pic ON pic.object_id=i.object_id AND pic.index_id=i.index_id AND pic.partition_ordinal=1
WHERE o.is_ms_shipped=0 AND i.index_id>0 AND i.is_primary_key=0 AND i.is_unique_constraint=0 AND i.is_hypothetical=0
  AND i.type IN (1,2,5,6) AND o.type IN ('U','V')
  AND (o.type='V' OR EXISTS (SELECT 1 FROM #tbl k WHERE k.object_id=o.object_id AND k.kind IN ('normal','node','edge')))
  {{GRAPHIDX}};
'@)

    Add-Section 'xml and spatial indexes' @'
INSERT #ddl(phase, ord, txt)
SELECT 31, 0, CASE WHEN xi.using_xml_index_id IS NULL
   THEN 'CREATE PRIMARY XML INDEX '+QUOTENAME(xi.name)+' ON '+QUOTENAME(SCHEMA_NAME(o.schema_id))+'.'+QUOTENAME(o.name)
        +' ('+QUOTENAME(COL_NAME(xi.object_id,(SELECT TOP 1 column_id FROM sys.index_columns ic WHERE ic.object_id=xi.object_id AND ic.index_id=xi.index_id)))+');'
   ELSE 'CREATE XML INDEX '+QUOTENAME(xi.name)+' ON '+QUOTENAME(SCHEMA_NAME(o.schema_id))+'.'+QUOTENAME(o.name)
        +' ('+QUOTENAME(COL_NAME(xi.object_id,(SELECT TOP 1 column_id FROM sys.index_columns ic WHERE ic.object_id=xi.object_id AND ic.index_id=xi.index_id)))+')'
        +' USING XML INDEX '+QUOTENAME((SELECT name FROM sys.xml_indexes p WHERE p.object_id=xi.object_id AND p.index_id=xi.using_xml_index_id))
        +' FOR '+xi.secondary_type_desc+';' END
FROM sys.xml_indexes xi JOIN sys.objects o ON o.object_id=xi.object_id;

INSERT #ddl(phase, ord, txt)
SELECT 31, 1, 'CREATE SPATIAL INDEX '+QUOTENAME(si.name)+' ON '+QUOTENAME(SCHEMA_NAME(o.schema_id))+'.'+QUOTENAME(o.name)
  +' ('+QUOTENAME(COL_NAME(si.object_id,(SELECT TOP 1 column_id FROM sys.index_columns ic WHERE ic.object_id=si.object_id AND ic.index_id=si.index_id)))+')'
  +' USING '+si.tessellation_scheme
  +' WITH ('
  +CASE WHEN si.tessellation_scheme LIKE 'GEOMETRY%'
        THEN 'BOUNDING_BOX=('+LTRIM(STR(t1.xmin,38,10))+','+LTRIM(STR(t1.ymin,38,10))+','+LTRIM(STR(t1.xmax,38,10))+','+LTRIM(STR(t1.ymax,38,10))+'), ' ELSE '' END
  +'GRIDS=(LEVEL_1='+t1.level_1+',LEVEL_2='+t1.level_2+',LEVEL_3='+t1.level_3+',LEVEL_4='+t1.level_4+')'
  +', CELLS_PER_OBJECT='+CAST(t1.cells_per_object AS varchar(10))+');'
FROM sys.spatial_indexes si JOIN sys.objects o ON o.object_id=si.object_id
CROSS APPLY (SELECT MAX(t2.level_1_grid_desc) level_1, MAX(t2.level_2_grid_desc) level_2,
                    MAX(t2.level_3_grid_desc) level_3, MAX(t2.level_4_grid_desc) level_4,
                    MAX(t2.cells_per_object) cells_per_object,
                    MAX(t2.bounding_box_xmin) xmin, MAX(t2.bounding_box_ymin) ymin,
                    MAX(t2.bounding_box_xmax) xmax, MAX(t2.bounding_box_ymax) ymax
             FROM sys.spatial_index_tessellations t2
             WHERE t2.object_id=si.object_id AND t2.index_id=si.index_id) t1;
'@

    $statIncr = Gate ($v -ge 12) "+CASE WHEN s.is_incremental=1 THEN ', INCREMENTAL = ON' ELSE '' END"
    Add-Section 'statistics' (@"
INSERT #ddl(phase, ord, txt)
SELECT 33, 0, 'CREATE STATISTICS '+QUOTENAME(s.name)+' ON '+$QN_O+' ('
  +STUFF((SELECT ', '+QUOTENAME(COL_NAME(sc.object_id,sc.column_id)) FROM sys.stats_columns sc WHERE sc.object_id=s.object_id AND sc.stats_id=s.stats_id ORDER BY sc.stats_column_id FOR XML PATH(''),TYPE).value('.','nvarchar(max)'),1,2,'')+')'
  +ISNULL(' WHERE '+s.filter_definition,'')
  +CASE WHEN s.no_recompute=1 OR $(if ($v -ge 12) { 's.is_incremental=1' } else { '1=0' }) THEN ' WITH '+STUFF(CASE WHEN s.no_recompute=1 THEN ', NORECOMPUTE' ELSE '' END$statIncr,1,2,'') ELSE '' END+';'
FROM sys.stats s JOIN sys.objects o ON o.object_id=s.object_id
WHERE o.is_ms_shipped=0 AND o.type IN ('U','V') AND s.user_created=1 AND s.auto_created=0
  AND (o.type='V' OR EXISTS (SELECT 1 FROM #tbl k WHERE k.object_id=o.object_id AND k.kind IN ('normal','node','edge')));
"@)

    # ---- 40-44: constraints ----------------------------------------------------------
    Add-Section 'check constraints' (Fill @'
INSERT #ddl(phase, ord, txt)
SELECT 40, 0, 'ALTER TABLE '+{{QN_O}}
  + CASE WHEN cc.is_not_trusted=1 THEN ' WITH NOCHECK' ELSE ' WITH CHECK' END
  + ' ADD CONSTRAINT '+QUOTENAME(cc.name)+' CHECK '+CASE WHEN cc.is_not_for_replication=1 THEN 'NOT FOR REPLICATION ' ELSE '' END+cc.definition+';'
  + CASE WHEN cc.is_disabled=1 THEN @nl+'ALTER TABLE '+{{QN_O}}+' NOCHECK CONSTRAINT '+QUOTENAME(cc.name)+';' ELSE '' END
FROM sys.check_constraints cc
JOIN sys.objects o ON o.object_id=cc.parent_object_id
JOIN #tbl k ON k.object_id=o.object_id AND k.kind IN ('normal','memopt','node','edge');
'@)

    Add-Section 'foreign keys' (Fill @'
INSERT #ddl(phase, ord, txt)
SELECT 41, 0, 'ALTER TABLE '+QUOTENAME(SCHEMA_NAME(po.schema_id))+'.'+QUOTENAME(po.name)
  + CASE WHEN fk.is_not_trusted=1 THEN ' WITH NOCHECK' ELSE ' WITH CHECK' END
  + ' ADD CONSTRAINT '+QUOTENAME(fk.name)+' FOREIGN KEY ('
  + STUFF((SELECT ', '+QUOTENAME(COL_NAME(fkc.parent_object_id,fkc.parent_column_id)) FROM sys.foreign_key_columns fkc WHERE fkc.constraint_object_id=fk.object_id ORDER BY fkc.constraint_column_id FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),1,2,'')
  + ') REFERENCES '+QUOTENAME(SCHEMA_NAME(ro.schema_id))+'.'+QUOTENAME(ro.name)+' ('
  + STUFF((SELECT ', '+QUOTENAME(COL_NAME(fkc.referenced_object_id,fkc.referenced_column_id)) FROM sys.foreign_key_columns fkc WHERE fkc.constraint_object_id=fk.object_id ORDER BY fkc.constraint_column_id FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),1,2,'')
  + ') ON DELETE '+REPLACE(fk.delete_referential_action_desc,'_',' ')
  + ' ON UPDATE '+REPLACE(fk.update_referential_action_desc,'_',' ')
  + CASE WHEN fk.is_not_for_replication=1 THEN ' NOT FOR REPLICATION' ELSE '' END + ';'
  + CASE WHEN fk.is_disabled=1 THEN @nl+'ALTER TABLE '+QUOTENAME(SCHEMA_NAME(po.schema_id))+'.'+QUOTENAME(po.name)+' NOCHECK CONSTRAINT '+QUOTENAME(fk.name)+';' ELSE '' END
FROM sys.foreign_keys fk
JOIN sys.objects po ON po.object_id=fk.parent_object_id
JOIN sys.objects ro ON ro.object_id=fk.referenced_object_id
JOIN #tbl k ON k.object_id=po.object_id AND k.kind IN ('normal','memopt','node','edge');
'@)

    if ($v -ge 15) {
        Add-Section 'edge constraints' @'
INSERT #ddl(phase, ord, txt)
SELECT 42, 0, 'ALTER TABLE '+QUOTENAME(SCHEMA_NAME(po.schema_id))+'.'+QUOTENAME(po.name)+' ADD CONSTRAINT '+QUOTENAME(ec.name)+' CONNECTION ('
  +STUFF((SELECT ', '+QUOTENAME(SCHEMA_NAME(fo.schema_id))+'.'+QUOTENAME(fo.name)+' TO '+QUOTENAME(SCHEMA_NAME(tobj.schema_id))+'.'+QUOTENAME(tobj.name)
          FROM sys.edge_constraint_clauses ecc
          JOIN sys.objects fo ON fo.object_id=ecc.from_object_id
          JOIN sys.objects tobj ON tobj.object_id=ecc.to_object_id
          WHERE ecc.object_id=ec.object_id ORDER BY ecc.clause_number FOR XML PATH(''),TYPE).value('.','nvarchar(max)'),1,2,'')+')'
  +CASE WHEN ec.delete_referential_action=1 THEN ' ON DELETE CASCADE' ELSE '' END+';'
FROM sys.edge_constraints ec JOIN sys.objects po ON po.object_id=ec.parent_object_id;
'@
    }

    Add-Section 'change tracking (tables)' @'
INSERT #ddl(phase, ord, txt)
SELECT 43, 0, 'ALTER TABLE '+QUOTENAME(SCHEMA_NAME(t.schema_id))+'.'+QUOTENAME(t.name)+' ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = '+CASE WHEN ctt.is_track_columns_updated_on=1 THEN 'ON' ELSE 'OFF' END+');'
FROM sys.change_tracking_tables ctt JOIN sys.tables t ON t.object_id=ctt.object_id;
'@

    Add-Section 'disabled indexes' (Fill @'
INSERT #ddl(phase, ord, txt)
SELECT 58, 9, 'ALTER INDEX '+QUOTENAME(i.name)+' ON '+{{QN_O}}+' DISABLE;'
FROM sys.indexes i JOIN sys.objects o ON o.object_id=i.object_id
WHERE i.is_disabled=1 AND i.index_id>0 AND o.is_ms_shipped=0 AND o.type IN ('U','V')
  AND (o.type='V' OR EXISTS (SELECT 1 FROM #tbl k WHERE k.object_id=o.object_id AND k.kind IN ('normal','node','edge')));
'@)

    # ---- 19 / 50 / 52 / 53: modules ----------------------------------------------------
    $modPre  = "CASE WHEN sm.uses_ansi_nulls=0 THEN 'SET ANSI_NULLS OFF;'+@nl+'GO'+@nl ELSE '' END + CASE WHEN sm.uses_quoted_identifier=0 THEN 'SET QUOTED_IDENTIFIER OFF;'+@nl+'GO'+@nl ELSE '' END"
    $modPost = "CASE WHEN sm.uses_ansi_nulls=0 OR sm.uses_quoted_identifier=0 THEN @nl+'GO'+@nl+'SET ANSI_NULLS ON;'+@nl+'SET QUOTED_IDENTIFIER ON;' ELSE '' END"
    $ledgerView = Gate ($v -ge 16) "AND NOT EXISTS (SELECT 1 FROM sys.views lv WHERE lv.object_id=o.object_id AND lv.ledger_view_type<>0)"
    Add-Section 'views and functions' (@"
INSERT #ddl(phase, ord, txt)
SELECT CASE WHEN m.early=1 THEN 19 ELSE 50 END, m.rnk, $modPre + sm.definition + $modPost
FROM #mod m
JOIN sys.objects o ON o.object_id=m.object_id
JOIN sys.sql_modules sm ON sm.object_id=o.object_id
WHERE o.type IN ('V','FN','IF','TF') AND sm.definition IS NOT NULL $ledgerView;
"@)

    Add-Section 'procedures' @"
INSERT #ddl(phase, ord, txt)
SELECT 52, 0, $modPre + sm.definition + $modPost
FROM sys.objects o JOIN sys.sql_modules sm ON sm.object_id=o.object_id
WHERE o.is_ms_shipped=0 AND o.type='P' AND sm.definition IS NOT NULL;
"@

    Add-Section 'DML triggers' @"
INSERT #ddl(phase, ord, txt)
SELECT 53, 0, $modPre + sm.definition
  + CASE WHEN tr.is_disabled=1 THEN @nl+'GO'+@nl+'DISABLE TRIGGER '+QUOTENAME(SCHEMA_NAME(o.schema_id))+'.'+QUOTENAME(o.name)+' ON '+QUOTENAME(SCHEMA_NAME(pt.schema_id))+'.'+QUOTENAME(pt.name)+';' ELSE '' END
  + $modPost
FROM sys.objects o
JOIN sys.sql_modules sm ON sm.object_id=o.object_id
JOIN sys.triggers tr ON tr.object_id=o.object_id
JOIN sys.objects pt ON pt.object_id=tr.parent_id
WHERE o.is_ms_shipped=0 AND o.type='TR' AND sm.definition IS NOT NULL;
"@

    Add-Section 'encrypted modules' @'
INSERT #ddl(phase, ord, txt)
SELECT 1, 8, '-- !! NOT SCRIPTED (WITH ENCRYPTION, definition unavailable): '+o.type_desc+' '+QUOTENAME(SCHEMA_NAME(o.schema_id))+'.'+QUOTENAME(o.name)
FROM sys.objects o JOIN sys.sql_modules sm ON sm.object_id=o.object_id
WHERE o.is_ms_shipped=0 AND o.type IN ('V','FN','IF','TF','P','TR') AND sm.definition IS NULL;
'@

    # ---- 55-58: synonyms, rules, defaults and their bindings ------------------------------
    Add-Section 'synonyms, rules, defaults' @'
INSERT #ddl(phase, ord, txt)
SELECT 55, 0, 'CREATE SYNONYM '+QUOTENAME(SCHEMA_NAME(sn.schema_id))+'.'+QUOTENAME(sn.name)+' FOR '+sn.base_object_name+';'
FROM sys.synonyms sn WHERE sn.is_ms_shipped = 0;

INSERT #ddl(phase, ord, txt)
SELECT 55, 1, sm.definition
FROM sys.sql_modules sm JOIN sys.objects o ON o.object_id=sm.object_id
WHERE o.type IN ('R','D') AND o.parent_object_id=0 AND o.is_ms_shipped=0 AND sm.definition IS NOT NULL;

/* bindings: sp_bindrule / sp_bindefault on columns and alias types */
INSERT #ddl(phase, ord, txt)
SELECT 58, 0, 'EXEC sys.sp_bindefault N'''+REPLACE(QUOTENAME(SCHEMA_NAME(d.schema_id))+'.'+QUOTENAME(d.name),'''','''''')+''', N'''+REPLACE(QUOTENAME(SCHEMA_NAME(t.schema_id))+'.'+QUOTENAME(t.name)+'.'+QUOTENAME(c.name),'''','''''')+''';'
FROM sys.columns c JOIN sys.objects t ON t.object_id=c.object_id AND t.type='U' AND t.is_ms_shipped=0
JOIN sys.objects d ON d.object_id=c.default_object_id AND d.type='D' AND d.parent_object_id=0;

INSERT #ddl(phase, ord, txt)
SELECT 58, 1, 'EXEC sys.sp_bindrule N'''+REPLACE(QUOTENAME(SCHEMA_NAME(r.schema_id))+'.'+QUOTENAME(r.name),'''','''''')+''', N'''+REPLACE(QUOTENAME(SCHEMA_NAME(t.schema_id))+'.'+QUOTENAME(t.name)+'.'+QUOTENAME(c.name),'''','''''')+''';'
FROM sys.columns c JOIN sys.objects t ON t.object_id=c.object_id AND t.type='U' AND t.is_ms_shipped=0
JOIN sys.objects r ON r.object_id=c.rule_object_id AND r.type='R';

INSERT #ddl(phase, ord, txt)
SELECT 58, 2, 'EXEC sys.sp_bindefault N'''+REPLACE(QUOTENAME(SCHEMA_NAME(d.schema_id))+'.'+QUOTENAME(d.name),'''','''''')+''', N'''+REPLACE(QUOTENAME(SCHEMA_NAME(ty.schema_id))+'.'+QUOTENAME(ty.name),'''','''''')+''';'
FROM sys.types ty JOIN sys.objects d ON d.object_id=ty.default_object_id AND d.type='D' AND d.parent_object_id=0
WHERE ty.is_user_defined=1;

INSERT #ddl(phase, ord, txt)
SELECT 58, 3, 'EXEC sys.sp_bindrule N'''+REPLACE(QUOTENAME(SCHEMA_NAME(r.schema_id))+'.'+QUOTENAME(r.name),'''','''''')+''', N'''+REPLACE(QUOTENAME(SCHEMA_NAME(ty.schema_id))+'.'+QUOTENAME(ty.name),'''','''''')+''';'
FROM sys.types ty JOIN sys.objects r ON r.object_id=ty.rule_object_id AND r.type='R'
WHERE ty.is_user_defined=1;
'@

    # ---- 57: full-text -----------------------------------------------------------------------
    $ftStop = "ISNULL(', STOPLIST = '+CASE WHEN fi.stoplist_id IS NULL THEN 'OFF' WHEN fi.stoplist_id=0 THEN 'SYSTEM' ELSE QUOTENAME(sl.name) END,'')"
    Add-Section 'full-text' (@"
INSERT #ddl(phase, ord, txt)
SELECT 57, 0, 'CREATE FULLTEXT STOPLIST '+QUOTENAME(sl.name)+';' FROM sys.fulltext_stoplists sl;

INSERT #ddl(phase, ord, txt)
SELECT 57, 1, 'ALTER FULLTEXT STOPLIST '+QUOTENAME(sl.name)+' ADD N'''+REPLACE(sw.stopword,'''','''''')+''' LANGUAGE '+CAST(sw.language_id AS varchar(10))+';'
FROM sys.fulltext_stopwords sw JOIN sys.fulltext_stoplists sl ON sl.stoplist_id=sw.stoplist_id;

INSERT #ddl(phase, ord, txt)
SELECT 57, 2, 'CREATE FULLTEXT CATALOG '+QUOTENAME(fc.name)+' WITH ACCENT_SENSITIVITY = '+CASE WHEN fc.is_accent_sensitivity_on=1 THEN 'ON' ELSE 'OFF' END
  + CASE WHEN fc.is_default=1 THEN ' AS DEFAULT' ELSE '' END+';'
FROM sys.fulltext_catalogs fc;

INSERT #ddl(phase, ord, txt)
SELECT 57, 3, 'CREATE FULLTEXT INDEX ON '+$QN_O+' ('
  + STUFF((SELECT ', '+QUOTENAME(COL_NAME(fic.object_id,fic.column_id))
                 + ISNULL(' TYPE COLUMN '+QUOTENAME(COL_NAME(fic.object_id,NULLIF(fic.type_column_id,0))),'')
                 + ISNULL(' LANGUAGE '+CAST(fic.language_id AS varchar(10)),'')
           FROM sys.fulltext_index_columns fic
           WHERE fic.object_id=fi.object_id ORDER BY fic.column_id
           FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),1,2,'')
  + ') KEY INDEX '+QUOTENAME(ki.name)
  + ' ON '+QUOTENAME(fc.name)
  + ' WITH (CHANGE_TRACKING = '+fi.change_tracking_state_desc+CASE WHEN fi.change_tracking_state_desc='OFF' THEN ', NO POPULATION' ELSE '' END+$ftStop+');'
FROM sys.fulltext_indexes fi
JOIN sys.objects o ON o.object_id=fi.object_id
JOIN sys.indexes ki ON ki.object_id=fi.object_id AND ki.index_id=fi.unique_index_id
JOIN sys.fulltext_catalogs fc ON fc.fulltext_catalog_id=fi.fulltext_catalog_id
LEFT JOIN sys.fulltext_stoplists sl ON sl.stoplist_id=fi.stoplist_id;
"@)

    # ---- 59: Service Broker --------------------------------------------------------------------
    Add-Section 'service broker' @'
INSERT #ddl(phase, ord, txt)
SELECT 59, 0, 'CREATE MESSAGE TYPE '+QUOTENAME(mt.name)+' AUTHORIZATION '+QUOTENAME(USER_NAME(mt.principal_id))
  +' VALIDATION = '+CASE mt.validation WHEN 'N' THEN 'NONE' WHEN 'E' THEN 'EMPTY' WHEN 'X' THEN 'WELL_FORMED_XML'
        ELSE 'VALID_XML WITH SCHEMA COLLECTION '+QUOTENAME(SCHEMA_NAME(xc.schema_id))+'.'+QUOTENAME(xc.name) END+';'
FROM sys.service_message_types mt
LEFT JOIN sys.xml_schema_collections xc ON xc.xml_collection_id=mt.xml_collection_id
WHERE mt.message_type_id > 65535;

INSERT #ddl(phase, ord, txt)
SELECT 59, 1, 'CREATE CONTRACT '+QUOTENAME(sc.name)+' AUTHORIZATION '+QUOTENAME(USER_NAME(sc.principal_id))+' ('
  +STUFF((SELECT ', '+QUOTENAME(mt.name)+' SENT BY '+CASE WHEN u.is_sent_by_initiator=1 AND u.is_sent_by_target=1 THEN 'ANY' WHEN u.is_sent_by_initiator=1 THEN 'INITIATOR' ELSE 'TARGET' END
          FROM sys.service_contract_message_usages u JOIN sys.service_message_types mt ON mt.message_type_id=u.message_type_id
          WHERE u.service_contract_id=sc.service_contract_id ORDER BY mt.name FOR XML PATH(''),TYPE).value('.','nvarchar(max)'),1,2,'')+');'
FROM sys.service_contracts sc WHERE sc.service_contract_id > 65535;

INSERT #ddl(phase, ord, txt)
SELECT 59, 2, 'CREATE QUEUE '+QUOTENAME(SCHEMA_NAME(o.schema_id))+'.'+QUOTENAME(o.name)
  +' WITH STATUS = '+CASE WHEN q.is_receive_enabled=1 THEN 'ON' ELSE 'OFF' END
  +', RETENTION = '+CASE WHEN q.is_retention_enabled=1 THEN 'ON' ELSE 'OFF' END
  +CASE WHEN q.is_activation_enabled=1 AND q.activation_procedure IS NOT NULL
        THEN ', ACTIVATION (STATUS = ON, PROCEDURE_NAME = '+q.activation_procedure+', MAX_QUEUE_READERS = '+CAST(q.max_readers AS varchar(10))
             +', EXECUTE AS '+CASE WHEN q.execute_as_principal_id IS NULL THEN 'SELF' WHEN q.execute_as_principal_id=-2 THEN 'OWNER' ELSE QUOTENAME(USER_NAME(q.execute_as_principal_id)) END+')'
        ELSE '' END+';'
FROM sys.service_queues q JOIN sys.objects o ON o.object_id=q.object_id WHERE o.is_ms_shipped=0;

INSERT #ddl(phase, ord, txt)
SELECT 59, 3, 'CREATE SERVICE '+QUOTENAME(s.name)+' AUTHORIZATION '+QUOTENAME(USER_NAME(s.principal_id))
  +' ON QUEUE '+QUOTENAME(SCHEMA_NAME(qo.schema_id))+'.'+QUOTENAME(qo.name)
  +ISNULL(' ('+STUFF((SELECT ', '+QUOTENAME(c.name) FROM sys.service_contract_usages u JOIN sys.service_contracts c ON c.service_contract_id=u.service_contract_id
                      WHERE u.service_id=s.service_id ORDER BY c.name FOR XML PATH(''),TYPE).value('.','nvarchar(max)'),1,2,'')+')','')+';'
FROM sys.services s JOIN sys.objects qo ON qo.object_id=s.service_queue_id
WHERE s.service_id > 65535;

INSERT #ddl(phase, ord, txt)
SELECT 59, 4, 'CREATE ROUTE '+QUOTENAME(r.name)+' AUTHORIZATION '+QUOTENAME(USER_NAME(r.principal_id))+' WITH '
  +STUFF(ISNULL(', SERVICE_NAME = N'''+REPLACE(r.remote_service_name,'''','''''')+'''','')
        +ISNULL(', BROKER_INSTANCE = N'''+r.broker_instance+'''','')
        +ISNULL(', LIFETIME = '+CAST(r.lifetime AS varchar(20)),'')
        +', ADDRESS = N'''+REPLACE(r.address,'''','''''')+''''
        +ISNULL(', MIRROR_ADDRESS = N'''+REPLACE(r.mirror_address,'''','''''')+'''',''),1,2,'')+';'
FROM sys.routes r WHERE r.name <> 'AutoCreatedLocal';

INSERT #ddl(phase, ord, txt)
SELECT 59, 5, 'CREATE REMOTE SERVICE BINDING '+QUOTENAME(b.name)+' AUTHORIZATION '+QUOTENAME(USER_NAME(b.principal_id))
  +' TO SERVICE N'''+REPLACE(b.remote_service_name,'''','''''')+''' WITH USER = '+QUOTENAME(USER_NAME(b.remote_principal_id))
  +', ANONYMOUS = '+CASE WHEN b.is_anonymous_on=1 THEN 'ON' ELSE 'OFF' END+';'
FROM sys.remote_service_bindings b;
'@

    # ---- 60: row-level security --------------------------------------------------------------
    if ($v -ge 13) {
        Add-Section 'security policies' @'
INSERT #ddl(phase, ord, txt)
SELECT 60, 0, 'CREATE SECURITY POLICY '+QUOTENAME(SCHEMA_NAME(sp.schema_id))+'.'+QUOTENAME(sp.name)
  + STUFF((SELECT @nl+'    ADD '+pr.predicate_type_desc+' PREDICATE '
                  + CASE WHEN LEFT(LTRIM(pr.predicate_definition),1)='(' AND RIGHT(RTRIM(pr.predicate_definition),1)=')'
                         THEN SUBSTRING(LTRIM(RTRIM(pr.predicate_definition)),2,LEN(LTRIM(RTRIM(pr.predicate_definition)))-2)
                         ELSE pr.predicate_definition END
                  +' ON '+QUOTENAME(OBJECT_SCHEMA_NAME(pr.target_object_id))+'.'+QUOTENAME(OBJECT_NAME(pr.target_object_id))
                  + ISNULL(' '+pr.operation_desc,'') + ','
           FROM sys.security_predicates pr WHERE pr.object_id=sp.object_id
           ORDER BY pr.security_predicate_id FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),1,0,'')
  + '@@' + @nl + '    WITH (STATE = '+CASE WHEN sp.is_enabled=1 THEN 'ON' ELSE 'OFF' END
  + ', SCHEMABINDING = '+CASE WHEN sp.is_schema_bound=1 THEN 'ON' ELSE 'OFF' END+');'
FROM sys.security_policies sp;
UPDATE #ddl SET txt = REPLACE(txt,',@@','') WHERE phase=60;
'@
    }

    # ---- 66: plan guides ---------------------------------------------------------------------
    Add-Section 'plan guides' @'
INSERT #ddl(phase, ord, txt)
SELECT 66, 0, 'EXEC sys.sp_create_plan_guide @name=N'''+REPLACE(pg.name,'''','''''')+''', @stmt=N'''+REPLACE(pg.query_text,'''','''''')
  +''', @type=N'''+pg.scope_type_desc+''', @module_or_batch='
  +CASE pg.scope_type_desc WHEN 'OBJECT' THEN 'N'''+REPLACE(QUOTENAME(OBJECT_SCHEMA_NAME(pg.scope_object_id))+'.'+QUOTENAME(OBJECT_NAME(pg.scope_object_id)),'''','''''')+''''
        WHEN 'SQL' THEN ISNULL('N'''+REPLACE(pg.scope_batch,'''','''''')+'''','NULL') ELSE 'NULL' END
  +', @params='+ISNULL('N'''+REPLACE(pg.parameters,'''','''''')+'''','NULL')
  +', @hints='+ISNULL('N'''+REPLACE(pg.hints,'''','''''')+'''','NULL')+';'
  +CASE WHEN pg.is_disabled=1 THEN @nl+'EXEC sys.sp_control_plan_guide N''DISABLE'', N'''+REPLACE(pg.name,'''','''''')+''';' ELSE '' END
FROM sys.plan_guides pg;
'@

    # ---- 70-75: membership and permissions ----------------------------------------------------
    Add-Section 'role membership' (Fill @'
INSERT #ddl(phase, ord, txt)
SELECT 72, 0, {{ROLE_ADD}}
FROM sys.database_role_members rm
JOIN sys.database_principals r ON r.principal_id=rm.role_principal_id
JOIN sys.database_principals m ON m.principal_id=rm.member_principal_id
WHERE m.name <> 'dbo';
'@)

    Add-Section 'revoke implicit user grants' (Fill @'
INSERT #ddl(phase, ord, txt)
SELECT 73, 0, 'REVOKE '+d.perm+' FROM '+QUOTENAME(dp.name)+';'
FROM sys.database_principals dp
CROSS APPLY (VALUES ('CONNECT'),('BACKUP DATABASE'),('BACKUP LOG')) d(perm)
WHERE dp.type IN ('S','U','G') AND dp.principal_id>4 AND dp.sid IS NOT NULL {{USER_NOTCONTAINED_SQL}}
  AND NOT EXISTS (SELECT 1 FROM sys.database_permissions p
                  WHERE p.grantee_principal_id = dp.principal_id
                    AND p.class = 0
                    AND RTRIM(p.permission_name) COLLATE DATABASE_DEFAULT = d.perm);
'@)

    Add-Section 'permissions' @'
INSERT #ddl(phase, ord, txt)
SELECT 75, 0,
  CASE WHEN x.target IS NULL THEN '-- !! NOT SCRIPTED (permission on '+perm.class_desc+' major_id '+CAST(perm.major_id AS varchar(12))+'): '+perm.state_desc+' '+perm.permission_name+' TO '+QUOTENAME(dp.name)
       ELSE CASE WHEN perm.state_desc='GRANT_WITH_GRANT_OPTION' THEN 'GRANT' ELSE perm.state_desc END
            + ' '+perm.permission_name + x.target
            + ' TO '+QUOTENAME(dp.name)
            + CASE WHEN perm.state_desc='GRANT_WITH_GRANT_OPTION' THEN ' WITH GRANT OPTION' ELSE '' END + ';' END
FROM sys.database_permissions perm
JOIN sys.database_principals dp ON dp.principal_id=perm.grantee_principal_id
LEFT JOIN sys.objects o ON o.object_id=perm.major_id AND perm.class=1
LEFT JOIN sys.database_principals tp ON tp.principal_id=perm.major_id AND perm.class=4
LEFT JOIN sys.assemblies asm ON asm.assembly_id=perm.major_id AND perm.class=5
LEFT JOIN sys.types ty ON ty.user_type_id=perm.major_id AND perm.class=6
LEFT JOIN sys.xml_schema_collections xs ON xs.xml_collection_id=perm.major_id AND perm.class=10
LEFT JOIN sys.service_message_types smt ON smt.message_type_id=perm.major_id AND perm.class=15
LEFT JOIN sys.service_contracts scn ON scn.service_contract_id=perm.major_id AND perm.class=16
LEFT JOIN sys.services svc ON svc.service_id=perm.major_id AND perm.class=17
LEFT JOIN sys.remote_service_bindings rsb ON rsb.remote_service_binding_id=perm.major_id AND perm.class=18
LEFT JOIN sys.routes rt ON rt.route_id=perm.major_id AND perm.class=19
LEFT JOIN sys.fulltext_catalogs ftc ON ftc.fulltext_catalog_id=perm.major_id AND perm.class=23
LEFT JOIN sys.symmetric_keys sk ON sk.symmetric_key_id=perm.major_id AND perm.class=24
LEFT JOIN sys.certificates ce ON ce.certificate_id=perm.major_id AND perm.class=25
LEFT JOIN sys.asymmetric_keys ak ON ak.asymmetric_key_id=perm.major_id AND perm.class=26
CROSS APPLY (SELECT CASE perm.class
    WHEN 0  THEN ''
    WHEN 1  THEN CASE WHEN o.object_id IS NULL OR o.is_ms_shipped=1 THEN NULL
                      ELSE ' ON '+QUOTENAME(SCHEMA_NAME(o.schema_id))+'.'+QUOTENAME(o.name)+ISNULL(' ('+QUOTENAME(COL_NAME(perm.major_id, NULLIF(perm.minor_id,0)))+')','') END
    WHEN 3  THEN ' ON SCHEMA::'+QUOTENAME(SCHEMA_NAME(perm.major_id))
    WHEN 4  THEN ' ON '+CASE tp.type WHEN 'R' THEN 'ROLE' WHEN 'A' THEN 'APPLICATION ROLE' ELSE 'USER' END+'::'+QUOTENAME(tp.name)
    WHEN 5  THEN ' ON ASSEMBLY::'+QUOTENAME(asm.name)
    WHEN 6  THEN ' ON TYPE::'+QUOTENAME(SCHEMA_NAME(ty.schema_id))+'.'+QUOTENAME(ty.name)
    WHEN 10 THEN ' ON XML SCHEMA COLLECTION::'+QUOTENAME(SCHEMA_NAME(xs.schema_id))+'.'+QUOTENAME(xs.name)
    WHEN 15 THEN ' ON MESSAGE TYPE::'+QUOTENAME(smt.name)
    WHEN 16 THEN ' ON CONTRACT::'+QUOTENAME(scn.name)
    WHEN 17 THEN ' ON SERVICE::'+QUOTENAME(svc.name)
    WHEN 18 THEN ' ON REMOTE SERVICE BINDING::'+QUOTENAME(rsb.name)
    WHEN 19 THEN ' ON ROUTE::'+QUOTENAME(rt.name)
    WHEN 23 THEN ' ON FULLTEXT CATALOG::'+QUOTENAME(ftc.name)
    WHEN 24 THEN ' ON SYMMETRIC KEY::'+QUOTENAME(sk.name)
    WHEN 25 THEN ' ON CERTIFICATE::'+QUOTENAME(ce.name)
    WHEN 26 THEN ' ON ASYMMETRIC KEY::'+QUOTENAME(ak.name)
    ELSE NULL END AS target) x
WHERE dp.is_fixed_role=0 AND (dp.principal_id>4 OR dp.name IN ('public','guest'))
  AND NOT (perm.class=1 AND o.is_ms_shipped=1)
  AND NOT (perm.class=1 AND o.object_id IS NULL);
'@

    # ---- 80: extended properties (every class we can address) ------------------------------------
    Add-Section 'extended properties' @'
INSERT #ddl(phase, ord, txt)
SELECT CASE WHEN x.l0t = 'TRIGGER' THEN 97 ELSE 80 END, 0, 'EXEC sys.sp_addextendedproperty @name=N'''+REPLACE(x.name,'''','''''')+''', @value=N'''+REPLACE(ISNULL(CAST(x.value AS nvarchar(max)),N''),'''','''''')+''''
  + ISNULL(', @level0type=N'''+x.l0t+''', @level0name=N'''+REPLACE(x.l0n,'''','''''')+'''','')
  + ISNULL(', @level1type=N'''+x.l1t+''', @level1name=N'''+REPLACE(x.l1n,'''','''''')+'''','')
  + ISNULL(', @level2type=N'''+x.l2t+''', @level2name=N'''+REPLACE(x.l2n,'''','''''')+'''','') + ';'
FROM (
  /* database */
  SELECT ep.name AS name, ep.value AS value, CAST(NULL AS nvarchar(60)) l0t, CAST(NULL AS nvarchar(256)) l0n, CAST(NULL AS nvarchar(60)) l1t, CAST(NULL AS nvarchar(256)) l1n, CAST(NULL AS nvarchar(60)) l2t, CAST(NULL AS nvarchar(256)) l2n
  FROM sys.extended_properties ep WHERE ep.class = 0
  UNION ALL
  /* objects, columns, constraints, DML triggers, and database-level DDL triggers */
  SELECT ep.name, ep.value,
    CASE WHEN tr.object_id IS NOT NULL AND tr.parent_class=0 THEN 'TRIGGER' ELSE 'SCHEMA' END,
    CASE WHEN tr.object_id IS NOT NULL AND tr.parent_class=0 THEN tr.name ELSE SCHEMA_NAME(b.schema_id) END,
    CASE WHEN tr.object_id IS NOT NULL AND tr.parent_class=0 THEN NULL ELSE
      CASE WHEN b.type='U' THEN 'TABLE' WHEN b.type='V' THEN 'VIEW' WHEN b.type IN ('P','PC') THEN 'PROCEDURE'
           WHEN b.type IN ('FN','IF','TF','FS','FT') THEN 'FUNCTION' WHEN b.type='AF' THEN 'AGGREGATE' WHEN b.type='SN' THEN 'SYNONYM'
           WHEN b.type='SO' THEN 'SEQUENCE' WHEN b.type='SQ' THEN 'QUEUE' WHEN b.type='R' THEN 'RULE' WHEN b.type='D' THEN 'DEFAULT' END END,
    CASE WHEN tr.object_id IS NOT NULL AND tr.parent_class=0 THEN NULL ELSE b.name END,
    CASE WHEN o.object_id<>b.object_id THEN CASE WHEN o.type='TR' THEN 'TRIGGER' ELSE 'CONSTRAINT' END
         WHEN ep.minor_id>0 THEN 'COLUMN' END,
    CASE WHEN o.object_id<>b.object_id THEN o.name
         WHEN ep.minor_id>0 THEN COL_NAME(ep.major_id, ep.minor_id) END
  FROM sys.extended_properties ep
  LEFT JOIN sys.objects o ON o.object_id=ep.major_id
  LEFT JOIN sys.objects b ON b.object_id = CASE WHEN o.parent_object_id<>0 AND o.type IN ('C','D','PK','UQ','F','TR') THEN o.parent_object_id ELSE o.object_id END
  LEFT JOIN sys.triggers tr ON tr.object_id=ep.major_id AND tr.parent_class=0
  WHERE ep.class = 1 AND (b.is_ms_shipped=0 OR tr.object_id IS NOT NULL)
  UNION ALL
  /* parameters */
  SELECT ep.name, ep.value, 'SCHEMA', SCHEMA_NAME(o.schema_id),
    CASE WHEN o.type IN ('P','PC') THEN 'PROCEDURE' ELSE 'FUNCTION' END, o.name, 'PARAMETER', p.name
  FROM sys.extended_properties ep JOIN sys.objects o ON o.object_id=ep.major_id JOIN sys.parameters p ON p.object_id=ep.major_id AND p.parameter_id=ep.minor_id
  WHERE ep.class = 2 AND o.is_ms_shipped=0
  UNION ALL
  SELECT ep.name, ep.value, 'SCHEMA', s.name, NULL, NULL, NULL, NULL FROM sys.extended_properties ep JOIN sys.schemas s ON s.schema_id=ep.major_id WHERE ep.class = 3
  UNION ALL
  SELECT ep.name, ep.value, 'USER', dp.name, NULL, NULL, NULL, NULL FROM sys.extended_properties ep JOIN sys.database_principals dp ON dp.principal_id=ep.major_id WHERE ep.class = 4
  UNION ALL
  SELECT ep.name, ep.value, 'ASSEMBLY', a.name, NULL, NULL, NULL, NULL FROM sys.extended_properties ep JOIN sys.assemblies a ON a.assembly_id=ep.major_id WHERE ep.class = 5
  UNION ALL
  SELECT ep.name, ep.value, 'SCHEMA', SCHEMA_NAME(t.schema_id), 'TYPE', t.name, NULL, NULL FROM sys.extended_properties ep JOIN sys.types t ON t.user_type_id=ep.major_id WHERE ep.class = 6
  UNION ALL
  SELECT ep.name, ep.value, 'SCHEMA', SCHEMA_NAME(o.schema_id), CASE WHEN o.type='V' THEN 'VIEW' ELSE 'TABLE' END, o.name, 'INDEX', i.name
  FROM sys.extended_properties ep JOIN sys.objects o ON o.object_id=ep.major_id JOIN sys.indexes i ON i.object_id=ep.major_id AND i.index_id=ep.minor_id
  WHERE ep.class = 7 AND o.is_ms_shipped=0
  UNION ALL
  SELECT ep.name, ep.value, 'SCHEMA', SCHEMA_NAME(x.schema_id), 'XML SCHEMA COLLECTION', x.name, NULL, NULL FROM sys.extended_properties ep JOIN sys.xml_schema_collections x ON x.xml_collection_id=ep.major_id WHERE ep.class = 10
  UNION ALL
  SELECT ep.name, ep.value, 'MESSAGE TYPE', m.name, NULL, NULL, NULL, NULL FROM sys.extended_properties ep JOIN sys.service_message_types m ON m.message_type_id=ep.major_id WHERE ep.class = 15
  UNION ALL
  SELECT ep.name, ep.value, 'CONTRACT', c.name, NULL, NULL, NULL, NULL FROM sys.extended_properties ep JOIN sys.service_contracts c ON c.service_contract_id=ep.major_id WHERE ep.class = 16
  UNION ALL
  SELECT ep.name, ep.value, 'SERVICE', s.name, NULL, NULL, NULL, NULL FROM sys.extended_properties ep JOIN sys.services s ON s.service_id=ep.major_id WHERE ep.class = 17
  UNION ALL
  SELECT ep.name, ep.value, 'REMOTE SERVICE BINDING', b.name, NULL, NULL, NULL, NULL FROM sys.extended_properties ep JOIN sys.remote_service_bindings b ON b.remote_service_binding_id=ep.major_id WHERE ep.class = 18
  UNION ALL
  SELECT ep.name, ep.value, 'ROUTE', r.name, NULL, NULL, NULL, NULL FROM sys.extended_properties ep JOIN sys.routes r ON r.route_id=ep.major_id WHERE ep.class = 19
  UNION ALL
  SELECT ep.name, ep.value, 'PARTITION FUNCTION', f.name, NULL, NULL, NULL, NULL FROM sys.extended_properties ep JOIN sys.partition_functions f ON f.function_id=ep.major_id WHERE ep.class = 21
  UNION ALL
  SELECT ep.name, ep.value, 'PARTITION SCHEME', s.name, NULL, NULL, NULL, NULL FROM sys.extended_properties ep JOIN sys.partition_schemes s ON s.data_space_id=ep.major_id WHERE ep.class = 20
) x
WHERE x.l1t IS NOT NULL OR x.l0t IS NULL OR x.l0t <> 'SCHEMA' OR x.l1n IS NULL;

INSERT #ddl(phase, ord, txt)
SELECT 1, 4, '-- !! NOT SCRIPTED: '+CAST(COUNT(*) AS varchar(12))+' extended propert'+CASE WHEN COUNT(*)=1 THEN 'y' ELSE 'ies' END+' of class '+CAST(ep.class AS varchar(5))+' ('+ep.class_desc+')'
FROM sys.extended_properties ep
WHERE ep.class NOT IN (0,1,2,3,4,5,6,7,10,15,16,17,18,19,20,21)
GROUP BY ep.class, ep.class_desc;
'@

    # ---- 85: sensitivity classifications (2019+) ----------------------------------------------------
    if ($v -ge 15) {
        Add-Section 'sensitivity classifications' @'
INSERT #ddl(phase, ord, txt)
SELECT 85, 0, 'ADD SENSITIVITY CLASSIFICATION TO '+QUOTENAME(SCHEMA_NAME(o.schema_id))+'.'+QUOTENAME(o.name)+'.'+QUOTENAME(COL_NAME(sc.major_id,sc.minor_id))
  +' WITH ('+STUFF(ISNULL(', LABEL='''+REPLACE(CAST(sc.label AS nvarchar(4000)),'''','''''')+'''','')
                  +ISNULL(', INFORMATION_TYPE='''+REPLACE(CAST(sc.information_type AS nvarchar(4000)),'''','''''')+'''','')
                  +ISNULL(', RANK='+sc.rank_desc,''),1,2,'')+');'
FROM sys.sensitivity_classifications sc JOIN sys.objects o ON o.object_id=sc.major_id
WHERE sc.class = 1 AND (sc.label IS NOT NULL OR sc.information_type IS NOT NULL OR sc.rank_desc IS NOT NULL);
'@
    }

    # ---- 90+: database options ----------------------------------------------------------------------
    Add-Section 'database options' @'
INSERT #ddl(phase, ord, txt)
SELECT 90, v.n, 'ALTER DATABASE CURRENT SET '+v.s+';'
FROM sys.databases d
CROSS APPLY (VALUES
  (1,  'RECOVERY '+d.recovery_model_desc),
  (2,  'AUTO_CLOSE '+CASE WHEN d.is_auto_close_on=1 THEN 'ON' ELSE 'OFF' END),
  (3,  'AUTO_SHRINK '+CASE WHEN d.is_auto_shrink_on=1 THEN 'ON' ELSE 'OFF' END),
  (4,  'AUTO_CREATE_STATISTICS '+CASE WHEN d.is_auto_create_stats_on=1 THEN 'ON' ELSE 'OFF' END),
  (5,  'AUTO_UPDATE_STATISTICS '+CASE WHEN d.is_auto_update_stats_on=1 THEN 'ON' ELSE 'OFF' END),
  (6,  'AUTO_UPDATE_STATISTICS_ASYNC '+CASE WHEN d.is_auto_update_stats_async_on=1 THEN 'ON' ELSE 'OFF' END),
  (7,  'ANSI_NULL_DEFAULT '+CASE WHEN d.is_ansi_null_default_on=1 THEN 'ON' ELSE 'OFF' END),
  (8,  'ANSI_NULLS '+CASE WHEN d.is_ansi_nulls_on=1 THEN 'ON' ELSE 'OFF' END),
  (9,  'ANSI_PADDING '+CASE WHEN d.is_ansi_padding_on=1 THEN 'ON' ELSE 'OFF' END),
  (10, 'ANSI_WARNINGS '+CASE WHEN d.is_ansi_warnings_on=1 THEN 'ON' ELSE 'OFF' END),
  (11, 'ARITHABORT '+CASE WHEN d.is_arithabort_on=1 THEN 'ON' ELSE 'OFF' END),
  (12, 'CONCAT_NULL_YIELDS_NULL '+CASE WHEN d.is_concat_null_yields_null_on=1 THEN 'ON' ELSE 'OFF' END),
  (13, 'NUMERIC_ROUNDABORT '+CASE WHEN d.is_numeric_roundabort_on=1 THEN 'ON' ELSE 'OFF' END),
  (14, 'QUOTED_IDENTIFIER '+CASE WHEN d.is_quoted_identifier_on=1 THEN 'ON' ELSE 'OFF' END),
  (15, 'RECURSIVE_TRIGGERS '+CASE WHEN d.is_recursive_triggers_on=1 THEN 'ON' ELSE 'OFF' END),
  (16, 'CURSOR_CLOSE_ON_COMMIT '+CASE WHEN d.is_cursor_close_on_commit_on=1 THEN 'ON' ELSE 'OFF' END),
  (17, 'CURSOR_DEFAULT '+CASE WHEN d.is_local_cursor_default=1 THEN 'LOCAL' ELSE 'GLOBAL' END),
  (18, 'DATE_CORRELATION_OPTIMIZATION '+CASE WHEN d.is_date_correlation_on=1 THEN 'ON' ELSE 'OFF' END),
  (19, 'PARAMETERIZATION '+CASE WHEN d.is_parameterization_forced=1 THEN 'FORCED' ELSE 'SIMPLE' END),
  (20, 'PAGE_VERIFY '+REPLACE(d.page_verify_option_desc,' ','_')),
  (21, 'DB_CHAINING '+CASE WHEN d.is_db_chaining_on=1 THEN 'ON' ELSE 'OFF' END),
  (22, 'ALLOW_SNAPSHOT_ISOLATION '+CASE WHEN d.snapshot_isolation_state=1 THEN 'ON' ELSE 'OFF' END),
  (23, 'READ_COMMITTED_SNAPSHOT '+CASE WHEN d.is_read_committed_snapshot_on=1 THEN 'ON' ELSE 'OFF' END),
  (24, 'COMPATIBILITY_LEVEL = '+CAST(d.compatibility_level AS varchar(5)))
) v(n, s)
WHERE d.database_id = DB_ID();

INSERT #ddl(phase, ord, txt)
SELECT 90, 30, 'ALTER DATABASE CURRENT SET TRUSTWORTHY ON;' FROM sys.databases d WHERE d.database_id=DB_ID() AND d.is_trustworthy_on=1;
'@

    if ($v -ge 12) {
        Add-Section 'database options (2014+)' @'
INSERT #ddl(phase, ord, txt)
SELECT 90, v.n, 'ALTER DATABASE CURRENT SET '+v.s+';'
FROM sys.databases d
CROSS APPLY (VALUES
  (31, CASE WHEN d.is_auto_create_stats_on=1 AND d.is_auto_create_stats_incremental_on=1 THEN 'AUTO_CREATE_STATISTICS ON (INCREMENTAL = ON)' END),
  (32, 'TARGET_RECOVERY_TIME = '+CAST(d.target_recovery_time_in_seconds AS varchar(10))+' SECONDS'),
  (33, 'DELAYED_DURABILITY = '+d.delayed_durability_desc),
  (34, 'MEMORY_OPTIMIZED_ELEVATE_TO_SNAPSHOT = '+CASE WHEN d.is_memory_optimized_elevate_to_snapshot_on=1 THEN 'ON' ELSE 'OFF' END)
) v(n, s)
WHERE d.database_id = DB_ID() AND v.s IS NOT NULL;
'@
    }
    if ($v -ge 14) {
        Add-Section 'database options (2017+)' @'
INSERT #ddl(phase, ord, txt)
SELECT 90, 35, 'ALTER DATABASE CURRENT SET TEMPORAL_HISTORY_RETENTION '+CASE WHEN d.is_temporal_history_retention_enabled=1 THEN 'ON' ELSE 'OFF' END+';'
FROM sys.databases d WHERE d.database_id = DB_ID();
'@
    }
    if ($v -ge 15) {
        Add-Section 'database options (2019+)' @'
INSERT #ddl(phase, ord, txt)
SELECT 90, 36, 'ALTER DATABASE CURRENT SET ACCELERATED_DATABASE_RECOVERY = '+CASE WHEN d.is_accelerated_database_recovery_on=1 THEN 'ON' ELSE 'OFF' END+';'
FROM sys.databases d WHERE d.database_id = DB_ID();
'@
    }

    if ($v -ge 13) {
        Add-Section 'database scoped configuration, credentials, Query Store' (@"
INSERT #ddl(phase, ord, txt)
SELECT 91, 0, 'ALTER DATABASE SCOPED CONFIGURATION SET '+(c.name COLLATE DATABASE_DEFAULT)+' = '
  +CASE WHEN c.name IN ('MAXDOP','PAUSED_RESUMABLE_INDEX_ABORT_DURATION_MINUTES') THEN CONVERT(varchar(100),c.value)
        WHEN CONVERT(varchar(100),c.value) IN ('0','1') THEN CASE WHEN CONVERT(varchar(100),c.value)='1' THEN 'ON' ELSE 'OFF' END
        ELSE CONVERT(varchar(100),c.value) END+';'
FROM sys.database_scoped_configurations c
WHERE c.value IS NOT NULL$(if ($v -ge 15) { ' AND c.is_value_default=0' });

INSERT #ddl(phase, ord, txt)
SELECT 91, 1, '-- !! MANUAL: database scoped credential '+QUOTENAME(dc.name)+' - the secret cannot be read. Create it by hand: CREATE DATABASE SCOPED CREDENTIAL '+QUOTENAME(dc.name)+' WITH IDENTITY = N'''+REPLACE(ISNULL(dc.credential_identity,N''),'''','''''')+''', SECRET = N''<set>'';'
FROM sys.database_scoped_credentials dc;

INSERT #ddl(phase, ord, txt)
SELECT 91, 2, 'ALTER DATABASE CURRENT SET QUERY_STORE = ON (OPERATION_MODE = '+CASE WHEN q.desired_state_desc='READ_ONLY' THEN 'READ_ONLY' ELSE 'READ_WRITE' END
  +', CLEANUP_POLICY = (STALE_QUERY_THRESHOLD_DAYS = '+CAST(q.stale_query_threshold_days AS varchar(10))+')'
  +', DATA_FLUSH_INTERVAL_SECONDS = '+CAST(q.flush_interval_seconds AS varchar(10))
  +', MAX_STORAGE_SIZE_MB = '+CAST(q.max_storage_size_mb AS varchar(10))
  +', INTERVAL_LENGTH_MINUTES = '+CAST(q.interval_length_minutes AS varchar(10))
  +', SIZE_BASED_CLEANUP_MODE = '+q.size_based_cleanup_mode_desc
  +', QUERY_CAPTURE_MODE = '+CASE WHEN q.query_capture_mode_desc IN ('ALL','NONE') THEN q.query_capture_mode_desc ELSE 'AUTO' END
  +', MAX_PLANS_PER_QUERY = '+CAST(q.max_plans_per_query AS varchar(10))
$(if ($v -ge 14) { "  +', WAIT_STATS_CAPTURE_MODE = '+q.wait_stats_capture_mode_desc" })
  +');'
FROM sys.database_query_store_options q WHERE q.desired_state_desc <> 'OFF';
"@)
    }

    # ---- 94-96: objects that must come last (they would fire / interfere during replay) ------------------
    Add-Section 'database DDL triggers' @"
INSERT #ddl(phase, ord, txt)
SELECT 95, 0, $modPre + sm.definition
     + CASE WHEN tr.is_disabled=1 THEN @nl+'GO'+@nl+'DISABLE TRIGGER '+QUOTENAME(tr.name)+' ON DATABASE;' ELSE '' END
     + $modPost
FROM sys.triggers tr
JOIN sys.sql_modules sm ON sm.object_id = tr.object_id
WHERE tr.parent_class = 0 AND tr.is_ms_shipped = 0 AND sm.definition IS NOT NULL;
"@

    Add-Section 'event notifications and audit specifications' @'
INSERT #ddl(phase, ord, txt)
SELECT 1, 3, '-- !! NOT SCRIPTED (event notification): '+QUOTENAME(en.name)+' - recreate it by hand (CREATE EVENT NOTIFICATION ... TO SERVICE ...)'
FROM sys.event_notifications en;

INSERT #ddl(phase, ord, txt)
SELECT 96, 0, 'CREATE DATABASE AUDIT SPECIFICATION '+QUOTENAME(das.name)+' FOR SERVER AUDIT '+QUOTENAME(sa.name)
  +ISNULL(' '+STUFF((SELECT ', ADD ('+CASE WHEN dd.is_group=1 THEN dd.audit_action_name
          ELSE dd.audit_action_name+' ON '+CASE dd.class WHEN 0 THEN 'DATABASE::'+QUOTENAME(DB_NAME())
                 WHEN 1 THEN 'OBJECT::'+QUOTENAME(OBJECT_SCHEMA_NAME(dd.major_id))+'.'+QUOTENAME(OBJECT_NAME(dd.major_id))
                 WHEN 3 THEN 'SCHEMA::'+QUOTENAME(SCHEMA_NAME(dd.major_id)) END
             +' BY '+QUOTENAME(USER_NAME(dd.audited_principal_id)) END+')'
        FROM sys.database_audit_specification_details dd WHERE dd.database_specification_id=das.database_specification_id
        FOR XML PATH(''),TYPE).value('.','nvarchar(max)'),1,2,''),'')
  +' WITH (STATE = '+CASE WHEN das.is_state_enabled=1 THEN 'ON' ELSE 'OFF' END+');'
FROM sys.database_audit_specifications das
JOIN sys.server_audits sa ON sa.audit_guid=das.audit_guid;
'@

    # ---- 1: informational warnings ------------------------------------------------------------------------
    $xmlCompWarn = ''
    if ($v -ge 16) {
        $xmlCompWarn = "`r`nINSERT #ddl(phase, ord, txt)`r`nSELECT TOP (1) 1, 2, '-- !! NOT SCRIPTED: XML_COMPRESSION is used by at least one partition; it is not scripted'`r`nFROM sys.partitions WHERE xml_compression = 1;`r`n"
    }
    $ledgerDbRow = ''
    if ($v -ge 16) {
        $ledgerDbRow = ",`r`n  (CASE WHEN d.is_ledger_on=1 THEN 'database is a ledger database (CREATE DATABASE ... WITH LEDGER = ON); create the target the same way, tables are scripted as ledger tables' END)"
        $xmlCompWarn += "`r`nINSERT #ddl(phase, ord, txt)`r`nSELECT TOP (1) 1, 2, '-- !! NOT SCRIPTED: automatic ledger digest storage is configured; it is not scripted'`r`nFROM sys.database_ledger_digest_locations;`r`n"
    }
    Add-Section 'warnings: features that are never scripted' (@"
INSERT #ddl(phase, ord, txt)
SELECT 1, 2, '-- !! NOT SCRIPTED: '+w.msg
FROM sys.databases d
CROSS APPLY (VALUES
  (CASE WHEN d.is_cdc_enabled=1 THEN 'Change Data Capture is enabled in the source; capture instances are not scripted' END),
  (CASE WHEN d.is_published=1 OR d.is_merge_published=1 THEN 'database is a replication publisher; publications/articles are not scripted' END),
  (CASE WHEN d.is_subscribed=1 THEN 'database is a replication subscriber; subscriptions are not scripted' END),
  (CASE WHEN d.is_encrypted=1 THEN 'Transparent Data Encryption is on; TDE is not scripted' END),
  (CASE WHEN d.is_read_only=1 THEN 'database is READ_ONLY in the source; not applied by this script' END)$ledgerDbRow
) w(msg)
WHERE d.database_id = DB_ID() AND w.msg IS NOT NULL;
$xmlCompWarn
"@)

    # ---- a statement that concatenated to NULL would otherwise vanish from the output without a trace ---------
    Add-Section 'dropped (NULL) statements' @'
INSERT #ddl(phase, ord, txt)
SELECT 1, 9, '-- !! INTERNAL: '+CAST(COUNT(*) AS varchar(12))+' statement(s) in phase(s) '
   +STUFF((SELECT DISTINCT ','+CAST(x.phase AS varchar(5)) FROM #ddl x WHERE x.txt IS NULL FOR XML PATH('')),1,1,'')
   +' evaluated to NULL and were dropped (an object is missing from this script)'
FROM #ddl WHERE txt IS NULL HAVING COUNT(*) > 0;
'@

    # ---- 99: footer ---------------------------------------------------------------------------------------
    Add-Section 'footer' @'
INSERT #ddl(phase, ord, txt)
SELECT 99, 0, '-- End of generated script. Items needing attention: '+CAST(ISNULL(SUM(a.k),0) AS varchar(12))
   +'; expected (not scripted by design, or a manual step): '+CAST(ISNULL(SUM(1 - a.k),0) AS varchar(12))+' (search for  -- !!)'
FROM (SELECT CASE WHEN txt LIKE '-- !! SECTION FAILED%' OR txt LIKE '-- !! INTERNAL%' OR txt LIKE '-- !! ORDER CONFLICT%'
                    OR txt LIKE '%could not be read%' OR txt LIKE '%not visible to this login%' OR txt LIKE '%may be truncated%' THEN 1 ELSE 0 END AS k
      FROM #ddl WHERE txt LIKE '-- !!%') a;
'@

    if ($EmitSections) { return $sections }

    # =================================================================== ASSEMBLE
    $sb = New-Object System.Text.StringBuilder
    [void]$sb.AppendLine(@'
SET NOCOUNT ON;
DECLARE @nl nvarchar(2) = NCHAR(13)+NCHAR(10);
DECLARE @dbcoll sysname = CONVERT(sysname, DATABASEPROPERTYEX(DB_NAME(),'Collation'));
DECLARE @q nvarchar(max);
IF OBJECT_ID('tempdb..#ddl') IS NOT NULL DROP TABLE #ddl;
CREATE TABLE #ddl (id int IDENTITY(1,1) PRIMARY KEY, phase int NOT NULL, ord int NOT NULL DEFAULT 0, txt nvarchar(max) COLLATE DATABASE_DEFAULT);
IF OBJECT_ID('tempdb..#coltype') IS NOT NULL DROP TABLE #coltype;
CREATE TABLE #coltype (object_id int NOT NULL, column_id int NOT NULL, typestr nvarchar(400) COLLATE DATABASE_DEFAULT, PRIMARY KEY (object_id, column_id));
IF OBJECT_ID('tempdb..#partype') IS NOT NULL DROP TABLE #partype;
CREATE TABLE #partype (object_id int NOT NULL, parameter_id int NOT NULL, typestr nvarchar(400) COLLATE DATABASE_DEFAULT, PRIMARY KEY (object_id, parameter_id));
IF OBJECT_ID('tempdb..#tbl') IS NOT NULL DROP TABLE #tbl;
CREATE TABLE #tbl (object_id int NOT NULL PRIMARY KEY, kind varchar(40) COLLATE DATABASE_DEFAULT NOT NULL);
IF OBJECT_ID('tempdb..#cmp') IS NOT NULL DROP TABLE #cmp;
CREATE TABLE #cmp (object_id int NOT NULL, index_id int NOT NULL, clause nvarchar(2000) COLLATE DATABASE_DEFAULT NULL, PRIMARY KEY (object_id, index_id));
IF OBJECT_ID('tempdb..#mod') IS NOT NULL DROP TABLE #mod;
CREATE TABLE #mod (object_id int NOT NULL PRIMARY KEY, type char(2) NOT NULL, rnk int NOT NULL, early bit NOT NULL);
IF OBJECT_ID('tempdb..#edge') IS NOT NULL DROP TABLE #edge;
CREATE TABLE #edge (src_id int NOT NULL, dst_id int NOT NULL);

/* ---------- session options the generated script needs (phase 0) ---------- */
INSERT #ddl(phase, ord, txt) VALUES (0, 0, 'SET ANSI_NULLS ON;'+@nl+'SET QUOTED_IDENTIFIER ON;'+@nl+'SET ANSI_PADDING ON;');
'@)

    foreach ($s in $sections) {
        $esc = $s.Sql.Replace("'", "''")
        [void]$sb.AppendLine("/* ---------- $($s.Name) ---------- */")
        [void]$sb.AppendLine('BEGIN TRY')
        [void]$sb.AppendLine("SET @q = N'$esc';")
        [void]$sb.AppendLine('EXEC sys.sp_executesql @q, N''@nl nvarchar(2), @dbcoll sysname'', @nl, @dbcoll;')
        [void]$sb.AppendLine('END TRY')
        [void]$sb.AppendLine('BEGIN CATCH')
        [void]$sb.AppendLine("INSERT #ddl(phase, ord, txt) VALUES (1, 9, N'-- !! SECTION FAILED [$($s.Name)]: ' + REPLACE(REPLACE(ISNULL(ERROR_MESSAGE(), N''), CHAR(13), N' '), CHAR(10), N' '));")
        [void]$sb.AppendLine('END CATCH;')
        [void]$sb.AppendLine('')
    }

    # ---- output --------------------------------------------------------------------------------------------
    [void]$sb.AppendLine(@'
/* ---------- output: every row ends with its own GO ---------- */
IF OBJECT_ID('tempdb..#out') IS NOT NULL DROP TABLE #out;
SELECT id, phase, ord, txt + @nl + N'GO' AS t INTO #out FROM #ddl WHERE txt IS NOT NULL;
'@)
    if ($OutputMode -eq 'Lines') {
        [void]$sb.AppendLine(@'
/* One PHYSICAL LINE per row: SSMS text output and sqlcmd cap characters per column.
   In SSMS also set Query Options > Results > Text > Maximum number of characters displayed in each column = 8192. */
;WITH s AS (
    SELECT phase, ord, id, CAST(1 AS int) AS ln, CAST(1 AS int) AS p, CAST(CHARINDEX(@nl, t, 1) AS int) AS e, t FROM #out
    UNION ALL
    SELECT phase, ord, id, ln + 1, e + 2, CAST(CHARINDEX(@nl, t, e + 2) AS int), t FROM s WHERE e > 0
)
SELECT SUBSTRING(t, p, CASE WHEN e = 0 THEN DATALENGTH(t)/2 + 1 - p ELSE e - p END) AS [ ]
FROM s ORDER BY phase, ord, id, ln
OPTION (MAXRECURSION 0);
'@)
    } else {
        [void]$sb.AppendLine('SELECT t AS [ddl] FROM #out ORDER BY phase, ord, id;')
    }
    return $sb.ToString()
}
#endregion DDL-EXTRACT

#region AGENTJOB-EXTRACT  (copy of mssql-schema-extract\Get-AgentJobScript.ps1, renamed ExtractAgentJobScript - edit it there, then run tools\Sync-ToMva.ps1)
<#
.SYNOPSIS
    Generates the T-SQL that scripts every SQL Server Agent job on an instance (msdb), for a given SQL Server version.

.DESCRIPTION
    Companion of Get-DdlExtractScript (which is database-level; Agent jobs are instance-level and live in msdb).
    Returns ONE T-SQL string. Run it against any database of the instance (it switches to msdb itself); the result
    set is the script, one row per batch (OutputMode Batches, every row ends with GO) or one physical line per row
    (OutputMode Lines).

    Scripted, in dependency order:
        job categories (custom)  ->  operators referenced by jobs  ->  proxies referenced by job steps (+ subsystem
        and login grants)  ->  schedules shared by several jobs  ->  jobs (steps, schedules, notifications, target)

    Each job is its own batch: guarded with IF EXISTS (an existing job is skipped, never overwritten), wrapped in a
    transaction with TRY/CATCH (a job is created completely or not at all) and the owner falls back to the executing
    login when the original owner login does not exist on the target.

    Not scripted - listed as  -- !! NOT SCRIPTED / -- !! MANUAL  lines at the top of the output:
        replication agent jobs, Change Data Capture jobs, Data Collector jobs and multi-server (MSX) jobs;
        credential secrets behind proxies; SSIS packages run by job steps; Agent alerts, fail-safe operator and the
        Agent properties.
    Job step commands are scripted verbatim - review the file before sharing it (commands can contain secrets).

.PARAMETER SqlVersion
    Anything whose first dotted component is the major version: '15.0.4153.1', '15', 13, '10.50.6000'.

.PARAMETER OutputMode
    Batches (default) - one row per batch; each row already ends with its own GO line (Invoke-Sql / SqlClient).
    Lines             - one PHYSICAL LINE per row (SSMS "Results to Text/File", sqlcmd).

.PARAMETER JobNameLike
    Optional T-SQL LIKE pattern. Only matching jobs (and the operators/proxies/categories/schedules they use) are scripted.

.PARAMETER FromProcedures
    Read steps, schedules, categories, operators, proxies, targets and alerts through the supported msdb.dbo.sp_help_*
    procedures instead of the msdb catalog tables. Use it on Amazon RDS for SQL Server, where only msdb.dbo.sysjobs can be
    selected from (the tables are denied, even to the master user) but the procedures are available. The generated script is
    the same as in table mode, with these differences: proxies that no job step uses are not scripted, and a step command
    of exactly 3200 characters is flagged (-- !! NOT SCRIPTED ...) in case an older build truncates there (SQL Server 2022 returns the full command).

.PARAMETER EmitSections
    Test hook: returns the raw section list (Name, Sql) instead of the assembled script.

.NOTES
    Canonical source lives in the mssql-schema-extract repo. MVA-Data-Collection.ps1 carries a copy between the
    "#region AGENTJOB-EXTRACT" markers; refresh it with tools\Sync-ToMva.ps1.
#>
function ExtractAgentJobScript {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)] $SqlVersion,
        [ValidateSet('Batches', 'Lines')] [string] $OutputMode = 'Batches',
        [string] $JobNameLike,
        [switch] $FromProcedures,
        [switch] $EmitSections
    )

    # ---------------------------------------------------------------- version
    $majorText = ([string]$SqlVersion).Trim().Split('.')[0]
    $v = 0
    if (-not [int]::TryParse($majorText, [ref]$v)) { throw "ExtractAgentJobScript: cannot read a major version from '$SqlVersion'" }
    if ($v -lt 10) { throw "ExtractAgentJobScript: SQL Server major version $v is not supported (minimum is 10 = SQL Server 2008)." }

    $sections = New-Object System.Collections.Generic.List[object]
    # $Guard (optional T-SQL predicate): the section only runs when it is true, so a section that cannot work on this
    # instance (e.g. proxies, which need sys.credentials) is skipped quietly when there is nothing for it to do.
    function Add-Section([string]$Name, [string]$Sql, [string]$Guard = '') { $sections.Add([pscustomobject]@{ Name = $Name; Sql = $Sql; Guard = $Guard }) }

    # T-SQL expression rendering a nullable string column as an N'...' literal (or the keyword NULL).
    function NL([string]$x) { return "CASE WHEN $x IS NULL THEN 'NULL' ELSE 'N'''+REPLACE($x,'''','''''')+'''' END" }
    # NOT NULL variant
    function NN([string]$x) { return "'N'''+REPLACE($x,'''','''''')+''''" }

    $filter = ''
    if (-not [string]::IsNullOrEmpty($JobNameLike)) {
        $filter = "AND j.name LIKE N'" + $JobNameLike.Replace("'", "''") + "'"
    }
    # every dependency selects from #job (jobs in scope); with no filter, operators/categories/proxies are scripted even when unused
    $all = [string]::IsNullOrEmpty($JobNameLike)

    # Where each section READS Agent metadata from. Normally the msdb catalog tables. With -FromProcedures (Amazon RDS for
    # SQL Server, where only msdb.dbo.sysjobs is readable) they are temp tables filled up front from the supported
    # msdb.dbo.sp_help_* procedures instead. Text the generated script EMITS (for the target to run) never uses these.
    if ($FromProcedures) {
        $tSteps = '#mva_jobsteps'; $tCategories = '#mva_categories'; $tOperators = '#mva_operators'
        $tProxies = '#mva_proxies'; $tProxyLogin = '#mva_proxylogin'; $tProxySubsys = '#mva_proxysubsys'
        $tSchedules = '#mva_schedules'; $tJobSchedules = '#mva_jobschedules'; $tJobServers = '#mva_jobservers'; $tAlerts = '#mva_alerts'
    } else {
        $tSteps = 'dbo.sysjobsteps'; $tCategories = 'dbo.syscategories'; $tOperators = 'dbo.sysoperators'
        $tProxies = 'dbo.sysproxies'; $tProxyLogin = 'dbo.sysproxylogin'; $tProxySubsys = 'dbo.sysproxysubsystem'
        $tSchedules = 'dbo.sysschedules'; $tJobSchedules = 'dbo.sysjobschedules'; $tJobServers = 'dbo.sysjobservers'; $tAlerts = 'dbo.sysalerts'
    }

    # jobs whose steps/schedules could not be read through the procedures (see 'read Agent objects through procedures') are not in scope
    $failedJobs = if ($FromProcedures) { 'AND j.job_id NOT IN (SELECT job_id FROM #mva_jobfail)' } else { '' }

    # =================================================================== SECTIONS
    if ($FromProcedures) {
        # Fills the #mva_* temp tables (created by the outer batch - temp tables made inside sp_executesql would vanish with it).
        # One sp_help_* call per job for steps and schedules; the rest are instance-wide.
        Add-Section 'read Agent objects through procedures' @"
DECLARE @jid uniqueidentifier;
DECLARE jc CURSOR LOCAL FAST_FORWARD FOR SELECT j.job_id FROM dbo.sysjobs j WHERE j.originating_server_id = 0 $filter;
OPEN jc;
FETCH NEXT FROM jc INTO @jid;
WHILE @@FETCH_STATUS = 0
BEGIN
    -- one job the login cannot read must not stop the others: a job whose steps/schedules could not be read is left out of the script
    -- entirely (it would otherwise be created without steps) and reported as NOT SCRIPTED with the reason
    BEGIN TRY
        DELETE #mva_hjstep;  INSERT #mva_hjstep  EXEC msdb.dbo.sp_help_jobstep     @job_id = @jid;
        INSERT #mva_jobsteps(job_id, step_id, step_name, subsystem, command, flags, cmdexec_success_code, on_success_action, on_success_step_id,
                             on_fail_action, on_fail_step_id, database_name, database_user_name, retry_attempts, retry_interval, os_run_priority, output_file_name, proxy_id)
        SELECT @jid, step_id, step_name, subsystem, command, flags, cmdexec_success_code, on_success_action, on_success_step_id,
               on_fail_action, on_fail_step_id, database_name, database_user_name, retry_attempts, retry_interval, os_run_priority, output_file_name, proxy_id
        FROM #mva_hjstep;
        DELETE #mva_hjsched; INSERT #mva_hjsched EXEC msdb.dbo.sp_help_jobschedule @job_id = @jid;
        INSERT #mva_jobschedules(job_id, schedule_id) SELECT @jid, schedule_id FROM #mva_hjsched;
        DELETE #mva_hjserv;  INSERT #mva_hjserv  EXEC msdb.dbo.sp_help_jobserver   @job_id = @jid;
        INSERT #mva_jobservers(job_id, server_id) SELECT @jid, server_id FROM #mva_hjserv;
    END TRY
    BEGIN CATCH
        DELETE #mva_jobsteps WHERE job_id = @jid;
        DELETE #mva_jobschedules WHERE job_id = @jid;
        DELETE #mva_jobservers WHERE job_id = @jid;
        INSERT #mva_jobfail(job_id, msg, num) VALUES (@jid, ERROR_MESSAGE(), ERROR_NUMBER());
    END CATCH;
    FETCH NEXT FROM jc INTO @jid;
END
CLOSE jc; DEALLOCATE jc;

-- instance-wide lists: each is its own step, so a procedure this login may not run is named in the failure and the others still load
BEGIN TRY INSERT #mva_categories(category_id, category_type, name) EXEC msdb.dbo.sp_help_category @class = N'JOB'; END TRY
BEGIN CATCH INSERT #ddl(phase, ord, txt) VALUES (1, 9, N'-- !! SECTION FAILED [sp_help_category]: ' + REPLACE(REPLACE(ISNULL(ERROR_MESSAGE(), N''), CHAR(13), N' '), CHAR(10), N' ')); END CATCH;
BEGIN TRY INSERT #mva_operators EXEC msdb.dbo.sp_help_operator; END TRY
BEGIN CATCH INSERT #ddl(phase, ord, txt) VALUES (1, 9, N'-- !! SECTION FAILED [sp_help_operator]: ' + REPLACE(REPLACE(ISNULL(ERROR_MESSAGE(), N''), CHAR(13), N' '), CHAR(10), N' ')); END CATCH;
BEGIN TRY INSERT #mva_schedules EXEC msdb.dbo.sp_help_schedule; END TRY
BEGIN CATCH INSERT #ddl(phase, ord, txt) VALUES (1, 9, N'-- !! SECTION FAILED [sp_help_schedule]: ' + REPLACE(REPLACE(ISNULL(ERROR_MESSAGE(), N''), CHAR(13), N' '), CHAR(10), N' ')); END CATCH;
BEGIN TRY INSERT #mva_alerts EXEC msdb.dbo.sp_help_alert; END TRY
-- alerts are never scripted (only counted, to warn that they exist), so an instance that does not let this login list them (Amazon RDS) is expected, not an error
BEGIN CATCH INSERT #ddl(phase, ord, txt) VALUES (1, 13, N'-- !! NOT SCRIPTED: SQL Server Agent alerts were not checked - sp_help_alert is not available to this login; Agent alerts are never scripted'); END CATCH;

-- proxies need sys.credentials, so they are only read when a step actually uses one (unused proxies are not scripted in this mode)
IF EXISTS (SELECT 1 FROM #mva_jobsteps WHERE proxy_id IS NOT NULL)
BEGIN
  BEGIN TRY
    INSERT #mva_proxies EXEC msdb.dbo.sp_help_proxy;
    DECLARE @pn sysname;
    DECLARE pc CURSOR LOCAL FAST_FORWARD FOR SELECT name FROM #mva_proxies;
    OPEN pc;
    FETCH NEXT FROM pc INTO @pn;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        DELETE #mva_hplogin; INSERT #mva_hplogin EXEC msdb.dbo.sp_enum_login_for_proxy     @proxy_name = @pn;
        INSERT #mva_proxylogin(proxy_id, sid) SELECT proxy_id, sid FROM #mva_hplogin;
        DELETE #mva_hpsub;   INSERT #mva_hpsub   EXEC msdb.dbo.sp_enum_proxy_for_subsystem @proxy_name = @pn;
        INSERT #mva_proxysubsys(proxy_id, subsystem_id) SELECT proxy_id, subsystem_id FROM #mva_hpsub;
        FETCH NEXT FROM pc INTO @pn;
    END
    CLOSE pc; DEALLOCATE pc;
  END TRY
  BEGIN CATCH
    INSERT #ddl(phase, ord, txt) VALUES (1, 9, N'-- !! SECTION FAILED [proxies through procedures]: ' + REPLACE(REPLACE(ISNULL(ERROR_MESSAGE(), N''), CHAR(13), N' '), CHAR(10), N' '));
  END CATCH;
END

INSERT #ddl(phase, ord, txt)
SELECT 1, 15, CASE WHEN f.num = 14262
       -- 14262 = "The specified @job_id does not exist": the job is in sysjobs but the Agent procedures hide it from this login (it belongs to another
       -- login, e.g. the Microsoft system job syspolicy_purge_history on Amazon RDS). An expected limit of this mode, not a fault.
       THEN '-- !! NOT SCRIPTED: job '+REPLACE(REPLACE(j.name,CHAR(13),' '),CHAR(10),' ')+' - hidden by the Agent procedures from this login (it is owned by another login); expected on Amazon RDS'
       ELSE '-- !! NOT SCRIPTED: job '+REPLACE(REPLACE(j.name,CHAR(13),' '),CHAR(10),' ')+' - its steps and schedules could not be read through sp_help_jobstep / sp_help_jobschedule / sp_help_jobserver as this login: '
            +REPLACE(REPLACE(f.msg,CHAR(13),' '),CHAR(10),' ') END
FROM #mva_jobfail f JOIN dbo.sysjobs j ON j.job_id = f.job_id;

-- the documented sp_help_jobstep result column is 3200 characters; SQL Server 2022 returns the full command (checked up to 10930 characters),
-- but flag a command of exactly 3200 in case an older build cuts it there
INSERT #ddl(phase, ord, txt)
SELECT 1, 14, '-- !! NOT SCRIPTED (step command may be truncated - exactly 3200 characters, the documented sp_help_jobstep limit): job '+j.name+' step '+CAST(s.step_id AS varchar(5))
FROM #mva_jobsteps s JOIN dbo.sysjobs j ON j.job_id = s.job_id WHERE LEN(s.command) = 3200;
"@
    }

    Add-Section 'prep: jobs in scope' @"
INSERT #job(job_id)
SELECT j.job_id FROM dbo.sysjobs j
LEFT JOIN $tCategories c ON c.category_id = j.category_id
WHERE j.originating_server_id = 0
  AND ISNULL(c.name,'') NOT LIKE 'REPL-%' AND ISNULL(c.name,'') <> 'Data Collector'
  AND NOT EXISTS (SELECT 1 FROM $tSteps s WHERE s.job_id = j.job_id AND s.command LIKE '%sp[_]MScdc[_]%')
  $failedJobs
  $filter;

INSERT #ddl(phase, ord, txt)
SELECT 1, 10, '-- !! NOT SCRIPTED ('+CASE WHEN j.originating_server_id <> 0 THEN 'multi-server job from a master server'
                                          WHEN c.name LIKE 'REPL-%' THEN 'replication agent job'
                                          WHEN c.name = 'Data Collector' THEN 'Data Collector job'
                                          ELSE 'Change Data Capture job' END+'): '+REPLACE(REPLACE(j.name,CHAR(13),' '),CHAR(10),' ')
FROM dbo.sysjobs j
LEFT JOIN $tCategories c ON c.category_id = j.category_id
WHERE j.job_id NOT IN (SELECT job_id FROM #job) $failedJobs $filter;
"@

    Add-Section 'categories' @"
INSERT #ddl(phase, ord, txt)
SELECT 5, 0, 'IF NOT EXISTS (SELECT 1 FROM dbo.syscategories WHERE name = '+$(NN 'c.name')+' AND category_class = 1)'+@nl
   +'    EXEC dbo.sp_add_category @class = N''JOB'', @type = N'''+CASE c.category_type WHEN 2 THEN 'MULTI-SERVER' WHEN 3 THEN 'NONE' ELSE 'LOCAL' END+''', @name = '+$(NN 'c.name')+';'
FROM $tCategories c
WHERE c.category_class = 1 AND c.category_id >= 100
  $(if ($all) { '' } else { 'AND EXISTS (SELECT 1 FROM dbo.sysjobs j JOIN #job x ON x.job_id = j.job_id WHERE j.category_id = c.category_id)' });
"@

    Add-Section 'operators' @"
INSERT #ddl(phase, ord, txt)
SELECT 6, 0, 'IF NOT EXISTS (SELECT 1 FROM dbo.sysoperators WHERE name = '+$(NN 'o.name')+')'+@nl
   +'    EXEC dbo.sp_add_operator @name = '+$(NN 'o.name')+', @enabled = '+CAST(o.enabled AS varchar(3))
   +', @weekday_pager_start_time = '+CAST(o.weekday_pager_start_time AS varchar(10))+', @weekday_pager_end_time = '+CAST(o.weekday_pager_end_time AS varchar(10))
   +', @saturday_pager_start_time = '+CAST(o.saturday_pager_start_time AS varchar(10))+', @saturday_pager_end_time = '+CAST(o.saturday_pager_end_time AS varchar(10))
   +', @sunday_pager_start_time = '+CAST(o.sunday_pager_start_time AS varchar(10))+', @sunday_pager_end_time = '+CAST(o.sunday_pager_end_time AS varchar(10))
   +', @pager_days = '+CAST(o.pager_days AS varchar(5))
   +CASE WHEN o.email_address IS NOT NULL THEN ', @email_address = '+$(NN 'o.email_address') ELSE '' END
   +CASE WHEN o.pager_address IS NOT NULL THEN ', @pager_address = '+$(NN 'o.pager_address') ELSE '' END
   +CASE WHEN o.netsend_address IS NOT NULL THEN ', @netsend_address = '+$(NN 'o.netsend_address') ELSE '' END+';'
FROM $tOperators o
$(if ($all) { '' } else { 'WHERE EXISTS (SELECT 1 FROM dbo.sysjobs j JOIN #job x ON x.job_id = j.job_id WHERE o.id IN (j.notify_email_operator_id, j.notify_page_operator_id, j.notify_netsend_operator_id))' });
"@

    Add-Section 'proxies' @"
INSERT #ddl(phase, ord, txt)
SELECT 7, 0, 'IF NOT EXISTS (SELECT 1 FROM dbo.sysproxies WHERE name = '+$(NN 'p.name')+')'+@nl
   +'BEGIN'+@nl
   +'    IF NOT EXISTS (SELECT 1 FROM sys.credentials WHERE name = '+$(NN 'cr.name COLLATE DATABASE_DEFAULT')+')'+@nl
   +'        RAISERROR(N''Proxy '+REPLACE(REPLACE(p.name,'%','%%'),'''','''''')+' was not created: credential '+REPLACE(REPLACE(cr.name COLLATE DATABASE_DEFAULT,'%','%%'),'''','''''')+' does not exist on this instance - create it first and rerun'', 16, 1);'+@nl
   +'    ELSE'+@nl
   +'    BEGIN'+@nl
   +'        EXEC dbo.sp_add_proxy @proxy_name = '+$(NN 'p.name')+', @enabled = '+CAST(p.enabled AS varchar(3))+', @description = '+$(NL 'p.description')+', @credential_name = '+$(NN 'cr.name COLLATE DATABASE_DEFAULT')+';'
   +ISNULL((SELECT @nl+'        EXEC dbo.sp_grant_proxy_to_subsystem @proxy_name = '+$(NN 'p.name')+', @subsystem_id = '+CAST(ps.subsystem_id AS varchar(5))+';'
            FROM $tProxySubsys ps WHERE ps.proxy_id = p.proxy_id ORDER BY ps.subsystem_id FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),'')
   +ISNULL((SELECT @nl+'        IF SUSER_SID('+$(NN 'sp.name COLLATE DATABASE_DEFAULT')+') IS NOT NULL EXEC dbo.sp_grant_login_to_proxy @proxy_name = '+$(NN 'p.name')+', @login_name = '+$(NN 'sp.name COLLATE DATABASE_DEFAULT')+';'
            FROM $tProxyLogin pl JOIN sys.server_principals sp ON sp.sid = pl.sid WHERE pl.proxy_id = p.proxy_id ORDER BY sp.name COLLATE DATABASE_DEFAULT FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),'')
   +@nl+'    END'+@nl
   +'END;'
FROM $tProxies p
JOIN sys.credentials cr ON cr.credential_id = p.credential_id
$(if ($all) { '' } else { "WHERE EXISTS (SELECT 1 FROM $tSteps s JOIN #job x ON x.job_id = s.job_id WHERE s.proxy_id = p.proxy_id)" });

INSERT #ddl(phase, ord, txt)
SELECT 1, 11, '-- !! MANUAL: proxy '+p.name+' runs as credential '+(cr.name COLLATE DATABASE_DEFAULT)+' - the credential secret cannot be scripted; create the credential on the target first'
FROM $tProxies p
JOIN sys.credentials cr ON cr.credential_id = p.credential_id
$(if ($all) { '' } else { "WHERE EXISTS (SELECT 1 FROM $tSteps s JOIN #job x ON x.job_id = s.job_id WHERE s.proxy_id = p.proxy_id)" });

-- a login that cannot see sys.credentials (needs ALTER ANY CREDENTIAL) gets no rows from the joins above, not an error: say so instead of dropping the proxy silently
INSERT #ddl(phase, ord, txt)
SELECT 1, 11, '-- !! NOT SCRIPTED: proxy '+p.name+' - its credential is not visible to this login (sys.credentials needs ALTER ANY CREDENTIAL), so the proxy and its grants are missing from this script'
FROM $tProxies p
WHERE NOT EXISTS (SELECT 1 FROM sys.credentials cr WHERE cr.credential_id = p.credential_id)
$(if ($all) { '' } else { "AND EXISTS (SELECT 1 FROM $tSteps s JOIN #job x ON x.job_id = s.job_id WHERE s.proxy_id = p.proxy_id)" });
"@ -Guard $(if ($FromProcedures) { 'EXISTS (SELECT 1 FROM #mva_proxies)' } else { '' })

    # schedules attached to more than one job are created once and attached by uid; single-use schedules are created with the job
    Add-Section 'shared schedules' @"
INSERT #ddl(phase, ord, txt)
SELECT 8, 0, 'IF NOT EXISTS (SELECT 1 FROM dbo.sysschedules WHERE schedule_uid = '''+CONVERT(varchar(36),sc.schedule_uid)+''')'+@nl
   +'    EXEC dbo.sp_add_schedule @schedule_name = '+$(NN 'sc.name')+', @enabled = '+CAST(sc.enabled AS varchar(3))
   +', @freq_type = '+CAST(sc.freq_type AS varchar(10))+', @freq_interval = '+CAST(sc.freq_interval AS varchar(10))
   +', @freq_subday_type = '+CAST(sc.freq_subday_type AS varchar(10))+', @freq_subday_interval = '+CAST(sc.freq_subday_interval AS varchar(10))
   +', @freq_relative_interval = '+CAST(sc.freq_relative_interval AS varchar(10))+', @freq_recurrence_factor = '+CAST(sc.freq_recurrence_factor AS varchar(10))
   +', @active_start_date = '+CAST(sc.active_start_date AS varchar(10))+', @active_end_date = '+CAST(sc.active_end_date AS varchar(10))
   +', @active_start_time = '+CAST(sc.active_start_time AS varchar(10))+', @active_end_time = '+CAST(sc.active_end_time AS varchar(10))
   +', @schedule_uid = '''+CONVERT(varchar(36),sc.schedule_uid)+''';'
FROM $tSchedules sc
WHERE (SELECT COUNT(*) FROM $tJobSchedules a WHERE a.schedule_id = sc.schedule_id) > 1
  AND EXISTS (SELECT 1 FROM $tJobSchedules a JOIN #job x ON x.job_id = a.job_id WHERE a.schedule_id = sc.schedule_id);
"@

    Add-Section 'jobs' @"
INSERT #ddl(phase, ord, txt)
SELECT 10, ROW_NUMBER() OVER (ORDER BY j.name),
   'BEGIN TRY'+@nl
  +'BEGIN TRANSACTION;'+@nl
  +'IF EXISTS (SELECT 1 FROM dbo.sysjobs WHERE name = '+$(NN 'j.name')+')'+@nl
  +'    PRINT N''Job already exists - skipped: '' + '+$(NN 'j.name')+';'+@nl
  +'ELSE'+@nl
  +'BEGIN'+@nl
  +'    DECLARE @jobId uniqueidentifier, @sid int, @owner sysname = '+$(NN "ISNULL(SUSER_SNAME(j.owner_sid),'sa') COLLATE DATABASE_DEFAULT")+';'+@nl
  +'    IF SUSER_SID(@owner) IS NULL SET @owner = SUSER_SNAME();'+@nl
  +'    EXEC dbo.sp_add_job @job_name = '+$(NN 'j.name')+', @enabled = '+CAST(j.enabled AS varchar(3))
  +', @notify_level_eventlog = '+CAST(j.notify_level_eventlog AS varchar(3))+', @notify_level_email = '+CAST(j.notify_level_email AS varchar(3))
  +', @notify_level_netsend = '+CAST(j.notify_level_netsend AS varchar(3))+', @notify_level_page = '+CAST(j.notify_level_page AS varchar(3))
  +', @delete_level = '+CAST(j.delete_level AS varchar(3))
  +', @description = '+$(NL 'j.description')
  +', @category_name = '+$(NL 'cat.name')+', @owner_login_name = @owner'
  +CASE WHEN oe.name IS NOT NULL THEN ', @notify_email_operator_name = '+$(NN 'oe.name') ELSE '' END
  +CASE WHEN op.name IS NOT NULL THEN ', @notify_page_operator_name = '+$(NN 'op.name') ELSE '' END
  +CASE WHEN onn.name IS NOT NULL THEN ', @notify_netsend_operator_name = '+$(NN 'onn.name') ELSE '' END
  +', @job_id = @jobId OUTPUT;'
  +ISNULL((SELECT @nl+'    EXEC dbo.sp_add_jobstep @job_id = @jobId, @step_name = '+$(NN 's.step_name')+', @step_id = '+CAST(s.step_id AS varchar(10))
        +', @cmdexec_success_code = '+CAST(s.cmdexec_success_code AS varchar(10))
        +', @on_success_action = '+CAST(s.on_success_action AS varchar(5))+', @on_success_step_id = '+CAST(s.on_success_step_id AS varchar(10))
        +', @on_fail_action = '+CAST(s.on_fail_action AS varchar(5))+', @on_fail_step_id = '+CAST(s.on_fail_step_id AS varchar(10))
        +', @retry_attempts = '+CAST(s.retry_attempts AS varchar(10))+', @retry_interval = '+CAST(s.retry_interval AS varchar(10))
        +', @os_run_priority = '+CAST(s.os_run_priority AS varchar(5))
        +', @subsystem = '+$(NN 's.subsystem')+', @command = '+$(NL 's.command')
        +CASE WHEN s.database_name IS NOT NULL THEN ', @database_name = '+$(NN 's.database_name') ELSE '' END
        +CASE WHEN s.database_user_name IS NOT NULL THEN ', @database_user_name = '+$(NN 's.database_user_name') ELSE '' END
        +', @flags = '+CAST(s.flags AS varchar(10))
        +CASE WHEN s.output_file_name IS NOT NULL THEN ', @output_file_name = '+$(NN 's.output_file_name') ELSE '' END
        +CASE WHEN px.name IS NOT NULL THEN ', @proxy_name = '+$(NN 'px.name') ELSE '' END+';'
     FROM $tSteps s LEFT JOIN $tProxies px ON px.proxy_id = s.proxy_id
     WHERE s.job_id = j.job_id ORDER BY s.step_id FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),'')
  +CASE WHEN j.start_step_id <> 1 THEN @nl+'    EXEC dbo.sp_update_job @job_id = @jobId, @start_step_id = '+CAST(j.start_step_id AS varchar(10))+';' ELSE '' END
  +ISNULL((SELECT @nl+CASE WHEN (SELECT COUNT(*) FROM $tJobSchedules a WHERE a.schedule_id = sc.schedule_id) > 1
        THEN '    SELECT @sid = schedule_id FROM dbo.sysschedules WHERE schedule_uid = '''+CONVERT(varchar(36),sc.schedule_uid)+''';'+@nl+'    EXEC dbo.sp_attach_schedule @job_id = @jobId, @schedule_id = @sid;'
        ELSE '    EXEC dbo.sp_add_jobschedule @job_id = @jobId, @name = '+$(NN 'sc.name')+', @enabled = '+CAST(sc.enabled AS varchar(3))
        +', @freq_type = '+CAST(sc.freq_type AS varchar(10))+', @freq_interval = '+CAST(sc.freq_interval AS varchar(10))
        +', @freq_subday_type = '+CAST(sc.freq_subday_type AS varchar(10))+', @freq_subday_interval = '+CAST(sc.freq_subday_interval AS varchar(10))
        +', @freq_relative_interval = '+CAST(sc.freq_relative_interval AS varchar(10))+', @freq_recurrence_factor = '+CAST(sc.freq_recurrence_factor AS varchar(10))
        +', @active_start_date = '+CAST(sc.active_start_date AS varchar(10))+', @active_end_date = '+CAST(sc.active_end_date AS varchar(10))
        +', @active_start_time = '+CAST(sc.active_start_time AS varchar(10))+', @active_end_time = '+CAST(sc.active_end_time AS varchar(10))+';' END
     FROM $tJobSchedules js JOIN $tSchedules sc ON sc.schedule_id = js.schedule_id
     WHERE js.job_id = j.job_id ORDER BY sc.name, sc.schedule_id FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),'')
  +CASE WHEN EXISTS (SELECT 1 FROM $tJobServers sv WHERE sv.job_id = j.job_id AND sv.server_id = 0)
        THEN @nl+'    EXEC dbo.sp_add_jobserver @job_id = @jobId, @server_name = N''(local)'';' ELSE '' END
  +@nl+'END'+@nl
  +'COMMIT TRANSACTION;'+@nl
  +'END TRY'+@nl
  +'BEGIN CATCH'+@nl
  +'    IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;'+@nl
  +'    DECLARE @m nvarchar(2048) = ERROR_MESSAGE();'+@nl
  +'    RAISERROR(N''Job '+REPLACE(REPLACE(j.name,'%','%%'),'''','''''')+' was not created: %s'', 16, 1, @m);'+@nl
  +'END CATCH;'
FROM #job x JOIN dbo.sysjobs j ON j.job_id = x.job_id
LEFT JOIN $tCategories cat ON cat.category_id = j.category_id AND cat.category_class = 1
LEFT JOIN $tOperators oe ON oe.id = j.notify_email_operator_id
LEFT JOIN $tOperators op ON op.id = j.notify_page_operator_id
LEFT JOIN $tOperators onn ON onn.id = j.notify_netsend_operator_id;

INSERT #ddl(phase, ord, txt)
SELECT 1, 12, '-- !! MANUAL: job '+j.name+' step '+CAST(s.step_id AS varchar(5))+' runs an SSIS package; the package (msdb store, file system or SSIS catalog) is not scripted'
FROM #job x JOIN dbo.sysjobs j ON j.job_id = x.job_id JOIN $tSteps s ON s.job_id = j.job_id
WHERE s.subsystem = 'SSIS';
"@

    Add-Section 'warnings: Agent objects that are never scripted' @"
INSERT #ddl(phase, ord, txt)
SELECT 1, 13, '-- !! NOT SCRIPTED: '+CAST(COUNT(*) AS varchar(12))+' SQL Server Agent alert(s); Agent alerts, the fail-safe operator and Agent properties are not scripted'
FROM $tAlerts HAVING COUNT(*) > 0;
"@

    Add-Section 'dropped (NULL) statements' @'
INSERT #ddl(phase, ord, txt)
SELECT 1, 99, '-- !! INTERNAL: '+CAST(COUNT(*) AS varchar(12))+' statement(s) in phase(s) '
   +STUFF((SELECT DISTINCT ','+CAST(x.phase AS varchar(5)) FROM #ddl x WHERE x.txt IS NULL FOR XML PATH('')),1,1,'')
   +' evaluated to NULL and were dropped (a job or dependency is missing from this script)'
FROM #ddl WHERE txt IS NULL HAVING COUNT(*) > 0;
'@

    Add-Section 'footer' @'
INSERT #ddl(phase, ord, txt)
SELECT 99, 0, '-- End of generated script. Items needing attention: '+CAST(ISNULL(SUM(a.k),0) AS varchar(12))
   +'; expected (not scripted by design, or a manual step): '+CAST(ISNULL(SUM(1 - a.k),0) AS varchar(12))+' (search for  -- !!)'
FROM (SELECT CASE WHEN txt LIKE '-- !! SECTION FAILED%' OR txt LIKE '-- !! INTERNAL%' OR txt LIKE '-- !! ORDER CONFLICT%'
                    OR txt LIKE '%could not be read%' OR txt LIKE '%not visible to this login%' OR txt LIKE '%may be truncated%' THEN 1 ELSE 0 END AS k
      FROM #ddl WHERE txt LIKE '-- !!%') a;
'@

    if ($EmitSections) { return $sections }

    # =================================================================== ASSEMBLE
    $sb = New-Object System.Text.StringBuilder
    [void]$sb.AppendLine(@"
USE [msdb];
SET NOCOUNT ON;
DECLARE @nl nvarchar(2) = NCHAR(13)+NCHAR(10);
DECLARE @q nvarchar(max);
IF OBJECT_ID('tempdb..#ddl') IS NOT NULL DROP TABLE #ddl;
CREATE TABLE #ddl (id int IDENTITY(1,1) PRIMARY KEY, phase int NOT NULL, ord bigint NOT NULL DEFAULT 0, txt nvarchar(max) COLLATE DATABASE_DEFAULT);
IF OBJECT_ID('tempdb..#job') IS NOT NULL DROP TABLE #job;
CREATE TABLE #job (job_id uniqueidentifier NOT NULL PRIMARY KEY);

INSERT #ddl(phase, ord, txt) VALUES (0, 0, 'USE [msdb];');
INSERT #ddl(phase, ord, txt) VALUES (0, 1, '-- SQL Server Agent jobs, generated for SQL Server major version $v');
"@)

    if ($FromProcedures) {
        # Result sets of the sp_help_* procedures (staging, exact column lists - INSERT ... EXEC is positional) and the
        # #mva_* tables the sections read. String columns use the msdb collation so comparisons match the catalog tables on
        # any server, case-sensitive ones included.
        [void]$sb.AppendLine(@'
INSERT #ddl(phase, ord, txt) VALUES (0, 2, '-- (Agent metadata read through the msdb.dbo.sp_help_* procedures - Amazon RDS mode)');
CREATE TABLE #mva_hjstep (step_id int, step_name sysname COLLATE DATABASE_DEFAULT, subsystem nvarchar(40) COLLATE DATABASE_DEFAULT, command nvarchar(max) COLLATE DATABASE_DEFAULT, flags int,
    cmdexec_success_code int, on_success_action int, on_success_step_id int, on_fail_action int, on_fail_step_id int, server sysname NULL, database_name sysname NULL, database_user_name sysname NULL,
    retry_attempts int, retry_interval int, os_run_priority int, output_file_name nvarchar(400) NULL, last_run_outcome int, last_run_duration int, last_run_retries int, last_run_date int, last_run_time int, proxy_id int NULL);
CREATE TABLE #mva_hjsched (schedule_id int, schedule_name sysname, enabled int, freq_type int, freq_interval int, freq_subday_type int, freq_subday_interval int, freq_relative_interval int,
    freq_recurrence_factor int, active_start_date int, active_end_date int, active_start_time int, active_end_time int, date_created datetime, schedule_description nvarchar(4000),
    next_run_date int, next_run_time int, schedule_uid uniqueidentifier, job_count int);
CREATE TABLE #mva_hjserv (server_id int, server_name sysname, enlist_date datetime NULL, last_poll_date datetime NULL);
CREATE TABLE #mva_hplogin (proxy_id int, proxy_name sysname, flags int, name sysname, sid varbinary(85), principal_id int);
CREATE TABLE #mva_hpsub (subsystem_id int, subsystem_name sysname, proxy_id int, proxy_name sysname);

CREATE TABLE #mva_jobsteps (job_id uniqueidentifier NOT NULL, step_id int, step_name sysname COLLATE DATABASE_DEFAULT, subsystem nvarchar(40) COLLATE DATABASE_DEFAULT, command nvarchar(max) COLLATE DATABASE_DEFAULT, flags int,
    cmdexec_success_code int, on_success_action int, on_success_step_id int, on_fail_action int, on_fail_step_id int, database_name sysname COLLATE DATABASE_DEFAULT NULL, database_user_name sysname COLLATE DATABASE_DEFAULT NULL,
    retry_attempts int, retry_interval int, os_run_priority int, output_file_name nvarchar(400) COLLATE DATABASE_DEFAULT NULL, proxy_id int NULL);
CREATE TABLE #mva_jobfail (job_id uniqueidentifier NOT NULL, msg nvarchar(2048), num int);
CREATE TABLE #mva_jobschedules (job_id uniqueidentifier NOT NULL, schedule_id int);
CREATE TABLE #mva_jobservers (job_id uniqueidentifier NOT NULL, server_id int);
CREATE TABLE #mva_categories (category_id int, category_type tinyint, name sysname COLLATE DATABASE_DEFAULT, category_class int NOT NULL DEFAULT 1);
CREATE TABLE #mva_operators (id int, name sysname COLLATE DATABASE_DEFAULT, enabled tinyint, email_address nvarchar(100) COLLATE DATABASE_DEFAULT NULL, last_email_date int, last_email_time int,
    pager_address nvarchar(100) COLLATE DATABASE_DEFAULT NULL, last_pager_date int, last_pager_time int, weekday_pager_start_time int, weekday_pager_end_time int, saturday_pager_start_time int,
    saturday_pager_end_time int, sunday_pager_start_time int, sunday_pager_end_time int, pager_days tinyint, netsend_address nvarchar(100) COLLATE DATABASE_DEFAULT NULL,
    last_netsend_date int, last_netsend_time int, category_name sysname NULL);
CREATE TABLE #mva_proxies (proxy_id int, name sysname COLLATE DATABASE_DEFAULT, credential_identity nvarchar(512) NULL, enabled tinyint, description nvarchar(1024) COLLATE DATABASE_DEFAULT NULL,
    user_sid varbinary(85) NULL, credential_id int, credential_identity_exists int);
CREATE TABLE #mva_proxylogin (proxy_id int, sid varbinary(85));
CREATE TABLE #mva_proxysubsys (proxy_id int, subsystem_id int);
CREATE TABLE #mva_schedules (schedule_id int, schedule_uid uniqueidentifier, name sysname COLLATE DATABASE_DEFAULT, enabled int, freq_type int, freq_interval int, freq_subday_type int,
    freq_subday_interval int, freq_relative_interval int, freq_recurrence_factor int, active_start_date int, active_end_date int, active_start_time int, active_end_time int,
    date_created datetime, schedule_description nvarchar(4000), job_count int);
CREATE TABLE #mva_alerts (id int, name sysname, event_source nvarchar(100), event_category_id int, event_id int, message_id int, severity int, enabled tinyint, delay_between_responses int,
    last_occurrence_date int, last_occurrence_time int, last_response_date int, last_response_time int, notification_message nvarchar(512), include_event_description tinyint,
    database_name nvarchar(128), event_description_keyword nvarchar(100), occurrence_count int, count_reset_date int, count_reset_time int, job_id uniqueidentifier, job_name sysname,
    has_notification int, flags int, performance_condition nvarchar(512), category_name sysname, wmi_namespace nvarchar(512), wmi_query nvarchar(512), type int);
'@)
    }

    foreach ($s in $sections) {
        $esc = $s.Sql.Replace("'", "''")
        [void]$sb.AppendLine("/* ---------- $($s.Name) ---------- */")
        [void]$sb.AppendLine('BEGIN TRY')
        [void]$sb.AppendLine("SET @q = N'$esc';")
        $run = 'EXEC sys.sp_executesql @q, N''@nl nvarchar(2)'', @nl;'
        if ($s.Guard) { $run = 'IF (' + $s.Guard + ') ' + $run }
        [void]$sb.AppendLine($run)
        [void]$sb.AppendLine('END TRY')
        [void]$sb.AppendLine('BEGIN CATCH')
        [void]$sb.AppendLine("INSERT #ddl(phase, ord, txt) VALUES (1, 9, N'-- !! SECTION FAILED [$($s.Name)]: ' + REPLACE(REPLACE(ISNULL(ERROR_MESSAGE(), N''), CHAR(13), N' '), CHAR(10), N' '));")
        [void]$sb.AppendLine('END CATCH;')
        [void]$sb.AppendLine('')
    }

    [void]$sb.AppendLine(@'
/* ---------- output: every row ends with its own GO ---------- */
IF OBJECT_ID('tempdb..#out') IS NOT NULL DROP TABLE #out;
SELECT id, phase, ord, txt + @nl + N'GO' AS t INTO #out FROM #ddl WHERE txt IS NOT NULL;
'@)
    if ($OutputMode -eq 'Lines') {
        [void]$sb.AppendLine(@'
;WITH s AS (
    SELECT phase, ord, id, CAST(1 AS int) AS ln, CAST(1 AS int) AS p, CAST(CHARINDEX(@nl, t, 1) AS int) AS e, t FROM #out
    UNION ALL
    SELECT phase, ord, id, ln + 1, e + 2, CAST(CHARINDEX(@nl, t, e + 2) AS int), t FROM s WHERE e > 0
)
SELECT SUBSTRING(t, p, CASE WHEN e = 0 THEN DATALENGTH(t)/2 + 1 - p ELSE e - p END) AS [ ]
FROM s ORDER BY phase, ord, id, ln
OPTION (MAXRECURSION 0);
'@)
    } else {
        [void]$sb.AppendLine('SELECT t AS [ddl] FROM #out ORDER BY phase, ord, id;')
    }
    return $sb.ToString()
}

Function IsAgSecondary()
{
    param( 
        [Parameter(Mandatory=$false)] [String] $Server, 
        [Parameter(Mandatory=$false)] [String] $Database 
     )
    TRY {
        [Bool]$IsAgSecondary = $true

        If (($SqlVersion).SubString(0,2) -in("12","13","14","15","16","17")) {  
            $sql = "SELECT 1 As Result FROM master.sys.databases db 
                INNER JOIN master.sys.dm_hadr_database_replica_states drs ON db.database_id = drs.database_id
                INNER JOIN master.sys.dm_hadr_availability_replica_states ars ON drs.replica_id = ars.replica_id
                WHERE db.name = '$Database' AND ars.role_desc NOT IN('PRIMARY') AND ars.is_local = 1"
            $AgSecondary = (Invoke-Sql -ServerInstance $Server -Database 'master' -Query $sql) 
            IF (!($AgSecondary.Result -eq 1)) {
               $IsAgSecondary = $False 
            }
        } Else {
            $IsAgSecondary = $False
        }
        Return $IsAgSecondary
    } CATCH {
        IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
        IF ($Null -ne $MvaLogQueue) { Throw "IsAgSecondary : $ErrorMsg" }      ## in a parallel worker: the worker's own error line reports it, once
        LogActivity "** ERROR: IsAgSecondary : $ErrorMsg" $True
        Exit_Script -ErrorRaised $True
    }
}    

Function IsRDS()
{
    param( 
        [Parameter(Mandatory=$true)] [String] $Server 
     )
    TRY {
        [Bool]$IsRDS = $false

        If (!([string]::IsNullOrWhiteSpace($Server))) {
            ForEach ($item in $RdsInstances) {
                IF (!([string]::IsNullOrWhiteSpace($item))) {
                    IF ( $RdsDetails."$item".split("|")[0] -like "$Server*" ) {
                        $IsRDS = $true
                    }
                }
            }
        } 
		
        IF (-not $IsRDS) {
            $sql = "SELECT 1 As Result FROM [master].[sys].[databases] WHERE [name] = 'rdsadmin'"
            $result = (Invoke-Sql -ServerInstance $Server -Database 'master' -Query $sql) 
            IF ($result.result -eq '1') {
                $IsRDS = $true
            }
        }	
		
        Return $IsRDS
    } CATCH {
        IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
        LogActivity "** ERROR: IsRDS() : $ErrorMsg" $True
        Exit_Script -ErrorRaised $True
    }
} 

Function HasSqlAgent()
{
    param( 
        [Parameter(Mandatory=$true)] [String] $Server 
     )
    TRY {
        [Bool]$HasSqlAgent = $false

        If (!([string]::IsNullOrWhiteSpace($Server))) {
            $sql = "SELECT SERVERPROPERTY ('edition') AS Edition"
            $output = (Invoke-Sql -ServerInstance $Server -Database 'master' -Query $sql) 
            IF (!($output.Edition -like 'Express Edition*')) {
               $HasSqlAgent = $True 
            }
        }
        Return $HasSqlAgent
    } CATCH {
        IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
        LogActivity "** ERROR: HasSqlAgent() : $ErrorMsg" $True
        Exit_Script -ErrorRaised $True
    }
} 

Function ContainsUserObjects()
{
    param( 
        [Parameter(Mandatory=$true)] [String] $Server,
        [Parameter(Mandatory=$true)] [String] $Database 
     )
    TRY {
        [Bool]$ContainsUserObjects = $false

        If ( (!([string]::IsNullOrWhiteSpace($Server))) -and (!([string]::IsNullOrWhiteSpace($Database))) ) {
            $sql = "SELECT COUNT(*) AS TotalUserObjects FROM sys.objects WHERE is_ms_shipped = 0;"
            $output = ''
            $output = (Invoke-Sql -ServerInstance $Server -Database $Database -Query $sql) 
            IF (!([string]::IsNullOrWhiteSpace($output))) {
                IF ( $output.TotalUserObjects -gt 0 ) {
                   $ContainsUserObjects = $True                 
                }
            }
        }
    } CATCH {
        IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
            LogActivity "** ERROR: ContainsUserObjects() : $ErrorMsg" $True
            $ContainsUserObjects = $True 
            #Exit_Script -ErrorRaised $True
    }
    Return $ContainsUserObjects
} 

Function FormatServerName()
{
    param( 
        [Parameter(Mandatory=$true)] [String] $Server 
     )
    TRY {
        [String]$FormatServerName = ''

        If (!([string]::IsNullOrWhiteSpace($Server))) {
            $FormatServerName = $Server.replace(".",$FileNameDelimiter).replace("\",$FileNameDelimiter).replace(",",$FileNameDelimiter)
        }
        Return $FormatServerName
    } CATCH {
        IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
        LogActivity "** ERROR: FormatServerName() : $ErrorMsg" $True
        Exit_Script -ErrorRaised $True
    }
} 

Function InternetCheck()
{
    TRY {
        $Internet = $False
        TRY {
            $response = Invoke-WebRequest -Uri "http://www.msftconnecttest.com/connecttest.txt" -UseBasicParsing -TimeoutSec 5
            IF ($response.StatusCode -eq 200) {
                $Internet = $True
                LogActivity "** INFO: Checking Internet Connectivity - Passed" $false 
            }
        } CATCH {
            $Internet = $False
            LogActivity "** INFO: Checking Internet Connectivity - No Access Found" $True 
        }    
        Return $Internet
    } CATCH {
        IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
        LogActivity "** INFO: Checking Internet Connectivity - No Access Found" $True 
        Return $False
    }
} 

Function ScriptVersionCheck()
{
    TRY {
        $Url = "https://api.github.com/repos/evolvecloudservicesgit/mva-data-collection/releases/latest"  

        IF (InternetCheck) {
            $CurrentScriptVersion = GetVersion
            $LatestScriptVersion = (Invoke-RestMethod -Uri $Url).name

            If ($LatestScriptVersion -gt $CurrentScriptVersion) {
                LogActivity "** ALERT: This version of MVA-Data-Collection.ps1 is out of date. Version: $LatestScriptVersion is now available for download" $True
                LogActivity "** ALERT: Download Version: $LatestScriptVersion @ https://github.com/evolvecloudservicesgit/mva-data-collection" $True

                $ContinueConfirm = Read-Host "Continue With Current Version? (or Exit and Download Update): (Y/N)"
                IF (!($ContinueConfirm.ToUpper() -eq 'Y')) {
                    LogActivity "** INFO: Exiting to Download Updated MVA-Data-Collection.ps1" $True
                    Exit_Script -ErrorRaised $False     
                } ELSE {
                    LogActivity "** INFO: MVA Data Collection Continuing - Please Update Soon" $True             
                }   
            }   
            Else {
                LogActivity "** INFO: MVA-Data-Collection.ps1 Version Check - Passed" $True 
            }      
        }
        Else {
            LogActivity "** INFO: MVA-Data-Collection.ps1 Version Check - Unable to Check Latest Version" $True 
        }
    } CATCH {
        IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
        LogActivity "** ERROR: ScriptVersionCheck() : $ErrorMsg" $True
    }
} 

Function FormatString()
{
    param( 
        [Parameter(Mandatory=$false)] [String] $InputString, 
        [Parameter(Mandatory=$false)] [Bool] $Double = $False
    )
    TRY {
        IF ($InputString) {
            IF (-not [string]::IsNullOrWhiteSpace($InputString)) {
                IF ($IsWindows) {
                    $FormattedString = $InputString.Replace("/","\")
                    $FormattedString = $FormattedString.Replace("\\\\\","\\\\")
                    $FormattedString = $FormattedString.Replace("\\\\","\\\")
                    $FormattedString = $FormattedString.Replace("\\\","\\")
                    $FormattedString = $FormattedString.Replace("\\","\")
                    IF ($Double) {
                        $FormattedString = $FormattedString.Replace("\","\\")
                    }
                } ElseIf ($IsMacOS -or $IsLinux) {
                    $FormattedString = $InputString.Replace("\","/")                    
                    $FormattedString = $FormattedString.Replace("/////","////")
                    $FormattedString = $FormattedString.Replace("////","///")                    
                    $FormattedString = $FormattedString.Replace("///","//")
                    $FormattedString = $FormattedString.Replace("//","/")
                    IF ($Double) {
                        $FormattedString = $FormattedString.Replace("/","//")
                    }
                } Else {
                    LogActivity "** ERROR: OS Not Recognized" $True
                    Exit_Script -ErrorRaised $True
                }
            }
        }
        Return $FormattedString
    } CATCH {
        IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
        LogActivity "** ERROR: FormatString() : $ErrorMsg" $True
        Exit_Script -ErrorRaised $True
    }
} 

Function PowerShellVersionCheck()
{
    TRY {
        $PSVersion = $PSVersionTable.PSVersion.Major.ToString() + "." + $PSVersionTable.PSVersion.Minor.ToString()

        IF ($PSVersion -lt 5.1) {
            LogActivity "** ALERT: This version of PowerShell ($PSVersion) is below the supported build of 5.1 or above. Please update PowerShell to 5.1 or higher" $True
            Exit_Script -ErrorRaised $False   
        }
        Else {
            LogActivity "** INFO: PowerShell Version $PSVersion - $PSEdition Detected" $True    
        }
    } CATCH {
        IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
        LogActivity "** ERROR: PowerShellVersionCheck() : $ErrorMsg" $True
    }
} 

Function InvokeMvaParallel()
{
    param(
        [Parameter(Mandatory=$true)]  [String]                              $Label,
        [Parameter(Mandatory=$true)]  [String[]]                            $Groups,
        [Parameter(Mandatory=$true)]  [ScriptBlock]                         $Prelude,
        [Parameter(Mandatory=$true)]  [ScriptBlock]                         $ItemWorker,
        [Parameter(Mandatory=$false)] [ScriptBlock]                         $Finalizer = $Null,
        [Parameter(Mandatory=$false)] [ScriptBlock]                         $OnGroupDone = $Null,
        [Parameter(Mandatory=$false)] [Object]                              $Context = $Null,
        [Parameter(Mandatory=$false)] [String[]]                            $Functions = @(),
        [Parameter(Mandatory=$false)] [System.Collections.IDictionary]      $Variables = @{},
        [Parameter(Mandatory=$false)] [Int]                                 $GroupThreads = 1,
        [Parameter(Mandatory=$false)] [Int]                                 $ItemThreads = 1,
        [Parameter(Mandatory=$false)] [String]                              $GroupNoun = 'group',
        [Parameter(Mandatory=$false)] [String]                              $ItemNoun = 'item'
    )

    $pool = $Null
    $tasks = New-Object System.Collections.ArrayList
    $States = @()

    TRY {
        $Groups = @($Groups | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
        IF ($Groups.Count -eq 0) { Return @() }
        $GroupThreads = [Math]::Min([Math]::Max($GroupThreads, 1), [Math]::Min(32, $Groups.Count))
        $ItemThreads  = [Math]::Min([Math]::Max($ItemThreads, 1), 16)
        LogActivity "** INFO: $Label : $($Groups.Count) $GroupNoun(s), $GroupThreads parallel $GroupNoun thread(s) x $ItemThreads parallel $ItemNoun thread(s) per $GroupNoun" $True

        ## ---- per-group state
        ForEach ($g in $Groups) {
            $States += [pscustomobject]@{
                Group = $g; State = 'Pending'; Fatal = ''; Prelude = $Null; Items = @()
                Queue = (New-Object 'System.Collections.Generic.Queue[int]'); BgQueue = (New-Object 'System.Collections.Generic.Queue[int]')
                Running = 0; BgRunning = 0; ItemResults = (New-Object System.Collections.ArrayList); Crashes = 0; Final = $Null; FinalCrashed = $False
            }
        }

        ## ---- runspace pool: copies of the named functions + worker-side replacements + the variables they read
        $Queue = New-Object 'System.Collections.Concurrent.ConcurrentQueue[object]'
        $iss = [System.Management.Automation.Runspaces.InitialSessionState]::CreateDefault()
        ForEach ($fn in @($Functions)) {
            $cmd = Get-Command -Name $fn -CommandType Function -ErrorAction SilentlyContinue
            IF ($Null -eq $cmd) { Throw "InvokeMvaParallel: function $fn is not defined" }
            $iss.Commands.Add((New-Object System.Management.Automation.Runspaces.SessionStateFunctionEntry($fn, $cmd.ScriptBlock.ToString())))
        }
        $iss.Commands.Add((New-Object System.Management.Automation.Runspaces.SessionStateFunctionEntry('LogActivity',
            'param([string]$ErrorInfo, [bool]$Display) [void]$MvaLogQueue.Enqueue([pscustomobject]@{ Text = $ErrorInfo; Display = $Display; Stamp = (Get-Date -Format "yyyy-MM-dd HH:mm:ss.fff") })')))
        $iss.Commands.Add((New-Object System.Management.Automation.Runspaces.SessionStateFunctionEntry('Exit_Script',
            'param([Bool]$ErrorRaised) throw "Exit_Script was requested inside a parallel worker (ErrorRaised=$ErrorRaised)"')))
        $all = [ordered]@{}
        ForEach ($k in $Variables.Keys) { $all[$k] = $Variables[$k] }
        $all['MvaLogQueue'] = $Queue
        ForEach ($name in $all.Keys) {
            $iss.Variables.Add((New-Object System.Management.Automation.Runspaces.SessionStateVariableEntry($name, $all[$name], $Null)))
        }
        $pool = [System.Management.Automation.Runspaces.RunspaceFactory]::CreateRunspacePool(1, [Math]::Max($GroupThreads * ($ItemThreads + 1), 1), $iss, $Host)
        $pool.Open()

        ## the runner executes a worker script and returns the last object the worker output
        $Runner = @'
param([string]$WorkerText, [object[]]$WorkerArgs)
$__sb = [scriptblock]::Create($WorkerText)
$__out = @(& $__sb @WorkerArgs)
IF ($__out.Count -gt 0) { $__out[$__out.Count - 1] }
'@

        ## ---- scheduler helpers (all run on this thread)
        ## $Max = 0 : write what is queued right now (a snapshot, so workers that keep logging cannot keep this thread here); $Max > 0 : at most that
        ## many. Either way the messages go out as one batch.
        $Drain = {
            param([int]$Max = 0)
            $limit = $(IF ($Max -gt 0) { $Max } Else { $Queue.Count })
            $batch = New-Object 'System.Collections.Generic.List[object]'
            $m = $Null
            WHILE ($batch.Count -lt $limit -and $Queue.TryDequeue([ref]$m)) { $batch.Add($m) }
            IF ($batch.Count -gt 0) { LogActivityBatch $batch.ToArray() }
        }
        $StartTask = {
            param([string]$Kind, $St, [int]$Index, [ScriptBlock]$Script, [object[]]$WorkerArgs)
            $ps = [System.Management.Automation.PowerShell]::Create()
            $ps.RunspacePool = $pool
            [void]$ps.AddScript($Runner).AddArgument($Script.ToString()).AddArgument($WorkerArgs)
            [void]$tasks.Add([pscustomobject]@{ Kind = $Kind; St = $St; Index = $Index; PS = $ps; Handle = $ps.BeginInvoke(); Reported = $False })
        }
        $FinishGroup = {
            param($St)
            $St.State = 'Done'
            IF ($Null -ne $OnGroupDone) { TRY { & $OnGroupDone $St } CATCH { LogActivity "** WARNING: $Label : $($St.Group) : group summary failed : $($_.Exception.Message)" $False } }
        }
        $ItemName = {
            param($St, [int]$Index)
            $it = $St.Items[$Index]
            IF ($Null -ne $it -and $it.PSObject.Properties['Name'] -and -not [string]::IsNullOrWhiteSpace([string]$it.Name)) { Return [string]$it.Name }
            Return "item $Index"
        }

        WHILE (@($States | Where-Object { $_.State -ne 'Done' }).Count -gt 0) {
            & $Drain 250        ## capped, so a flood of worker messages cannot keep the scheduler from collecting finished tasks and starting new ones

            ## ---- collect finished tasks
            ForEach ($task in @($tasks | Where-Object { (-not $_.Reported) -and $_.Handle.IsCompleted })) {
                $task.Reported = $True
                $st = $task.St
                $res = $Null
                $crash = ''
                TRY {
                    $out = @($task.PS.EndInvoke($task.Handle))
                    IF ($out.Count -gt 0) { $res = $out[$out.Count - 1] }
                } CATCH {
                    $inner = $_.Exception
                    WHILE ($Null -ne $inner.InnerException) { $inner = $inner.InnerException }
                    $crash = $inner.Message
                }
                IF ($task.PS.Streams.Error.Count -gt 0) {
                    $task.PS.Streams.Error | Select-Object -First 3 | ForEach-Object { LogActivity "** WARNING: $Label : $($st.Group) : worker error stream : $($_.ToString())" $False }
                }
                $task.PS.Dispose()
                & $Drain
                IF ($Null -eq $res) {
                    $crash = $(IF ([string]::IsNullOrWhiteSpace($crash)) { 'no result was returned by the worker' } Else { "worker failed : $crash" })
                }

                SWITCH ($task.Kind) {
                    'Prelude' {
                        IF ($Null -eq $res) {
                            $st.Fatal = $crash
                            LogActivity "** ERROR: $Label : $($st.Group) : $crash - this server did not complete" $True
                            & $FinishGroup $st
                        } Else {
                            $st.Prelude = $res
                            IF ($res.PSObject.Properties['Fatal'] -and -not [string]::IsNullOrWhiteSpace([string]$res.Fatal)) {
                                $st.Fatal = [string]$res.Fatal
                                & $FinishGroup $st
                            } Else {
                                $st.Items = @($(IF ($res.PSObject.Properties['Items']) { $res.Items }))
                                For ($i = 0; $i -lt $st.Items.Count; $i++) {
                                    [void]$st.ItemResults.Add($Null)
                                    $isBg = ($Null -ne $st.Items[$i]) -and $st.Items[$i].PSObject.Properties['Background'] -and [bool]$st.Items[$i].Background
                                    IF ($isBg) { $st.BgQueue.Enqueue($i) } Else { $st.Queue.Enqueue($i) }
                                }
                                $st.State = 'Items'
                            }
                        }
                    }
                    'Item' {
                        IF ($st.Items[$task.Index].PSObject.Properties['Background'] -and [bool]$st.Items[$task.Index].Background) { $st.BgRunning-- } Else { $st.Running-- }
                        IF ($Null -eq $res) {
                            $st.Crashes++
                            LogActivity "** ERROR: $Label : $($st.Group) : $(& $ItemName $st $task.Index) - $crash" $True
                        } Else {
                            $st.ItemResults[$task.Index] = $res
                        }
                    }
                    'Final' {
                        IF ($Null -eq $res) {
                            $st.FinalCrashed = $True
                            LogActivity "** ERROR: $Label : $($st.Group) : final step - $crash" $True
                        } Else {
                            $st.Final = $res
                        }
                        & $FinishGroup $st
                    }
                }
            }

            ## ---- start new work: at most $GroupThreads groups active, at most $ItemThreads foreground items per group (+1 background)
            $active = @($States | Where-Object { $_.State -ne 'Pending' -and $_.State -ne 'Done' }).Count
            ForEach ($st in $States) {
                IF ($st.State -eq 'Pending' -and $active -lt $GroupThreads) {
                    $st.State = 'Prelude'
                    $active++
                    & $StartTask 'Prelude' $st -1 $Prelude @($st.Group, $Context)
                }
                IF ($st.State -eq 'Items') {
                    WHILE ($st.BgRunning -lt 1 -and $st.BgQueue.Count -gt 0) {
                        $i = $st.BgQueue.Dequeue(); $st.BgRunning++
                        & $StartTask 'Item' $st $i $ItemWorker @($st.Group, $st.Prelude, $st.Items[$i])
                    }
                    WHILE ($st.Running -lt $ItemThreads -and $st.Queue.Count -gt 0) {
                        $i = $st.Queue.Dequeue(); $st.Running++
                        & $StartTask 'Item' $st $i $ItemWorker @($st.Group, $st.Prelude, $st.Items[$i])
                    }
                    IF ($st.Queue.Count -eq 0 -and $st.BgQueue.Count -eq 0 -and $st.Running -eq 0 -and $st.BgRunning -eq 0) {
                        IF ($Null -ne $Finalizer) {
                            $st.State = 'Final'
                            & $StartTask 'Final' $st -1 $Finalizer @($st.Group, $st.Prelude, @($st.ItemResults.ToArray()))
                        } Else {
                            & $FinishGroup $st
                        }
                    }
                }
            }

            IF (@($States | Where-Object { $_.State -ne 'Done' }).Count -gt 0 -and $Queue.Count -eq 0) { Start-Sleep -Milliseconds 200 }     ## no pause while messages are waiting
        }
        & $Drain
    } CATCH {
        IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
        LogActivity "** ERROR: InvokeMvaParallel($Label) : $ErrorMsg" $True
        ForEach ($st in $States) { IF ($st.State -ne 'Done') { IF ([string]::IsNullOrWhiteSpace($st.Fatal)) { $st.Fatal = "scheduler failed : $ErrorMsg" }; $st.State = 'Done' } }
    } FINALLY {
        ForEach ($task in $tasks) { IF (-not $task.Reported) { TRY { $task.PS.Stop() } CATCH { } ; TRY { $task.PS.Dispose() } CATCH { } } }
        IF ($Null -ne $pool) { TRY { $pool.Close(); $pool.Dispose() } CATCH { } }
    }
    Return $States
}

Function GetMvaWorkerVariables()
{
    Return [ordered]@{
        SqlUser                    = $SqlUser
        SqlPassword                = $SqlPassword
        SqlServerConnectionTimeout = $SqlServerConnectionTimeout
        SqlServerQueryTimeout      = $SqlServerQueryTimeout
        FileNameDelimiter          = $FileNameDelimiter
        IsWindows                  = $IsWindows
        IsMacOS                    = $IsMacOS
        IsLinux                    = $IsLinux
        datestamp                  = $datestamp
        ScriptRoot                 = $ScriptRoot
    }
}

Function ExportDdlParallel()
{
    param(
        [Parameter(Mandatory=$true)]  [String[]] $Servers,
        [Parameter(Mandatory=$true)]  [String]   $DatabaseQuery,
        [Parameter(Mandatory=$false)] [Int]      $Threads = 1,
        [Parameter(Mandatory=$false)] [Int]      $DatabaseThreads = 1,
        [Parameter(Mandatory=$false)] [Int]      $QueryTimeout = 1800
    )

    $Summary = [pscustomobject]@{ Servers = 0; ServersFailed = 0; Databases = 0; Exported = 0; Failed = 0; AgentJobs = 0 }
    TRY {
        $Servers = @($Servers | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
        IF ($Servers.Count -eq 0) { Return $Summary }

        ## per-server export folders (main thread)
        $ExportPaths = @{}
        ForEach ($Server in $Servers) {
            $FormattedServer = ( FormatServerName $Server )
            $ExportPath = FormatString -InputString $("$ScriptRoot\Export\$datestamp\$FormattedServer\")
            IF (!(test-path $ExportPath)) { New-Item -ItemType Directory -Force -Path $ExportPath | Out-Null }
            $ExportPaths[$Server] = $ExportPath
        }

        ## step 1: SQL version + database list
        $PreludeWorker = {
            param([string]$Server, $Ctx)

            $r = [pscustomobject]@{ Server = $Server; SqlVersion = ''; ExportPath = [string]$Ctx.ExportPaths[$Server]; Fatal = ''; Items = @() }
            TRY {
                LogActivity "** INFO: Exporting SQL DDL Scripts: $Server" $True

                ## SQL Version drives which T-SQL is generated
                $result = Invoke-Sql -ServerInstance $Server -Database 'master' -Query "SELECT SERVERPROPERTY('productversion') AS version" -RaiseError $False
                IF ($result -is [string]) {
                    LogActivity "** ERROR: Determing SQL Version : $Server : $result" $True
                    $r.Fatal = "unable to read the SQL version: $result"
                    Return $r
                }
                $r.SqlVersion = [string]$result.version
                IF ([string]::IsNullOrWhiteSpace($r.SqlVersion)) {
                    LogActivity "** ERROR: DDL Export Skipped - SQL Version Unknown : $Server" $True
                    $r.Fatal = 'SQL version unknown'
                    Return $r
                }

                $Databases = Invoke-Sql -ServerInstance $Server -Database 'master' -Query $Ctx.DatabaseQuery -RaiseError $False
                IF ($Databases -is [string]) {
                    LogActivity "** ERROR: Exporting DDL : $Server : Unable to list databases : $Databases" $True
                    $r.Fatal = "unable to list databases: $Databases"
                    Return $r
                }
                $r.Items = @(@($Databases.name) | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | ForEach-Object { [pscustomobject]@{ Name = [string]$_; Background = $False } })
            } CATCH {
                IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
                LogActivity "** ERROR: Exporting DDL : $Server : $ErrorMsg" $True
                $r.Fatal = $ErrorMsg
            }
            Return $r
        }

        ## step 2: one database
        $DbWorker = {
            param([string]$Server, $P, $Item)

            $Database = [string]$Item.Name
            $ExportPath = [string]$P.ExportPath
            $r = [pscustomobject]@{ Server = $Server; Database = $Database; Exported = 0; Skipped = 0; Failed = 0 }
            $SqlVersion = [string]$P.SqlVersion                 ## IsAgSecondary reads $SqlVersion from this scope
            $SqlMajorVersion = $SqlVersion.Split('.')[0]
            TRY {
                IF (IsAgSecondary $Server $Database) { $r.Skipped = 1; Return $r }
                IF (!(ContainsUserObjects $Server $Database)) {
                    LogActivity "** INFO: Database $Database has no User Objects : Skipping DDL Export" $False
                    $r.Skipped = 1
                    Return $r
                }

                LogActivity "** INFO: Beginning Export : SQL Database DDL : $Database : $Server" $False
                $TargetSqlFile = FormatString -InputString $("$ExportPath\$Database$FileNameDelimiter$SqlMajorVersion.sql")

                ## Version specific T-SQL -> Invoke-Sql -> rows -> .sql file
                $DdlQuery = ExtractDDLScript -SqlVersion $SqlVersion
                $DdlRows  = Invoke-Sql -ServerInstance $Server -Database $Database -Query $DdlQuery -CommandTimeout $DdlCommandTimeout -RaiseError $False

                IF ($DdlRows -is [string]) {
                    LogActivity "** ERROR: Exporting DDL : $Database : $Server : $DdlRows" $True
                    $r.Failed = 1
                } Else {
                    $DdlLines = @($DdlRows | ForEach-Object { $_[0] })
                    IF ($DdlLines.Count -eq 0) {
                        LogActivity "** ERROR: Exporting DDL : $Database : $Server : No DDL was returned" $True
                        $r.Failed = 1
                    } Else {
                        [System.IO.File]::WriteAllText($TargetSqlFile, (($DdlLines -join "`r`n") + "`r`n"), (New-Object System.Text.UTF8Encoding($True)))
                        $r.Exported = 1

                        ## Anything the extractor could not script is flagged in the file with '-- !!'
                        ## ERROR = a real failure (a section failed, or statements were dropped); WARNING = needs attention (replay-order conflict, or something this
                        ## login could not read); INFO = expected - not scripted by design / unsupported, a manual step, a READ_ONLY note. Only ERROR fails the export.
                        $Flags      = @($DdlLines | Where-Object { $_ -like '-- !!*' })
                        $Failed     = @($Flags | Where-Object { $_ -like '-- !! SECTION FAILED*' -or $_ -like '-- !! INTERNAL*' })
                        $Attention  = @($Flags | Where-Object { $_ -notlike '-- !! SECTION FAILED*' -and $_ -notlike '-- !! INTERNAL*' -and ($_ -like '-- !! ORDER CONFLICT*' -or $_ -match 'could not be read|not visible to this login|may be truncated') })
                        IF ($Failed.Count -gt 0) {
                            $r.Failed = 1
                            LogActivity "** ERROR: DDL Export : $Database : $Server : $($Failed.Count) section(s) failed - DDL is incomplete : $TargetSqlFile" $True
                            $Failed | Select-Object -First 5 | ForEach-Object { LogActivity ("** ERROR: DDL Export : " + (($_ -split "`r?`n")[0])) $False }
                        }
                        IF ($Attention.Count -gt 0) {
                            LogActivity "** WARNING: DDL Export : $Database : $Server : $($Attention.Count) item(s) need attention (search the .sql file for '-- !!')" $True
                            $Attention | Select-Object -First 5 | ForEach-Object { LogActivity ("** WARNING: DDL Export : " + (($_ -split "`r?`n")[0])) $False }
                        }
                        ## expected notes (not scripted by design, manual steps) are not logged: they are counted in the footer of the .sql file
                        LogActivity "** INFO: Export Complete : SQL Database DDL : $Database : $Server" $True
                    }
                }
            } CATCH {
                IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
                LogActivity "** ERROR: Exporting DDL : $Database : $Server : $ErrorMsg" $True
                $r.Failed = 1
            }
            Return $r
        }

        ## step 3: SQL Server Agent jobs
        $AgentWorker = {
            param([string]$Server, $P, $ItemResults)

            $ExportPath = [string]$P.ExportPath
            $r = [pscustomobject]@{ Server = $Server; AgentJobs = 0; Failed = 0 }
            $SqlVersion = [string]$P.SqlVersion
            $SqlMajorVersion = $SqlVersion.Split('.')[0]
            TRY {
                LogActivity "** INFO: Beginning Export : SQL Agent Jobs : $Server" $False
                $TargetJobFile = FormatString -InputString $("$ExportPath\SQLAgentJobs$FileNameDelimiter$SqlMajorVersion.sql")

                ## Amazon RDS (rdsadmin database present): only msdb.dbo.sysjobs can be selected from, so read steps, schedules, operators etc.
                ## through the supported msdb.dbo.sp_help_* procedures instead of the msdb tables. Same detection as the 47.001 query.
                $IsRds = $False
                $IsExpress = $False
                $RdsCheck = Invoke-Sql -ServerInstance $Server -Database 'master' -Query "SELECT (SELECT COUNT(*) FROM [sys].[databases] WHERE [name] = 'rdsadmin'), CAST(SERVERPROPERTY('EngineEdition') AS int)" -RaiseError $False
                ## Invoke-Sql hands back a DataTable that PowerShell unrolls into DataRow(s) on return, so read the first row either way
                $RdsRow = IF ($RdsCheck -is [System.Data.DataTable]) { @($RdsCheck.Rows)[0] } ELSE { @($RdsCheck)[0] }
                IF ($RdsCheck -isnot [string] -and $Null -ne $RdsRow) {
                    IF ([int]$RdsRow[0] -gt 0) { $IsRds = $True }
                    IF ([int]$RdsRow[1] -eq 4) { $IsExpress = $True }
                }
                ## SQL Server Express (EngineEdition 4, Express on RDS too) has no SQL Server Agent: expected, not an error
                IF ($IsExpress) {
                    LogActivity "** INFO: SQL Agent Job Export : $Server : SQL Server Express has no SQL Server Agent - nothing to export" $True
                    Return $r
                }
                IF ($IsRds) { LogActivity "** INFO: SQL Agent Job Export : $Server : Amazon RDS detected - reading Agent metadata through the msdb.dbo.sp_help_* procedures" $False }

                $JobQuery = ExtractAgentJobScript -SqlVersion $SqlVersion -FromProcedures:$IsRds
                $JobRows  = Invoke-Sql -ServerInstance $Server -Database 'msdb' -Query $JobQuery -CommandTimeout $DdlCommandTimeout -RaiseError $False

                IF ($JobRows -is [string]) {
                    ## no msdb access / no Agent (Express): not fatal for the rest of the collection
                    LogActivity "** WARNING: Exporting SQL Agent Jobs : $Server : $JobRows" $True
                } Else {
                    $JobLines  = @($JobRows | ForEach-Object { $_[0] })
                    $JobFlags  = @($JobLines | Where-Object { $_ -like '-- !!*' })
                    ## ERROR = a real failure; WARNING = needs attention (something this login could not read); INFO = expected (not scripted by design / unsupported, manual step)
                    $JobFailed = @($JobFlags | Where-Object { $_ -like '-- !! SECTION FAILED*' -or $_ -like '-- !! INTERNAL*' })
                    $JobAttention = @($JobFlags | Where-Object { $_ -notlike '-- !! SECTION FAILED*' -and $_ -notlike '-- !! INTERNAL*' -and ($_ -like '-- !! ORDER CONFLICT*' -or $_ -match 'could not be read|not visible to this login|may be truncated') })
                    $JobCount  = @($JobLines | Where-Object { $_ -like '*EXEC dbo.sp_add_job @job_name*' }).Count
                    IF ($JobCount -eq 0 -and $JobFlags.Count -eq 0) {
                        LogActivity "** INFO: No SQL Agent Jobs found : $Server" $False
                    } Else {
                        [System.IO.File]::WriteAllText($TargetJobFile, (($JobLines -join "`r`n") + "`r`n"), (New-Object System.Text.UTF8Encoding($True)))
                        $r.AgentJobs = $JobCount
                        IF ($JobFailed.Count -gt 0) {
                            $r.Failed = 1
                            LogActivity "** ERROR: SQL Agent Job Export : $Server : $($JobFailed.Count) section(s) failed - script is incomplete : $TargetJobFile" $True
                            $JobFailed | Select-Object -First 5 | ForEach-Object { LogActivity ("** ERROR: SQL Agent Job Export : " + (($_ -split "`r?`n")[0])) $False }
                        }
                        IF ($JobAttention.Count -gt 0) {
                            LogActivity "** WARNING: SQL Agent Job Export : $Server : $($JobAttention.Count) item(s) need attention (search the .sql file for '-- !!')" $True
                            $JobAttention | Select-Object -First 5 | ForEach-Object { LogActivity ("** WARNING: SQL Agent Job Export : " + (($_ -split "`r?`n")[0])) $False }
                        }
                        ## expected notes (not scripted by design, manual steps) are not logged: they are counted in the footer of the .sql file
                        LogActivity "** INFO: Export Complete : SQL Agent Jobs ($JobCount) : $Server" $True
                    }
                }
            } CATCH {
                IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
                LogActivity "** ERROR: Exporting SQL Agent Jobs : $Server : $ErrorMsg" $True
                $r.Failed = 1
            }
            Return $r
        }

        $OnDone = {
            param($St)
            IF (-not [string]::IsNullOrWhiteSpace($St.Fatal)) { Return }
            $ex = 0; $sk = 0; $fl = 0
            ForEach ($ir in $St.ItemResults) { IF ($Null -ne $ir) { $ex += $ir.Exported; $sk += $ir.Skipped; $fl += $ir.Failed } Else { $fl++ } }
            $aj = $(IF ($Null -ne $St.Final) { $St.Final.AgentJobs } Else { 0 })
            LogActivity "** INFO: DDL Export Finished : $($St.Group) : $ex of $($St.Items.Count) database(s) exported, $sk skipped, $fl failure(s), $aj agent job(s)" $False
        }

        $Variables = GetMvaWorkerVariables
        $Variables['DdlCommandTimeout'] = $QueryTimeout
        $states = InvokeMvaParallel -Label 'DDL Export' -Groups $Servers -GroupThreads $Threads -ItemThreads $DatabaseThreads -GroupNoun 'server' -ItemNoun 'database' `
            -Context @{ DatabaseQuery = $DatabaseQuery; ExportPaths = $ExportPaths } `
            -Prelude $PreludeWorker -ItemWorker $DbWorker -Finalizer $AgentWorker -OnGroupDone $OnDone `
            -Functions @('Invoke-Sql','FormatString','IsAgSecondary','ContainsUserObjects','ExtractDDLScript','ExtractAgentJobScript') -Variables $Variables

        ForEach ($st in @($states)) {
            $Summary.Servers++
            IF (-not [string]::IsNullOrWhiteSpace($st.Fatal)) { $Summary.ServersFailed++; Continue }
            $Summary.Databases += $st.Items.Count
            ForEach ($ir in $st.ItemResults) { IF ($Null -ne $ir) { $Summary.Exported += $ir.Exported; $Summary.Failed += $ir.Failed } Else { $Summary.Failed++ } }
            IF ($Null -ne $st.Final) { $Summary.AgentJobs += $st.Final.AgentJobs; $Summary.Failed += $st.Final.Failed } ElseIf ($st.FinalCrashed) { $Summary.Failed++ }
        }

        LogActivity "** INFO: DDL Export Complete : $($Summary.Servers) server(s), $($Summary.Exported) database script(s), $($Summary.AgentJobs) agent job(s)" $True
        IF ($Summary.ServersFailed -gt 0 -or $Summary.Failed -gt 0) {
            LogActivity "** ERROR: DDL Export : $($Summary.ServersFailed) server(s) did not complete and $($Summary.Failed) database/job export(s) failed or are incomplete - see the messages above" $True
        }
    } CATCH {
        IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
        LogActivity "** ERROR: ExportDdlParallel() : $ErrorMsg" $True
        $Summary.ServersFailed++
    }
    Return $Summary
}

Function CollectPerfCountersParallel()
{
    param(
        [Parameter(Mandatory=$true)]  [String[]]  $Servers,
        [Parameter(Mandatory=$true)]  [ValidateSet(1, 2)] [Int] $Sample,
        [Parameter(Mandatory=$true)]  [Hashtable] $Cache,          ## sample 1 fills it (per server: export path, query, start time); sample 2 reads it
        [Parameter(Mandatory=$false)] [Int]       $IntervalSeconds = 60
    )

    $Summary = [pscustomobject]@{ Servers = 0; Sampled = 0; NotSampled = 0; Failed = 0; MinInterval = 0.0; MaxInterval = 0.0 }
    TRY {
        $Servers = @($Servers | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
        IF ($Servers.Count -eq 0) { Return $Summary }

        $PreludeWorker = {
            param([string]$Server, $Ctx)

            $r = [pscustomobject]@{ Server = $Server; Fatal = ''; Skipped = $False; Sample = [int]$Ctx.Sample; ExportPath = ''; PerfQ = $Null
                                    FirstAt = $Null; IntervalSeconds = [int]$Ctx.IntervalSeconds; Items = @() }
            ## quiet: Invoke-Sql logs its own errors, and the T-SQL collection reports an unreachable / unsupported server - once is enough
            Function LogActivity { param([string]$ErrorInfo, [bool]$Display) }
            TRY {
                $item = [pscustomobject]@{ Name = "performance counter sample $($Ctx.Sample) of 2"; Kind = 'Perf'; Database = ''; Background = $False }
                IF ([int]$Ctx.Sample -eq 2) {
                    IF (-not $Ctx.Cache.ContainsKey($Server)) { $r.Skipped = $True; Return $r }
                    $c = $Ctx.Cache[$Server]
                    $r.ExportPath = [string]$c.ExportPath; $r.PerfQ = $c.PerfQ; $r.FirstAt = $c.FirstAt; $r.Items = @($item)
                    Return $r
                }

                $result = Invoke-Sql -ServerInstance $Server -Database 'master' -Query "SELECT SERVERPROPERTY('productversion') AS version" -RaiseError $False
                IF ($result -is [string]) { $r.Skipped = $True; Return $r }
                $SqlVersion = [string]$result.version
                IF ([string]::IsNullOrWhiteSpace($SqlVersion) -or $SqlVersion.Length -lt 2 -or ($SqlVersion.SubString(0,2) -notin ("10","11","12","13","14","15","16","17"))) { $r.Skipped = $True; Return $r }   ## SQL 2008 forward, as the T-SQL collection

                $FormattedServer = ( FormatServerName $Server )
                $r.ExportPath = FormatString -InputString $("$ScriptRoot\Export\$datestamp\$FormattedServer\")
                IF (!(test-path $r.ExportPath)) { New-Item -ItemType Directory -Force -Path $r.ExportPath | Out-Null }

                LoadTSqlArray $SqlVersion
                $PerfKey = '44.001~[sys_dm_os_performance_counters]'
                IF (-not $global:TsqlInstance.ContainsKey($PerfKey)) { $r.Skipped = $True; Return $r }
                $r.PerfQ = [pscustomobject]@{ Key = $PerfKey; Sql = [string]$global:TsqlInstance[$PerfKey] }
                $r.Items = @($item)
            } CATCH {
                IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
                $r.Skipped = $True; $r.Items = @()      ## not sampled; the T-SQL collection reports the problem
            }
            Return $r
        }

        $ItemWorker = {
            param([string]$Server, $P, $Item)

            $ExportPath = [string]$P.ExportPath               ## ExportData writes to $ExportPath
            $r = [pscustomobject]@{ Server = $Server; Queries = 0; Failed = 0; StartedAt = $Null }
            IF ([int]$P.Sample -eq 2) {
                $wait = ([datetime]$P.FirstAt).AddSeconds([int]$P.IntervalSeconds).Subtract([datetime]::Now).TotalSeconds
                IF ($wait -gt 0) { Start-Sleep -Milliseconds ([int][Math]::Ceiling($wait * 1000)) }
            }
            $r.StartedAt = [datetime]::Now
            $ok = ExportData $Server $P.PerfQ.Key $P.PerfQ.Sql 'master' ([int]$P.Sample -eq 2) -PassThru     ## sample 2 is appended
            $r.Queries = 1
            IF (-not $ok) { $r.Failed = 1 }
            IF ([int]$P.Sample -eq 2) {
                LogActivity "** INFO: Performance counter sample 2 of 2 : $Server : $([Math]::Round(($r.StartedAt - [datetime]$P.FirstAt).TotalSeconds, 1))s after sample 1" $True
            } Else {
                LogActivity "** INFO: Performance counter sample 1 of 2 : $Server" $True
            }
            Return $r
        }

        $Variables = GetMvaWorkerVariables
        $Ctx = @{ Sample = $Sample; Cache = $Cache; IntervalSeconds = $IntervalSeconds }
        $states = InvokeMvaParallel -Label "Performance Counter Sampling ($Sample of 2)" -Groups $Servers -GroupThreads $Servers.Count -ItemThreads 1 -GroupNoun 'server' -ItemNoun 'sample' `
            -Context $Ctx -Prelude $PreludeWorker -ItemWorker $ItemWorker `
            -Functions @('Invoke-Sql','FormatString','FormatServerName','IsAgSecondary','ExportData','LoadTSqlArray') -Variables $Variables

        $intervals = @()
        ForEach ($st in @($states)) {
            $Summary.Servers++
            $ir = $(IF ($st.ItemResults.Count -gt 0) { $st.ItemResults[0] } Else { $Null })
            IF ((-not [string]::IsNullOrWhiteSpace($st.Fatal)) -or ($Null -ne $st.Prelude -and $st.Prelude.Skipped)) { $Summary.NotSampled++; Continue }
            IF ($Null -eq $ir) { $Summary.Failed++; Continue }                   ## the worker died (already logged as an error)
            IF ($ir.Failed -gt 0) { $Summary.Failed++; Continue }               ## ExportData logged the error
            $Summary.Sampled++
            IF ($Sample -eq 1) {
                $Cache[$st.Group] = [pscustomobject]@{ ExportPath = $st.Prelude.ExportPath; PerfQ = $st.Prelude.PerfQ; FirstAt = $ir.StartedAt }
            } Else {
                $intervals += ($ir.StartedAt - [datetime]$st.Prelude.FirstAt).TotalSeconds
            }
        }
        IF ($intervals.Count -gt 0) {
            $Summary.MinInterval = [Math]::Round(($intervals | Measure-Object -Minimum).Minimum, 1)
            $Summary.MaxInterval = [Math]::Round(($intervals | Measure-Object -Maximum).Maximum, 1)
        }
        LogActivity "** INFO: Performance Counter Sampling ($Sample of 2) Complete : $($Summary.Sampled) server(s) sampled, $($Summary.NotSampled) not sampled, $($Summary.Failed) failed$(IF ($Sample -eq 2 -and $Summary.Sampled -gt 0) { ", interval $($Summary.MinInterval)s - $($Summary.MaxInterval)s" })" $True
        IF ($Summary.Failed -gt 0) {
            LogActivity "** ERROR: Performance Counter Sampling ($Sample of 2) : $($Summary.Failed) server(s) could not be sampled - see the messages above" $True
        }
    } CATCH {
        IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
        LogActivity "** ERROR: CollectPerfCountersParallel() : $ErrorMsg" $True
        $Summary.Failed++
    }
    Return $Summary
}

Function CollectTsqlParallel()
{
    param(
        [Parameter(Mandatory=$true)]  [String[]] $Servers,
        [Parameter(Mandatory=$true)]  [String]   $DatabaseQuery,
        [Parameter(Mandatory=$false)] [Int]      $Threads = 1,
        [Parameter(Mandatory=$false)] [Int]      $DatabaseThreads = 1
    )

    $Summary = [pscustomobject]@{ Servers = 0; ServersFailed = 0; ServersSkipped = 0; Queries = 0; QueryFailures = 0; ItemFailures = 0 }
    TRY {
        $Servers = @($Servers | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
        IF ($Servers.Count -eq 0) { Return $Summary }

        $PreludeWorker = {
            param([string]$Server, $Ctx)

            $r = [pscustomobject]@{ Server = $Server; SqlVersion = ''; Fatal = ''; Skipped = $False; ExportPath = ''
                                    InstQ = @(); DbQ = @(); SsrsQ = @(); SsisQ = @(); Items = @() }
            TRY {
                LogActivity "** INFO: Collecting SQL Data: $Server" $True

                $result = Invoke-Sql -ServerInstance $Server -Database 'master' -Query "SELECT SERVERPROPERTY('productversion') AS version" -RaiseError $False
                IF ($result -is [string]) {
                    LogActivity "** ERROR: Determing SQL Version : $Server : $result" $True
                    $r.Fatal = "unable to read the SQL version: $result"
                    Return $r
                }
                $SqlVersion = [string]$result.version
                $r.SqlVersion = $SqlVersion
                IF ([string]::IsNullOrWhiteSpace($SqlVersion) -or $SqlVersion.Length -lt 2 -or ($SqlVersion.SubString(0,2) -notin ("10","11","12","13","14","15","16","17"))) {   ## SQL 2008 forward
                    LogActivity "** INFO: Unsupported SQL Version (Skipping Tsql data collection) : $Server : $SqlVersion" $True
                    $r.Skipped = $True
                    Return $r
                }

                ## Export directory
                $FormattedServer = ( FormatServerName $Server )
                $r.ExportPath = FormatString -InputString $("$ScriptRoot\Export\$datestamp\$FormattedServer\")
                IF (!(test-path $r.ExportPath)) { New-Item -ItemType Directory -Force -Path $r.ExportPath | Out-Null }

                ## Version specific T-SQL scripts (fills the $global:Tsql* tables of THIS runspace); sorted for a stable order
                LoadTSqlArray $SqlVersion
                $PerfKey = '44.001~[sys_dm_os_performance_counters]'     ## sampled by CollectPerfCountersParallel, not here
                $r.InstQ = @($global:TsqlInstance.GetEnumerator() | Where-Object { $_.Key -ne $PerfKey } | Sort-Object Key | ForEach-Object { [pscustomobject]@{ Key = [string]$_.Key; Sql = [string]$_.Value } })
                $r.DbQ   = @($global:TsqlDatabase.GetEnumerator()      | Sort-Object Key | ForEach-Object { [pscustomobject]@{ Key = [string]$_.Key; Sql = [string]$_.Value } })
                $r.SsrsQ = @($global:TsqlSsrsDatabases.GetEnumerator() | Sort-Object Key | ForEach-Object { [pscustomobject]@{ Key = [string]$_.Key; Sql = [string]$_.Value } })
                $r.SsisQ = @($global:TsqlSsisDatabases.GetEnumerator() | Sort-Object Key | ForEach-Object { [pscustomobject]@{ Key = [string]$_.Key; Sql = [string]$_.Value } })

                $items = @()
                $items += [pscustomobject]@{ Name = 'instance queries'; Kind = 'Instance'; Database = ''; Background = $False }

                ## Databases (replica state is checked here, one cheap query each)
                $Databases = Invoke-Sql -ServerInstance $Server -Database 'master' -Query $Ctx.DatabaseQuery -RaiseError $False
                IF ($Databases -is [string]) {
                    LogActivity "** ERROR: Collecting database names : $Databases" $True
                    $r.Fatal = "unable to list databases: $Databases"
                    Return $r
                }
                ForEach ($Database in @($Databases.name)) {
                    IF ([string]::IsNullOrWhiteSpace($Database)) { Continue }
                    TRY {
                        IF (!(IsAgSecondary $Server $Database)) { $items += [pscustomobject]@{ Name = "database $Database"; Kind = 'Db'; Database = [string]$Database; Background = $False } }
                    } CATCH {
                        IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
                        LogActivity "** ERROR: Detecting Secondary Replica State : $Database : $ErrorMsg" $True
                        $r.Fatal = "replica state check failed for ${Database}: $ErrorMsg"
                        Return $r
                    }
                }

                ## SSRS databases
                TRY {
                    $sql = @'
WITH ReportServerDatabases AS (
    SELECT name AS DatabaseName
    FROM sys.databases
    WHERE LOWER(name) LIKE '%reportserver%' AND LOWER(name) NOT LIKE '%tempdb%'
    AND state_desc = 'ONLINE'
),
AccessCheck AS (
    SELECT
        DatabaseName,
        HAS_PERMS_BY_NAME(DatabaseName, 'DATABASE', 'SELECT') AS has_select
    FROM ReportServerDatabases
)
SELECT DatabaseName
FROM AccessCheck
WHERE has_select = 1;
'@
                    $SSRSDatabases = Invoke-Sql -ServerInstance $Server -Database 'master' -Query $sql -RaiseError $False
                    IF ($SSRSDatabases -is [string]) { Throw $SSRSDatabases }
                    ForEach ($Database in @($SSRSDatabases.name)) {
                        IF ([string]::IsNullOrWhiteSpace($Database)) { Continue }
                        IF (!(IsAgSecondary $Server $Database)) { $items += [pscustomobject]@{ Name = "SSRS database $Database"; Kind = 'Ssrs'; Database = [string]$Database; Background = $False } }
                    }
                } CATCH {
                    IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
                    LogActivity "** ERROR: Collecting SSRS Metrics : $ErrorMsg" $True
                    $r.Fatal = "SSRS discovery failed: $ErrorMsg"
                    Return $r
                }

                ## SSIS database
                TRY {
                    $sql = "IF (SELECT HAS_PERMS_BY_NAME('SSISDB', 'DATABASE', 'SELECT')) = 1
                        SELECT [name] FROM [master].[sys].[databases] WHERE [name] = 'SSISDB' AND state_desc = 'ONLINE'"
                    $SSISDatabase = Invoke-Sql -ServerInstance $Server -Database 'master' -Query $sql -RaiseError $False
                    IF ($SSISDatabase -is [string]) { Throw $SSISDatabase }
                    ForEach ($Database in @($SSISDatabase.name)) {
                        IF ([string]::IsNullOrWhiteSpace($Database)) { Continue }
                        IF (!(IsAgSecondary $Server $Database)) { $items += [pscustomobject]@{ Name = "SSIS database $Database"; Kind = 'Ssis'; Database = [string]$Database; Background = $False } }
                    }
                } CATCH {
                    IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
                    LogActivity "** ERROR: Collecting SSIS Metrics : $ErrorMsg" $True
                    $r.Fatal = "SSIS discovery failed: $ErrorMsg"
                    Return $r
                }

                $r.Items = $items
            } CATCH {
                IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
                LogActivity "** ERROR: Collecting SQL Data : $Server : $ErrorMsg" $True
                $r.Fatal = $ErrorMsg
            }
            Return $r
        }

        $ItemWorker = {
            param([string]$Server, $P, $Item)

            $ExportPath = [string]$P.ExportPath               ## ExportData writes to $ExportPath
            $SqlVersion = [string]$P.SqlVersion
            $r = [pscustomobject]@{ Server = $Server; Kind = [string]$Item.Kind; Queries = 0; Failed = 0 }
            ## one start and one end event per work item (the workers' messages carry the time they were logged), so the log shows how many items of
            ## a server really ran at the same time
            $ItemName = $(IF ($Item.PSObject.Properties['Name'] -and -not [string]::IsNullOrWhiteSpace([string]$Item.Name)) { [string]$Item.Name } Else { [string]$Item.Kind })
            $ItemClock = [System.Diagnostics.Stopwatch]::StartNew()
            LogActivity "** INFO: Beginning Collection : $ItemName : $Server" $False
            SWITCH ([string]$Item.Kind) {
                'Instance' {
                    ForEach ($q in @($P.InstQ)) { $ok = ExportData $Server $q.Key $q.Sql 'master' $False -PassThru; $r.Queries++; IF (-not $ok) { $r.Failed++ } }
                }
                'Db'   { ForEach ($q in @($P.DbQ))   { $ok = ExportData $Server $q.Key $q.Sql ([string]$Item.Database) $False -PassThru; $r.Queries++; IF (-not $ok) { $r.Failed++ } } }
                'Ssrs' { ForEach ($q in @($P.SsrsQ)) { $ok = ExportData $Server $q.Key $q.Sql ([string]$Item.Database) $False -PassThru; $r.Queries++; IF (-not $ok) { $r.Failed++ } } }
                'Ssis' { ForEach ($q in @($P.SsisQ)) { $ok = ExportData $Server $q.Key $q.Sql ([string]$Item.Database) $False -PassThru; $r.Queries++; IF (-not $ok) { $r.Failed++ } } }
            }
            LogActivity "** INFO: Collection Complete : $ItemName : $($r.Queries) quer(ies), $($r.Failed) failed, $([Math]::Round($ItemClock.Elapsed.TotalSeconds, 1))s : $Server" $False
            Return $r
        }

        $OnDone = {
            param($St)
            IF (-not [string]::IsNullOrWhiteSpace($St.Fatal)) { Return }
            IF ($Null -ne $St.Prelude -and $St.Prelude.Skipped) { Return }
            $q = 0; $qf = 0
            ForEach ($ir in $St.ItemResults) { IF ($Null -ne $ir) { $q += $ir.Queries; $qf += $ir.Failed } }
            LogActivity "** INFO: Tsql Collection Finished : $($St.Group) : $q queries ($qf failed), $($St.Items.Count) work item(s)" $False
        }

        $Variables = GetMvaWorkerVariables
        $states = InvokeMvaParallel -Label 'Tsql Collection' -Groups $Servers -GroupThreads $Threads -ItemThreads $DatabaseThreads -GroupNoun 'server' -ItemNoun 'database' `
            -Context @{ DatabaseQuery = $DatabaseQuery } -Prelude $PreludeWorker -ItemWorker $ItemWorker -OnGroupDone $OnDone `
            -Functions @('Invoke-Sql','FormatString','FormatServerName','IsAgSecondary','ExportData','LoadTSqlArray') -Variables $Variables

        ForEach ($st in @($states)) {
            $Summary.Servers++
            IF (-not [string]::IsNullOrWhiteSpace($st.Fatal)) { $Summary.ServersFailed++; Continue }
            IF ($Null -ne $st.Prelude -and $st.Prelude.Skipped) { $Summary.ServersSkipped++; Continue }
            ForEach ($ir in $st.ItemResults) { IF ($Null -ne $ir) { $Summary.Queries += $ir.Queries; $Summary.QueryFailures += $ir.Failed } }
            $Summary.ItemFailures += $st.Crashes
        }
        LogActivity "** INFO: Tsql Collection Complete : $($Summary.Servers) server(s), $($Summary.Queries) queries, $($Summary.QueryFailures) failed, $($Summary.ServersSkipped) skipped" $True
        IF ($Summary.ServersFailed -gt 0 -or $Summary.ItemFailures -gt 0 -or $Summary.QueryFailures -gt 0) {
            LogActivity "** ERROR: Tsql Collection : $($Summary.ServersFailed) server(s) did not complete, $($Summary.ItemFailures) work item(s) failed and $($Summary.QueryFailures) quer(ies) failed - the collection is incomplete, see the messages above" $True
        }
    } CATCH {
        IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
        LogActivity "** ERROR: CollectTsqlParallel() : $ErrorMsg" $True
        $Summary.ServersFailed++
    }
    Return $Summary
}

Function CollectCloudWatchParallel()
{
    param(
        [Parameter(Mandatory=$true)]  [Object]    $Regions,
        [Parameter(Mandatory=$false)] [Object]    $EC2InstanceIds,
        [Parameter(Mandatory=$false)] [Object]    $FSxFileSystems,
        [Parameter(Mandatory=$false)] [Object]    $RdsInstances,
        [Parameter(Mandatory=$true)]  [Hashtable] $OutFiles,          ## EC2 / EBS / FSX / FSXVolume / RDS -> CSV path (header already written)
        [Parameter(Mandatory=$false)] [Int]       $Period = 30,
        [Parameter(Mandatory=$false)] [Int]       $Threads = 1,
        [Parameter(Mandatory=$false)] [Int]       $QueriesPerCall = 500     ## 0 = the older get-metric-statistics call per metric per day
    )

    $Summary = [pscustomobject]@{ Metrics = 0; Calls = 0; FailedCalls = 0; Rows = 0; Fallbacks = 0 }
    $Jobs = New-Object System.Collections.ArrayList

    ## the config .json files go next to the metric CSVs (the CloudWatch export folder); this folder never depends on an earlier stage
    $ConfigFolder = Split-Path -Parent $OutFiles['EC2']
    IF (!(Test-Path $ConfigFolder)) { New-Item -ItemType Directory -Force -Path $ConfigFolder | Out-Null }

    ## one job = one metric of one resource (its calls - one per day - are expanded later)
    Function AddCwJobs([string]$Kind, [string]$Region, [string]$Namespace, [string[]]$Metrics, [string[]]$Dims, [string[]]$Stats, [string]$Mode, [string[]]$Prefix, [string]$LogLabel)
    {
        ForEach ($m in $Metrics) {
            $metric = ($m).Trim()
            [void]$Jobs.Add([pscustomobject]@{ Kind = $Kind; Region = $Region; Namespace = $Namespace; Metric = $metric; Dims = $Dims; Stats = $Stats
                                               Mode = $Mode; Prefix = $Prefix; LogText = "** INFO: Exported $LogLabel CW Metric $metric" })
        }
    }

    ## ------------------------------------------------------------------------------------------------ EC2 (+ EBS volumes)
    TRY {
        ForEach ($region in $Regions) {
            IF (!([string]::IsNullOrWhiteSpace($region))) {
                [System.Array]$InstanceIdArray = $Null
                IF (!([string]::IsNullOrWhiteSpace($EC2InstanceIds))) {
                    ForEach ($InstanceId in $EC2InstanceIds) {
                        IF (!([string]::IsNullOrWhiteSpace($InstanceId))) {
                            $output = $Null
                            TRY {
                                $output = aws ec2 describe-instances --instance-ids $InstanceId --region $region 2>$null | ConvertFrom-JSON
                            } CATCH {
                                ## Ignore
                            }
                            IF (!([string]::IsNullOrWhiteSpace($output))) {
                                $InstanceIdArray += $InstanceId
                            }
                        }
                    }

                    ForEach ($EC2 in $InstanceIdArray) {
                        $EC2 = (aws ec2 describe-instances --instance-ids $EC2 --region $region 2>$null | ConvertFrom-JSON).Reservations
                        $instanceId = $insTags = $instanceName = ""
                        $instanceId = $EC2.Instances.InstanceId

                        $oFileEC2Config = FormatString -InputString $("$ConfigFolder\EC2-$instanceId.json")
                        ($EC2.Instances | ConvertTo-JSON -Depth 5) | Out-File $oFileEC2Config -encoding UTF8

                        $insTags = $EC2.Instances.Tags
                        $insType = ($EC2.Instances.InstanceType)
                        IF ($insTags.Key -eq "Name") {
                            $instanceName =  $insTags | Where-Object { $_.Key -eq "Name" } | Select-Object -expand Value
                        }

                        $ec2Dim = "Name=InstanceId,Value=$instanceId"
                        $ec2Prefix = @([string]$instanceId, [string]$instanceName, [string]$insType)
                        AddCwJobs 'EC2' $region 'AWS/EC2' @('CPUUtilization') @($ec2Dim) @('Average','Maximum') 'AvgMax' $ec2Prefix "EC2"
                        AddCwJobs 'EC2' $region 'AWS/EC2' @('EBSReadOps','EBSWriteOps','EBSReadBytes','EBSWriteBytes') @($ec2Dim) @('Sum') 'Sum' $ec2Prefix "EC2"
                        AddCwJobs 'EC2' $region 'AWS/EC2' @('NetworkIn','NetworkOut','NetworkPacketsIn','NetworkPacketsOut') @($ec2Dim) @('Maximum') 'Max' $ec2Prefix "EC2"

                        $EBS = $Null
                        $EBS = (aws ec2 describe-volumes --filters "Name=attachment.instance-id,Values=$instanceId" --region $region | ConvertFrom-JSON)
                        ForEach ($volume in $EBS.Volumes) {
                            $VolumeId = $volume.Attachments.VolumeId

                            $oFileEBSConfig = FormatString -InputString $("$ConfigFolder\$VolumeId.json")
                            ($volume | ConvertTo-JSON -Depth 4) | Out-File $oFileEBSConfig -encoding UTF8

                            AddCwJobs 'EBS' $region 'AWS/EBS' @('VolumeReadOps','VolumeWriteOps','VolumeReadBytes','VolumeWriteBytes') @("Name=VolumeId,Value=$($volume.Attachments.VolumeId)") @('Sum') 'Sum' @([string]$instanceId, [string]$VolumeId) "EBS"
                        }
                    }
                }
            }
        }
    } CATCH {
        IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
        LogActivity "** ERROR: Exporting EC2 CW Metrics : $instanceId : $ErrorMsg" $True
        Exit_Script -ErrorRaised $True
    }

    ## ------------------------------------------------------------------------------------------------ FSx
    ## 1st Gen - one dimension, FileSystemId;  2nd Gen - two dimensions, FileSystemId and FileServer
    TRY {
        ForEach ($region in $Regions) {
            IF (!([string]::IsNullOrWhiteSpace($region))) {
                [System.Array]$FSXFileSystemArray = $Null
                IF (!([string]::IsNullOrWhiteSpace($FSxFileSystems))) {
                    ForEach ($FSxFileSystemsId in $FSxFileSystems) {
                        IF (!([string]::IsNullOrWhiteSpace($FSxFileSystemsId))) {
                            $output = $Null
                            TRY {
                                $output = aws fsx describe-file-systems --file-system-ids $FSxFileSystemsId --region $region 2>$null
                            } CATCH {
                                ## Ignore
                            }
                            IF (!([string]::IsNullOrWhiteSpace($output))) {
                                $FSXFileSystemArray += $FSxFileSystemsId
                            }
                        }
                    }

                    $FSxSystems = $Null
                    $FSxSystems = aws fsx describe-file-systems --file-system-ids @($FSXFileSystemArray) --region $region | ConvertFrom-JSON

                    ForEach ($FSx in $FSxSystems.FileSystems) {
                        $FileSystemId = $FileSystemType = $StorageCapacity = $StorageType = $Tags = $FileSystemName = ""
                        $FileSystemId = $FSx.FileSystemId
                        $FileSystemType = $FSx.FileSystemType
                        $StorageCapacity = $FSx.StorageCapacity
                        $StorageType = $FSx.StorageType
                        $Tags = $FSx.Tags
                        $FSxWindowsConfig = $FSx.WindowsConfiguration | ConvertTo-JSON -Depth 10
                        $FSxOntapConfig = $FSx.OntapConfiguration | ConvertTo-JSON -Depth 10

                        if($Tags.Key -eq "Name"){$FileSystemName =  $Tags | Where-Object { $_.Key -eq "Name" } | Select-Object -expand Value}

                        $fsxDim = "Name=FileSystemId,Value=$FileSystemId"
                        $fsxPrefix = @([string]$FileSystemId, [string]$FileSystemName, [string]$FileSystemType, [string]$StorageCapacity, [string]$StorageType)
                        Switch ($FileSystemType) {
                            "Windows" {
                                $oFileFSxConfig = FormatString -InputString $("$ConfigFolder\FSx-$FileSystemId.json")
                                $FSxWindowsConfig | Out-File $oFileFSxConfig -encoding UTF8

                                AddCwJobs 'FSX' $region 'AWS/FSx' @('DataReadBytes','DataWriteBytes','DataWriteOperations','DataReadOperations','MetadataOperations') @($fsxDim) @('Sum') 'Sum' $fsxPrefix "FSx"
                                AddCwJobs 'FSX' $region 'AWS/FSx' @('ClientConnections') @($fsxDim) @('Average','Maximum') 'AvgMax' $fsxPrefix "FSx"
                            }
                            "ONTAP" {
                                $oFileFSxConfig = FormatString -InputString $("$ConfigFolder\FSx-$FileSystemId.json")
                                $FSxOntapConfig | Out-File $oFileFSxConfig -encoding UTF8

                                IF (($FSxOntapConfig | ConvertFrom-JSON).DeploymentType -like "*AZ_1") {
                                    ## Gen 1 Ontap
                                    $FSxGen = 1
                                } Else {
                                    ## Gen 2 Ontap
                                    $FSxGen = 2
                                }
                                LogActivity "** INFO: FSx Generation : $FSxGen Detected" $False

                                AddCwJobs 'FSX' $region 'AWS/FSx' @('NetworkSentBytes','NetworkReceivedBytes','DataReadBytes','DataWriteBytes','DataReadOperations','DataWriteOperations','MetadataOperations','DataReadOperationTime','DataWriteOperationTime','DiskReadBytes','DiskWriteBytes','DiskReadOperations','DiskWriteOperations') @($fsxDim) @('Sum') 'Sum' $fsxPrefix "FSx"
                                AddCwJobs 'FSX' $region 'AWS/FSx' @('FileServerCacheHitRatio') @($fsxDim) @('Average','Maximum') 'AvgMax' $fsxPrefix "FSx"

                                $FSxVolumes = aws fsx describe-volumes --filters "Name=file-system-id,Values=$FileSystemId" --no-paginate --region $region | ConvertFrom-JSON
                                ForEach ($FSxVolume in $FSxVolumes.Volumes) {
                                    $FileSystemId = $VolumeId = $VolumeType = ""
                                    $FileSystemId = $FSxVolume.FileSystemId
                                    $VolumeId = $FSxVolume.VolumeId
                                    $VolumeType = $FSxVolume.VolumeType

                                    $oFileFSxConfig = FormatString -InputString $("$ConfigFolder\FSx-$FileSystemId-$VolumeId.json")
                                    ($FSxVolume | ConvertTo-JSON -Depth 10) | Out-File $oFileFSxConfig -encoding UTF8

                                    $volDims = @("Name=FileSystemId,Value=$FileSystemId", "Name=VolumeId,Value=$VolumeId")
                                    $volPrefix = @([string]$FileSystemId, [string]$VolumeId, [string]$VolumeType)
                                    AddCwJobs 'FSXVolume' $region 'AWS/FSx' @('DataReadBytes','DataWriteBytes','DataReadOperations','DataWriteOperations','MetadataOperations','DataReadOperationTime','DataWriteOperationTime','MetadataOperationTime') $volDims @('Sum') 'Sum' $volPrefix "FSx Volume"
                                    AddCwJobs 'FSXVolume' $region 'AWS/FSx' @('StorageCapacity') $volDims @('Maximum') 'Max' $volPrefix "FSx Volume"
                                    AddCwJobs 'FSXVolume' $region 'AWS/FSx' @('StorageUsed') $volDims @('Average') 'Avg' $volPrefix "FSx Volume"
                                }
                            }
                        }
                    }
                }
            }
        }
    } CATCH {
        IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
        LogActivity "** ERROR: Exporting FSx CW Metrics : $FileSystemId : $ErrorMsg" $True
        Exit_Script -ErrorRaised $True
    }

    ## ------------------------------------------------------------------------------------------------ RDS
    TRY {
        ForEach ($region in $Regions) {
            IF (!([string]::IsNullOrWhiteSpace($region))) {
                [String]$RdsFilterValues = $Null
                IF (!([string]::IsNullOrWhiteSpace($RdsInstances))) {
                    ForEach ($Rds in $RdsInstances) {
                        IF (!([string]::IsNullOrWhiteSpace($Rds))) {
                            $output = $Null
                            TRY {
                                $output = aws rds describe-db-instances --db-instance-identifier $Rds --region $region 2>$null
                            } CATCH {
                                ## Ignore
                            }
                            IF (!([string]::IsNullOrWhiteSpace($output))) {
                                IF ([string]::IsNullOrWhiteSpace($RdsFilterValues)) {
                                    $RdsFilterValues = "DBInstances[?DBInstanceIdentifier=='$Rds' "
                                } ELSE {
                                    $RdsFilterValues = $RdsFilterValues + " || DBInstanceIdentifier=='$Rds' "
                                }
                            }
                        }
                    }
                    IF (!([string]::IsNullOrWhiteSpace($RdsFilterValues))) {
                        $RdsFilterValues = $RdsFilterValues + "]"
                    }

                    IF (!([string]::IsNullOrWhiteSpace($RdsFilterValues))) {
                        $RDSInstances2 = aws rds describe-db-instances --query $RdsFilterValues --region $region | ConvertFrom-JSON
                        ForEach ($RDSInstance in $RDSInstances2) {
                            $DBInstanceIdentifier = $RDSInstance.DBInstanceIdentifier
                            $DBEngine = $RDSInstance.Engine

                            ## the SQL Server and the non-SQL Server branches of the sequential code were identical
                            $oFileRDSConfig = FormatString -InputString $("$ConfigFolder\RDS-$DBInstanceIdentifier.json")
                            ($RDSInstance | ConvertTo-JSON -Depth 4) | Out-File $oFileRDSConfig -encoding UTF8

                            IF (-not $RDSInstance.DBParameterGroups.DBParameterGroupName.StartsWith("default.")) {
                                $DBParameterGroupName = $RDSInstance.DBParameterGroups.DBParameterGroupName
                                $oFileCustomParameterGroup = FormatString -InputString $("$ConfigFolder\CPG-$DBParameterGroupName.json")

                                $CustomParameterGroup = aws rds describe-db-parameters --db-parameter-group-name $DBParameterGroupName --region $region
                                $CustomParameterGroup | Out-File $oFileCustomParameterGroup -encoding UTF8
                            }

                            $rdsDim = "Name=DBInstanceIdentifier,Value=$DBInstanceIdentifier"
                            $rdsPrefix = @([string]$DBInstanceIdentifier, [string]$DBEngine)
                            AddCwJobs 'RDS' $region 'AWS/RDS' @('ReadThroughput','WriteThroughput','ReadIOPS','WriteIOPS','ReadLatency','WriteLatency') @($rdsDim) @('Sum') 'Sum' $rdsPrefix "RDS"
                            AddCwJobs 'RDS' $region 'AWS/RDS' @('DatabaseConnections','CPUUtilization') @($rdsDim) @('Average','Maximum') 'AvgMax' $rdsPrefix "RDS"
                            AddCwJobs 'RDS' $region 'AWS/RDS' @('NetworkReceiveThroughput','NetworkTransmitThroughput') @($rdsDim) @('Maximum') 'Max' $rdsPrefix "RDS"
                        }
                    }
                }
            }
        }
    } CATCH {
        IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
        LogActivity "** ERROR: Exporting RDS CW Metrics : $DBInstanceIdentifier : $ErrorMsg" $True
        Exit_Script -ErrorRaised $True
    }

    ## ------------------------------------------------------------------------------------------------ the AWS calls
    IF ($Jobs.Count -eq 0) {
        LogActivity "** INFO: CloudWatch Collection : no resources found - nothing to collect" $True
        Return $Summary
    }
    $Summary.Metrics = $Jobs.Count

    ## ONE clock reading, floored to a 5 minute boundary (CloudWatch builds its 300 s buckets from the request times, so every day and every run
    ## lines up). The span is unchanged: ($Period + 1) days ending now.
    [datetime]$Now = (Get-Date).ToUniversalTime()
    $Now = $Now.AddMinutes(-($Now.Minute % 5)).AddSeconds(-$Now.Second).AddMilliseconds(-$Now.Millisecond)

    $JobLines = @{}                                                       ## job index -> rows (string[])
    $JobError = @{}                                                       ## job index -> first error
    $PreludeWorker = {
        param([string]$Group, $Ctx)
        [pscustomobject]@{ Fatal = ''; Items = $Ctx.Items }
    }

    ## ---- worker A: ONE get-metric-data call for a batch of series (up to 500 queries); returns the finished rows per job
    $BatchWorker = {
        param([string]$Group, $P, $Item)

        $r = [pscustomobject]@{ Batch = $Item.Batch; Lines = @{}; Error = '' }
        $tmp = Join-Path ([System.IO.Path]::GetTempPath()) ('mva-cw-' + [guid]::NewGuid().ToString('N') + '.json')
        TRY {
            [System.IO.File]::WriteAllText($tmp, [string]$Item.QueriesJson, (New-Object System.Text.UTF8Encoding($False)))
            $psi = New-Object System.Diagnostics.ProcessStartInfo
            $psi.FileName = [string]$Item.AwsExe
            $psi.Arguments = 'cloudwatch get-metric-data --metric-data-queries "file://' + $tmp + '" --start-time ' + $Item.Start + ' --end-time ' + $Item.End + ' --scan-by TimestampAscending --region ' + $Item.Region + ' --output json'
            $psi.UseShellExecute = $False
            $psi.RedirectStandardOutput = $True
            $psi.RedirectStandardError = $True
            $psi.CreateNoWindow = $True
            $psi.StandardOutputEncoding = [System.Text.Encoding]::UTF8
            $proc = [System.Diagnostics.Process]::Start($psi)
            $errTask = $proc.StandardError.ReadToEndAsync()
            $stdout = $proc.StandardOutput.ReadToEnd()
            $proc.WaitForExit()
            $stderr = $errTask.Result
            IF ($proc.ExitCode -ne 0) {
                $r.Error = ($stderr -replace '\s+', ' ').Trim()
                IF ([string]::IsNullOrWhiteSpace($r.Error)) { $r.Error = "aws exited with code $($proc.ExitCode)" }
                Return $r
            }

            ## the CLI pages through NextToken itself; a series can come back in several pieces, so pieces with the same Id are joined
            $o = $stdout | ConvertFrom-JSON
            $series = @{}
            $bad = @()
            ForEach ($m in @($o.MetricDataResults)) {
                IF ($Null -eq $m) { Continue }
                IF ($m.StatusCode -eq 'InternalError' -or $m.StatusCode -eq 'Forbidden') { $bad += "$($m.Id): $($m.StatusCode)" }
                IF (-not $series.ContainsKey($m.Id)) { $series[$m.Id] = [pscustomobject]@{ T = (New-Object System.Collections.Generic.List[string]); V = (New-Object System.Collections.Generic.List[object]) } }
                ForEach ($t in @($m.Timestamps)) { $series[$m.Id].T.Add([string]$t) }
                ForEach ($v in @($m.Values))     { $series[$m.Id].V.Add($v) }
            }
            IF ($bad.Count -gt 0) { $r.Error = 'CloudWatch could not return: ' + (($bad | Select-Object -First 3) -join ', '); Return $r }

            ForEach ($spec in @($Item.Specs)) {
                $sa = $series["j$($spec.Index)_a"]; $sm = $series["j$($spec.Index)_m"]; $ss = $series["j$($spec.Index)_s"]
                $prefix = '"' + (@($spec.Prefix) -join '"|"') + '"|"'
                $lines = New-Object System.Collections.Generic.List[string]
                SWITCH ([string]$spec.Mode) {
                    'AvgMax' {
                        $mIdx = @{}
                        IF ($Null -ne $sm) { For ($i = 0; $i -lt $sm.T.Count; $i++) { $mIdx[$sm.T[$i]] = $i } }
                        $seen = @{}
                        IF ($Null -ne $sa) {
                            For ($i = 0; $i -lt $sa.T.Count; $i++) {
                                $AvgValue = $sa.V[$i]; $MaxValue = $Null
                                IF ($mIdx.ContainsKey($sa.T[$i])) { $MaxValue = $sm.V[$mIdx[$sa.T[$i]]] }
                                $seen[$sa.T[$i]] = 1
                                $lines.Add($prefix + $sa.T[$i] + '"|"' + $spec.Metric + '"|"' + $AvgValue + '"|"' + $MaxValue + '"|"' + 0 + '"')
                            }
                        }
                        IF ($Null -ne $sm) {
                            For ($i = 0; $i -lt $sm.T.Count; $i++) {
                                IF (-not $seen.ContainsKey($sm.T[$i])) { $lines.Add($prefix + $sm.T[$i] + '"|"' + $spec.Metric + '"|"' + $Null + '"|"' + $sm.V[$i] + '"|"' + 0 + '"') }
                            }
                        }
                    }
                    'Sum' { IF ($Null -ne $ss) { For ($i = 0; $i -lt $ss.T.Count; $i++) { $lines.Add($prefix + $ss.T[$i] + '"|"' + $spec.Metric + '"|"' + 0 + '"|"' + 0 + '"|"' + $ss.V[$i] + '"') } } }
                    'Max' { IF ($Null -ne $sm) { For ($i = 0; $i -lt $sm.T.Count; $i++) { $lines.Add($prefix + $sm.T[$i] + '"|"' + $spec.Metric + '"|"' + 0 + '"|"' + $sm.V[$i] + '"|"' + 0 + '"') } } }
                    'Avg' { IF ($Null -ne $sa) { For ($i = 0; $i -lt $sa.T.Count; $i++) { $lines.Add($prefix + $sa.T[$i] + '"|"' + $spec.Metric + '"|"' + $sa.V[$i] + '"|"' + 0 + '"|"' + 0 + '"') } } }
                }
                $r.Lines[[int]$spec.Index] = $lines.ToArray()
            }
        } CATCH {
            $r.Error = $_.Exception.Message
        } FINALLY {
            IF (Test-Path $tmp) { Remove-Item $tmp -Force -ErrorAction SilentlyContinue }
        }
        Return $r
    }

    ## ---- worker B (fallback / -CloudWatchQueriesPerCall 0): one get-metric-statistics call for one metric and one day
    $CallWorker = {
        param([string]$Group, $P, $Item)

        $r = [pscustomobject]@{ Job = $Item.Job; Day = $Item.Day; Lines = @(); Error = '' }
        TRY {
            $cli = @('cloudwatch','get-metric-statistics','--namespace',$Item.Namespace,'--metric-name',$Item.Metric,'--dimensions') + @($Item.Dims) +
                   @('--start-time',$Item.Start,'--end-time',$Item.End,'--period','300','--statistics') + @($Item.Stats) + @('--region',$Item.Region)
            $raw = @(& aws @cli 2>&1)
            $errs = @($raw | Where-Object { $_ -is [System.Management.Automation.ErrorRecord] })
            $out  = @($raw | Where-Object { $_ -isnot [System.Management.Automation.ErrorRecord] })
            IF ($LASTEXITCODE -ne 0) {
                $r.Error = (($errs | ForEach-Object { $_.ToString() }) -join ' ').Trim()
                IF ([string]::IsNullOrWhiteSpace($r.Error)) { $r.Error = "aws exited with code $LASTEXITCODE" }
                Return $r
            }
            $Data = $Null
            IF ($out.Count -gt 0) { $Data = ($out -join "`n") | ConvertFrom-JSON }
            $lines = New-Object System.Collections.Generic.List[string]
            $prefix = '"' + (@($Item.Prefix) -join '"|"') + '"|"'
            ForEach ($d in $Data.Datapoints) {
                $timeStamp = ""
                $timeStamp = $d.TimeStamp
                SWITCH ([string]$Item.Mode) {
                    'AvgMax' { $AvgValue = $d.Average; $MaxValue = $d.Maximum; $SumValue = 0 }
                    'Sum'    { $AvgValue = 0; $MaxValue = 0; $SumValue = $d.Sum }
                    'Max'    { $AvgValue = 0; $MaxValue = $d.Maximum; $SumValue = 0 }
                    'Avg'    { $AvgValue = $d.Average; $MaxValue = 0; $SumValue = 0 }
                }
                $lines.Add($prefix + $timeStamp + '"|"' + $Item.Metric + '"|"' + $AvgValue + '"|"' + $MaxValue + '"|"' + $SumValue + '"')
            }
            $r.Lines = $lines.ToArray()
        } CATCH {
            $r.Error = $_.Exception.Message
        }
        Return $r
    }

    ## ---- runs the per-day calls for the given jobs (job -> its days) and fills $JobLines / $JobError
    $RunPerDay = {
        param([int[]]$JobIndexes)
        $dayItems = New-Object System.Collections.ArrayList
        ForEach ($j in $JobIndexes) {
            $job = $Jobs[$j]
            ForEach ($i in 0..$Period) {
                [datetime]$eTime = $Now.AddDays(($i*-1))
                [datetime]$sTime = $eTime.AddDays(-1)
                [void]$dayItems.Add([pscustomobject]@{
                    Name = "$($job.Kind) $($job.Metric) day $i"; Job = $j; Day = $i; Background = $False
                    Region = $job.Region; Namespace = $job.Namespace; Metric = $job.Metric; Dims = $job.Dims; Stats = $job.Stats; Mode = $job.Mode; Prefix = $job.Prefix
                    Start = $sTime.ToString("yyyy-MM-ddTHH:mm:ssZ"); End = $eTime.ToString("yyyy-MM-ddTHH:mm:ssZ")
                })
            }
        }
        $script:CwCalls += $dayItems.Count
        LogActivity "** INFO: CloudWatch Collection : $($JobIndexes.Count) metric(s) by get-metric-statistics, $($dayItems.Count) AWS call(s), $Threads in parallel" $True
        $sts = InvokeMvaParallel -Label 'CloudWatch Collection' -Groups @('CloudWatch') -GroupThreads 1 -ItemThreads $Threads `
            -Context @{ Items = @($dayItems.ToArray()) } -Prelude $PreludeWorker -ItemWorker $CallWorker -Functions @() -Variables @{}
        $s1 = @($sts)[0]
        IF ($Null -eq $s1 -or -not [string]::IsNullOrWhiteSpace($s1.Fatal)) {
            LogActivity "** ERROR: CloudWatch Collection : $(IF ($Null -ne $s1) { $s1.Fatal } Else { 'no result' })" $True
            Exit_Script -ErrorRaised $True
        }
        $k = 0
        ForEach ($j in $JobIndexes) {
            $all = New-Object System.Collections.Generic.List[string]
            $failed = 0; $first = ''
            ForEach ($i in 0..$Period) {
                $res = $s1.ItemResults[$k]; $k++
                IF ($Null -eq $res) { $failed++; IF ($first -eq '') { $first = 'the worker failed or returned nothing' } }
                ElseIf (-not [string]::IsNullOrWhiteSpace($res.Error)) { $failed++; IF ($first -eq '') { $first = $res.Error } }
                Else { IF (@($res.Lines).Count -gt 0) { $all.AddRange([string[]]@($res.Lines)) } }
            }
            $JobLines[$j] = $all.ToArray()
            IF ($failed -gt 0) { $JobError[$j] = "$failed of $($Period + 1) AWS call(s) failed : $first"; $script:CwFailedCalls += $failed }
        }
    }

    $script:CwCalls = 0
    $script:CwFailedCalls = 0
    $script:CwFallbackJobs = 0
    $AwsExe = [string](Get-Command aws -ErrorAction SilentlyContinue | Select-Object -First 1).Source
    $QueriesPerCall = [Math]::Min([Math]::Max($QueriesPerCall, 0), 500)
    IF ($QueriesPerCall -gt 0 -and [string]::IsNullOrWhiteSpace($AwsExe)) {
        LogActivity "** WARNING: CloudWatch Collection : the aws CLI path could not be resolved - using get-metric-statistics" $True
        $QueriesPerCall = 0
    }

    IF ($QueriesPerCall -eq 0) {
        & $RunPerDay @(0..($Jobs.Count - 1))
    } Else {
        ## ---- get-metric-data: every series of a region is a query (average + maximum are two); a call carries up to $QueriesPerCall queries,
        ## fewer when the answer would get big (about 200,000 data points per call keeps one response around 15 MB)
        $pointsPerQuery = ($Period + 1) * 288
        $maxQ = [Math]::Min($QueriesPerCall, [Math]::Max(1, [int][Math]::Floor(200000 / $pointsPerQuery)))
        $startS = $Now.AddDays(-($Period + 1)).ToString("yyyy-MM-ddTHH:mm:ssZ"); $endS = $Now.ToString("yyyy-MM-ddTHH:mm:ssZ")
        $batches = New-Object System.Collections.ArrayList
        $idAbbr = @{ 'Average' = 'a'; 'Maximum' = 'm'; 'Sum' = 's' }

        ForEach ($region in @($Jobs | ForEach-Object { $_.Region } | Select-Object -Unique)) {
            $queries = New-Object System.Collections.ArrayList
            $specs = New-Object System.Collections.ArrayList
            $members = New-Object System.Collections.ArrayList
            $Flush = {
                IF ($queries.Count -gt 0) {
                    [void]$batches.Add([pscustomobject]@{
                        Name = "CloudWatch $region batch $($batches.Count + 1) ($($queries.Count) queries)"; Batch = $batches.Count + 1; Background = $False
                        Region = $region; AwsExe = $AwsExe; Start = $startS; End = $endS
                        QueriesJson = (ConvertTo-Json -InputObject @($queries.ToArray()) -Depth 8 -Compress)
                        Specs = @($specs.ToArray()); Members = @($members.ToArray())
                    })
                    $queries.Clear(); $specs.Clear(); $members.Clear()
                }
            }
            For ($j = 0; $j -lt $Jobs.Count; $j++) {
                $job = $Jobs[$j]
                IF ($job.Region -ne $region) { Continue }
                IF ($queries.Count -gt 0 -and ($queries.Count + @($job.Stats).Count) -gt $maxQ) { & $Flush }
                $dims = @(@($job.Dims) | ForEach-Object { IF ($_ -match '^Name=(.+?),Value=(.+)$') { [ordered]@{ Name = $Matches[1]; Value = $Matches[2] } } })
                ForEach ($stat in @($job.Stats)) {
                    [void]$queries.Add([ordered]@{
                        Id = "j$($j)_$($idAbbr[$stat])"
                        MetricStat = [ordered]@{ Metric = [ordered]@{ Namespace = $job.Namespace; MetricName = $job.Metric; Dimensions = $dims }; Period = 300; Stat = $stat }
                        ReturnData = $True
                    })
                }
                [void]$specs.Add([pscustomobject]@{ Index = $j; Mode = $job.Mode; Prefix = $job.Prefix; Metric = $job.Metric })
                [void]$members.Add($j)
            }
            & $Flush
        }
        $script:CwCalls += $batches.Count
        LogActivity "** INFO: CloudWatch Collection : $($Jobs.Count) metric(s) by get-metric-data, $($batches.Count) AWS call(s) of up to $maxQ queries, $Threads in parallel" $True

        $states = InvokeMvaParallel -Label 'CloudWatch Collection' -Groups @('CloudWatch') -GroupThreads 1 -ItemThreads $Threads `
            -Context @{ Items = @($batches.ToArray()) } -Prelude $PreludeWorker -ItemWorker $BatchWorker -Functions @() -Variables @{}
        $st = @($states)[0]
        IF ($Null -eq $st -or -not [string]::IsNullOrWhiteSpace($st.Fatal)) {
            LogActivity "** ERROR: CloudWatch Collection : $(IF ($Null -ne $st) { $st.Fatal } Else { 'no result' })" $True
            Exit_Script -ErrorRaised $True
        }

        ## a batch that failed (for example no cloudwatch:GetMetricData permission) is collected again the old way, metric by metric and day by day
        $fallback = New-Object System.Collections.ArrayList
        For ($b = 0; $b -lt $batches.Count; $b++) {
            $res = $st.ItemResults[$b]
            IF ($Null -eq $res -or -not [string]::IsNullOrWhiteSpace($res.Error)) {
                $why = $(IF ($Null -eq $res) { 'the worker failed or returned nothing' } Else { $res.Error })
                LogActivity "** WARNING: CloudWatch Collection : get-metric-data failed for $($batches[$b].Name) : $why - collecting those $(@($batches[$b].Members).Count) metric(s) with get-metric-statistics instead" $True
                ForEach ($j in @($batches[$b].Members)) { [void]$fallback.Add([int]$j) }
            } Else {
                ForEach ($k in @($res.Lines.Keys)) { $JobLines[[int]$k] = [string[]]@($res.Lines[$k]) }
            }
        }
        IF ($fallback.Count -gt 0) {
            $script:CwFallbackJobs = $fallback.Count
            & $RunPerDay @($fallback.ToArray())
        }
    }

    ## ---- write the rows in the original order (resource, metric), then log per metric
    $Buffers = @{}
    ForEach ($k in @('EC2','EBS','FSX','FSXVolume','RDS')) { $Buffers[$k] = New-Object System.Collections.Generic.List[string] }
    For ($j = 0; $j -lt $Jobs.Count; $j++) {
        $job = $Jobs[$j]
        $lines = $JobLines[$j]
        IF ($Null -ne $lines -and @($lines).Count -gt 0) { $Buffers[$job.Kind].AddRange([string[]]@($lines)); $Summary.Rows += @($lines).Count }
        LogActivity ($job.LogText + " : " + ($job.Prefix[0]) + $(IF ($job.Prefix.Count -gt 1 -and $job.Kind -in @('EBS','FSXVolume')) { " : " + $job.Prefix[1] } Else { '' })) $False
        IF ($JobError.ContainsKey($j)) {
            LogActivity "** ERROR: CloudWatch $($job.Namespace) $($job.Metric) : $($job.Prefix[0]) : $($JobError[$j]) - the data for this metric is incomplete" $True
        } ElseIf ($Null -eq $lines) {
            $JobError[$j] = 'no data was returned'
            $script:CwFailedCalls++
            LogActivity "** ERROR: CloudWatch $($job.Namespace) $($job.Metric) : $($job.Prefix[0]) : no result was returned - the data for this metric is incomplete" $True
        }
    }
    ForEach ($kind in @('EC2','EBS','FSX','FSXVolume','RDS')) {
        IF ($Buffers[$kind].Count -gt 0) { $Buffers[$kind] | Out-File $OutFiles[$kind] -Append -encoding UTF8 }
    }
    $Summary.Calls = $script:CwCalls
    $Summary.FailedCalls = $script:CwFailedCalls
    $Summary.Fallbacks = $script:CwFallbackJobs
    LogActivity "** INFO: CloudWatch Collection Complete : $($Summary.Metrics) metric(s), $($Summary.Calls) AWS call(s), $($Summary.Rows) row(s), $($Summary.FailedCalls) failed call(s), $($Summary.Fallbacks) metric(s) collected through the fallback" $True
    Return $Summary
}

Function Main 
{
    param( 
        [Parameter(Mandatory=$false)] [bool]   $CollectConnectionsOnly,
        [Parameter(Mandatory=$false)] [bool]   $ExportDacPacs,
        [Parameter(Mandatory=$false)] [bool]   $ExportDDL,
        [Parameter(Mandatory=$false)] [int]    $ExportDdlThreads,
        [Parameter(Mandatory=$false)] [int]    $ExportDdlDatabaseThreads,
        [Parameter(Mandatory=$false)] [int]    $CollectTsqlThreads,
        [Parameter(Mandatory=$false)] [int]    $CollectTsqlDatabaseThreads,
        [Parameter(Mandatory=$false)] [int]    $CloudWatchThreads,
        [Parameter(Mandatory=$false)] [int]    $CloudWatchQueriesPerCall,
        [Parameter(Mandatory=$false)] [bool]   $CollectCloudWatchData,
        [Parameter(Mandatory=$false)] [bool]   $CollectTsqlData,
        [Parameter(Mandatory=$false)] [bool]   $AMAOnly,
        [Parameter(Mandatory=$false)] [bool]   $CleanUpEnvironment,
        [Parameter(Mandatory=$false)] [int]    $SqlServerConnectionTimeout,   
        [Parameter(Mandatory=$false)] [int]    $SqlServerQueryTimeout,     
        [Parameter(Mandatory=$false)] [int]    $CloudWatchCollectionPeriod, 
        [Parameter(Mandatory=$false)] [bool]   $IncludeAllMsgs,
        [Parameter(Mandatory=$false)] [bool]   $ValidateResourcesOnly,
        [Parameter(Mandatory=$false)] [String] $AWSProfile,   
        [Parameter(Mandatory=$false)] [bool]   $UseSSOLogin,  
        [Parameter(Mandatory=$false)] [String] $SqlUser,
        [Parameter(Mandatory=$false)] [String] $SqlPassword,
        [Parameter(Mandatory=$false)] [String] $ExportPath,
        [Parameter(Mandatory=$false)] [String] $FileNameDelimiter,
        [Parameter(Mandatory=$false)] [bool]   $UsePublicIPs,
        [Parameter(Mandatory=$false)] [bool]   $DebugMode 
    )

    $global:LogFile = ''
    
    TRY {

        IF ($DebugMode) { 
            LogActivity "** INFO: Debug Mode Enabled" $False
            $ErrorActionPreference = "Continue" 
        } ELSE {
            $ErrorActionPreference = "SilentlyContinue"
        }

        ## Locate MVA-Data-Collection.ps1 script and set Root Path
        IF (!([string]::IsNullOrWhiteSpace($PSScriptRoot))) {
            $ScriptRoot = $PSScriptRoot
        } ElseIf ($psISE) {
            $scriptPath = $psISE.CurrentFile.FullPath
            $ScriptRoot = Split-Path -Parent $scriptPath
        } ElseIf ($MyInvocation.MyCommand.Path) {
            $scriptPath = $MyInvocation.MyCommand.Path
            $ScriptRoot = Split-Path -Parent $scriptPath
        } Else {
            LogActivity "** ERROR: Unable to Detect Script Path" $True
            Exit_Script -ErrorRaised $True
        }

        ## Test OS
        IF ($IsWindows -or $IsMacOS -or $IsLinux) {
            LogActivity "** INFO: OS Detected: $OS" $False
        } Else {
            $OS = [System.Environment]::OSVersion.Platform
            IF ($OS -eq 'Win32NT') {
                $IsWindows = $True
                $IsMacOS =  $False
            } ELSEIF ($OS -eq 'Unix') {
                $IsMacOS = $True
                $IsWindows = $False
            } ELSE {
                LogActivity "** ERROR: OS Not Recognized" $True
                Exit_Script -ErrorRaised $True
            }
            LogActivity "** INFO: OS Detected: $OS" $False
        }

        ## Test PowerShell Version
        PowerShellVersionCheck

        ## Test ExportPath Override parameter
        IF (!([string]::IsNullOrWhiteSpace($ExportPath))) {
            $ExportPath = FormatString -InputString $ExportPath
            IF (!(test-path $ExportPath)) { New-Item -ItemType Directory -Force -Path $ExportPath | Out-Null }
            $DefaultScriptRoot = $ScriptRoot
            $ScriptRoot = $ExportPath
            LogActivity "** INFO: Export Path Override Set: $ExportPath" $False
        } Else {
            $DefaultScriptRoot = $ScriptRoot
        }

        LogActivity "** INFO: Setting Script Path: $ScriptRoot" $False

        ## Initialize Log
        $global:LogFile = $Null
        $datestamp = (Get-Date -Format "MMddyyHHmmss")
        $LogPath = FormatString -InputString $("$ScriptRoot\Export\$datestamp\Log\")
        IF (!(test-path $LogPath)) { New-Item -ItemType Directory -Force -Path $LogPath | Out-Null }
        $global:LogFile = FormatString -InputString $("$LogPath\MVA_Log_$datestamp.log")
        LogActivity "** INFO: Logfile Path Set: $global:LogFile" $False

        ## Log Script Version
        $ver = GetVersion
        LogActivity "** INFO: MVA Data Collection Script - Version : $ver" $True

        ## Check for Latest Version of This Script
        ScriptVersionCheck

        ## Log Parameters
        LogActivity "** INFO: Parameter - CollectConnectionsOnly: $CollectConnectionsOnly" $False
        LogActivity "** INFO: Parameter - ExportDacPacs: $ExportDacPacs" $False
        LogActivity "** INFO: Parameter - ExportDDL: $ExportDDL" $False
        LogActivity "** INFO: Parameter - ExportDdlThreads: $ExportDdlThreads" $False
        LogActivity "** INFO: Parameter - ExportDdlDatabaseThreads: $ExportDdlDatabaseThreads" $False
        LogActivity "** INFO: Parameter - CollectTsqlThreads: $CollectTsqlThreads" $False
        LogActivity "** INFO: Parameter - CollectTsqlDatabaseThreads: $CollectTsqlDatabaseThreads" $False
        LogActivity "** INFO: Parameter - CloudWatchThreads: $CloudWatchThreads" $False
        LogActivity "** INFO: Parameter - CloudWatchQueriesPerCall: $CloudWatchQueriesPerCall" $False
        LogActivity "** INFO: Parameter - CollectCloudWatchData: $CollectCloudWatchData" $False
        LogActivity "** INFO: Parameter - CollectTsqlData: $CollectTsqlData" $False
        LogActivity "** INFO: Parameter - AMAOnly: $AMAOnly" $False        
        LogActivity "** INFO: Parameter - CleanUpEnvironment: $CleanUpEnvironment" $False
        LogActivity "** INFO: Parameter - SqlServerConnectionTimeout: $SqlServerConnectionTimeout" $False
        LogActivity "** INFO: Parameter - SqlServerQueryTimeout: $SqlServerQueryTimeout" $False
        LogActivity "** INFO: Parameter - CloudWatchCollectionPeriod: $CloudWatchCollectionPeriod" $False
        LogActivity "** INFO: Parameter - IncludeAllMsgs: $IncludeAllMsgs" $False
        LogActivity "** INFO: Parameter - ValidateResourcesOnly: $ValidateResourcesOnly" $False
        LogActivity "** INFO: Parameter - AWSProfile: $AWSProfile" $False       
        LogActivity "** INFO: Parameter - UseSSOLogin: $UseSSOLogin" $False          
        LogActivity "** INFO: Parameter - SqlUser: $SqlUser" $False
        LogActivity "** INFO: Parameter - SqlPassword: *********" $False
        LogActivity "** INFO: Parameter - ExportPath: $ExportPath" $False
        LogActivity "** INFO: Parameter - FileNameDelimiter: $FileNameDelimiter" $False
        LogActivity "** INFO: Parameter - UsePublicIPs: $UsePublicIPs" $False
        LogActivity "** INFO: Parameter - DebugMode: $DebugMode" $False

        ## Check AWS CLI Version installed
        TRY {
            TRY {
                $awsVersion = aws --version 2>$null
            } CATCH {
                IF (!([string]::IsNullOrWhiteSpace($awsVersion))) {
                    IF ($awsVersion -like 'aws-cli/1.*') {
                        LogActivity "** INFO: AWS CLI v1.x Detected: Version $awsVersion" $False
                    } ELSEIF ($awsVersion -like 'aws-cli/2.*') {
                        LogActivity "** INFO: AWS CLI v2.x Detected: Version $awsVersion" $False
                    } ELSE {
                        LogActivity "** ERROR: Required - AWS CLI Not Found or Accessible: $ErrorMsg" $True
                        Exit_Script -ErrorRaised $True
                    }
                } Else {
                    IF (InternetCheck) {
                        IF ($IsWindows) {  
                            LogActivity "** INFO: AWS CLI Not Installed - this is a Required Object" $True
                            LogActivity "** INFO: Beginning AWS CLI Install" $True
                            
                            $installerUrl = "https://awscli.amazonaws.com/AWSCLIV2.msi"
                            $installerPath = "$ScriptRoot\AWSCLIV2.msi"

                            Invoke-WebRequest -Uri $installerUrl -OutFile $installerPath
                            Start-Process msiexec.exe -Wait -ArgumentList "/i `"$installerPath`" /qn"
                            Remove-Item $installerPath -Force
                            LogActivity "** INFO: AWS CLI Install Complete - Please Restart Session" $True
                            Exit_Script -ErrorRaised $False
                        } ElseIf ($IsMacOS) {
                            LogActivity "** INFO: AWS CLI Not Installed - this is a Required Object" $True
                            LogActivity "** INFO: Beginning AWS CLI Install" $True   

                            curl "https://awscli.amazonaws.com/AWSCLIV2.pkg" -o "AWSCLIV2.pkg"
                            sudo installer -pkg AWSCLIV2.pkg -target /
                            LogActivity "** INFO: AWS CLI Install Complete - Please Restart Session" $True
                            Exit_Script -ErrorRaised $False                        
                        }
                    } 
                    Else {
                        LogActivity "** ERROR: Unable to Complete AWS CLI Install due to no Internet Access - Please Manually Install AWS CLI" $True
                        Exit_Script -ErrorRaised $True                              
                    }                    
                }
            }
        } CATCH {
            IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }             
            LogActivity "** ERROR: Required - AWS CLI Not Found or Accessible: $ErrorMsg" $True
            Exit_Script -ErrorRaised $True
        }

        $ConfigFile = FormatString -InputString $("$DefaultScriptRoot\config.txt")

        IF (!(test-path $ConfigFile)) {     
            LogActivity "** INFO: Config File Not Found : Creating : $ConfigFile" $True
            Add-content $ConfigFile -value "CustomerName:"
            Add-content $ConfigFile -value "region:"
            Add-content $ConfigFile -value "server:"
            Add-content $ConfigFile -value "instance:"
            Add-content $ConfigFile -value "rds:"
            Add-content $ConfigFile -value "fsx:"
            Add-content $ConfigFile -value "database:"
        }

        foreach($line in [System.IO.File]::ReadLines($ConfigFile)) {
	        IF (!([string]::IsNullOrWhiteSpace($line.Split(':')[1]))) { 
		        switch ($line.Split(':')[0]) {
			        "server"       { [System.Array]$EC2Servers = ($line.Split(':')[1].trim()).split(";") } 
			        "instance"     { [System.Array]$EC2InstanceIds = ($line.Split(':')[1].trim()).split(";") } 
			        "rds"          { [System.Array]$RdsInstances = ($line.Split(':')[1].trim()).split(";") }  
			        "fsx"          { [System.Array]$FSxFileSystems = ($line.Split(':')[1].trim()).split(";") } 
			        "customerName" {       [String]$Customer = ($line.Split(':')[1].trim()) } 
			        "region"       { [System.Array]$Regions = ($line.Split(':')[1].trim()).split(";") } 
                    "database"     { [System.Array]$DatabaseFilter = ($line.Split(':')[1].trim()).split(";") } 
			        default {
				        LogActivity "** ERROR: Invalid Configuration File Entry : $line" $True
				        Exit_Script -ErrorRaised $True
			        }
		        }
	        }   
        }

        ## Validate Resources Provided
        $Regions = $Regions | Sort-Object -Unique
        $EC2Servers = $EC2Servers | Sort-Object -Unique
        $EC2InstanceIds = $EC2InstanceIds | Sort-Object -Unique
        $RdsInstances = $RdsInstances | Sort-Object -Unique
        $FSxFileSystems = $FSxFileSystems | Sort-Object -Unique
        $DatabaseFilter = $DatabaseFilter | Sort-Object -Unique
 
        IF ([string]::IsNullOrWhiteSpace($Customer)) {
            ## No Customer Name Provided
            LogActivity "** ERROR: No Customer Name Provided" $True
            Exit_Script -ErrorRaised $True
        } ElseIF ( ([string]::IsNullOrWhiteSpace($EC2Servers)) -and ([string]::IsNullOrWhiteSpace($EC2InstanceIds)) -and ([string]::IsNullOrWhiteSpace($RdsInstances)) -and ([string]::IsNullOrWhiteSpace($FSxFileSystems)) ) {
            ## No Resources Provided for Collection
            LogActivity "** ERROR: No Resources Provided for Collection" $True
            Exit_Script -ErrorRaised $True
        } ElseIf ( (!([string]::IsNullOrWhiteSpace($EC2InstanceIds))) -and ([string]::IsNullOrWhiteSpace($Regions)) ) {
            ## No Region Provided for EC2-InstanceIds Collection
            LogActivity "** ERROR: No Region Provided for EC2-InstanceIds Collection" $True
            Exit_Script -ErrorRaised $True
        } ElseIf ( (!([string]::IsNullOrWhiteSpace($RdsInstances))) -and ([string]::IsNullOrWhiteSpace($Regions)) ) {
            ## No Region Provided for RDS Instance Collection
            LogActivity "** ERROR: No Region Provided for RDS Instance Collection" $True
            Exit_Script -ErrorRaised $True
        } ElseIf ( (!([string]::IsNullOrWhiteSpace($FSxFileSystems))) -and ([string]::IsNullOrWhiteSpace($Regions)) ) {
            ## No Region Provided for FSx Storage Collection
            LogActivity "** ERROR: No Region Provided for FSx Storage Collection" $True
            Exit_Script -ErrorRaised $True
        } 

        ## Validate Parameter Combinations
        IF ($CleanUpEnvironment) {
            LogActivity "** INFO: CleanUpEnvironment Selected : Disabling all other collections " $True
            $CollectConnectionsOnly = $False
            $ExportDacPacs = $False
            $ExportDDL = $False
            $CollectTsqlData = $False
            $CollectCloudWatchData = $False
            $AMAOnly = $False
        } ElseIf ($CollectConnectionsOnly) {
            LogActivity "** INFO: CollectConnectionsOnly Selected : Disabling all other collections" $True
            $ExportDacPacs = $False
            $ExportDDL = $False
            $CollectTsqlData = $False
            $CollectCloudWatchData = $False
            $AMAOnly = $False  
        } ElseIf ($ValidateResourcesOnly) {
            LogActivity "** INFO: ValidateResourcesOnly Selected : Disabling all other collections" $True
            $ExportDacPacs = $False
            $ExportDDL = $False
            $CollectTsqlData = $False
            $CollectCloudWatchData = $False
            $AMAOnly = $False
        } ElseIf ($AMAOnly) {
            If ( ($AMAOnly) -and ([string]::IsNullOrWhiteSpace($RdsInstances)) -and ([string]::IsNullOrWhiteSpace($EC2InstanceIds)) ) {
                LogActivity "** ERROR: AMA Collection Requested But No RDS/EC2 Instances Provided" $True
                Exit_Script -ErrorRaised $True  
            }          
            LogActivity "** INFO: AMA Only Selected : Disabling all other collections" $True
            $ExportDacPacs = $false
            $ExportDDL = $True
            $CollectTsqlData = $False
            $CollectCloudWatchData = $False   
        } ElseIf ( ($CollectCloudWatchData) -and ( ([string]::IsNullOrWhiteSpace($FSxFileSystems)) -and ([string]::IsNullOrWhiteSpace($RdsInstances)) -and ([string]::IsNullOrWhiteSpace($EC2InstanceIds)) ) ) {
            LogActivity "** ERROR: CollectCloudWatchData Selected : No AWS Resources Provided" $True
            Exit_Script -ErrorRaised $True
        } ElseIf ( $UseSSOLogin -and (!([string]::IsNullOrWhiteSpace($AWSProfile))) ) {        
            LogActivity "** ERROR: SSO Login Selected but No Profile Provided" $True
            Exit_Script -ErrorRaised $True
        } ElseIf ( (!($CollectCloudWatchData)) -and (!($ExportDacPacs)) -and (!($ExportDDL)) -and (!($CollectTsqlData)) ) {
            LogActivity "** ERROR: No Collection Parameters Provided" $True
            Exit_Script -ErrorRaised $True
        }         

        ## Check AWS Profiles   
        TRY {
            IF ( (!([string]::IsNullOrWhiteSpace($AWSProfile))) -and (!($UseSSOLogin)) ) {
                [Bool]$ProfileFound = $False
                LogActivity "** INFO: Profile $AWSProfile Supplied: Validating" $False
                $storedCredentials = aws configure list-profiles 2>$null
                IF (!([string]::IsNullOrWhiteSpace($storedCredentials))) {
                    ForEach ($Credentials IN $storedCredentials) {
                        IF ($Credentials -ieq $AWSProfile) {
                            $AWSProfile = $Credentials ## Fix Case Sensitive Profile Name Requirement                            
                            $ProfileFound = $True
                        }
                    }
                } 

                IF ($ProfileFound) {
                    $env:AWS_PROFILE=$AWSProfile
                    IF(-not (aws configure get region --profile $AWSProfile)) {
                        $env:AWS_REGION='us-east-1'
                        LogActivity "** INFO: No Region Configured for Profile: $AWSProfile - Setting to us-east-1" $False    
                    }
                    $identity = aws sts get-caller-identity | ConvertFrom-JSON
                    IF ($identity) {
                        LogActivity "** INFO: Profile $AWSProfile Found" $False    
                        LogActivity "** INFO: Profile: User ID: $($identity.UserId)" $False 
                        LogActivity "** INFO: Profile: Account: $($identity.Account)" $False 
                        LogActivity "** INFO: Profile: ARN: $($identity.Arn)" $False 
                    } ELSE {
                        LogActivity "** ERROR: Get-STSCallerIdentity Failed" $True
                        Exit_Script -ErrorRaised $True
                    }
                } ELSE {
                    LogActivity "** ERROR: Profile $AWSProfile Not Configured Here" $True
                    Exit_Script -ErrorRaised $True
                }
            }
        } CATCH {
            IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }            
            LogActivity "** ERROR: Unable to Validate Profile : $AWSProfile : $ErrorMsg" $True
            Exit_Script -ErrorRaised $True
        }

        ## Use SSO Login  
        TRY {
            IF ( (!([string]::IsNullOrWhiteSpace($AWSProfile))) -and $UseSSOLogin ) {
                [Bool]$ProfileFound = $False
                LogActivity "** INFO: Profile $AWSProfile Supplied: Validating" $False
                $storedCredentials = aws configure list-profiles 2>$null
                IF (!([string]::IsNullOrWhiteSpace($storedCredentials))) {
                    ForEach ($Credentials IN $storedCredentials) {
                        IF ($Credentials -ieq $AWSProfile) {
                            $AWSProfile = $Credentials ## Fix Case Sensitive Profile Name Requirement 
                            $ProfileFound = $True
                        }
                    }
                } 
                IF ($ProfileFound) {
                    LogActivity "** INFO: Starting SSO Login Process" $True
                    aws configure sso --profile $AWSProfile
                    aws sso login --profile $AWSProfile                  
                    $identity = aws sts get-caller-identity | ConvertFrom-JSON
                    IF ($identity) {
                        LogActivity "** INFO: Profile $AWSProfile Found" $False    
                        LogActivity "** INFO: Profile: User ID: $($identity.UserId)" $False 
                        LogActivity "** INFO: Profile: Account: $($identity.Account)" $False 
                        LogActivity "** INFO: Profile: ARN: $($identity.Arn)" $False 
                    } ELSE {
                        LogActivity "** ERROR: Get-STSCallerIdentity Failed" $True
                        Exit_Script -ErrorRaised $True
                    }                    
                } ELSE {
                    LogActivity "** ERROR: Profile $AWSProfile Not Configured Here" $True
                    Exit_Script -ErrorRaised $True
                }
            }
        } CATCH {
            IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }            
            LogActivity "** ERROR: SSO Login Failed : $ErrorMsg" $True
            Exit_Script -ErrorRaised $True
        }

        ## Check SQL Server Credentials
        TRY {
            $whoami =  $SecureSqlPassword = $Null
            IF ([string]::IsNullOrWhiteSpace($SqlUser)) {
                $whoami = whoami
                LogActivity "** INFO: No SQL Credentials Provided. Enabling Pass Through Authentication: $whoami" $True
            } ElseIf (!([string]::IsNullOrWhiteSpace($SqlUser))) {
                IF ([string]::IsNullOrWhiteSpace($SqlPassword)) {
                    $SecureSqlPassword = Read-Host "Please enter password for $SqlUser" -AsSecureString
                    IF ([string]::IsNullOrWhiteSpace([Runtime.InteropServices.Marshal]::PtrToStringAuto([Runtime.InteropServices.Marshal]::SecureStringToBSTR($SecureSqlPassword)))) {
                        LogActivity "** ERROR: Blank Password Entered" $True
                        Exit_Script -ErrorRaised $True
                    } Else {
                        $SqlPassword = [Runtime.InteropServices.Marshal]::PtrToStringAuto([Runtime.InteropServices.Marshal]::SecureStringToBSTR($SecureSqlPassword))
                    }
                }
            } Else {
                LogActivity "** ERROR: Unable to identify credentials" $True
                Exit_Script -ErrorRaised $True        
            }
        } CATCH {
            IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
            LogActivity "** ERROR: Unable to identify credentials : $ErrorMsg" $True
            Exit_Script -ErrorRaised $True        
        }

        ## Global Objects
        $InstanceIdDetails = @{}
        $EC2ServerDetails = @{}
        $RdsDetails = @{}

        [System.Array]$global:AllServers = @()
        [System.Array]$global:AllServersByIP = @()

        $Customer = $Customer.replace(";","")

        IF (!(ValidateRegions $Regions)) {
            LogActivity "** ERROR: Region(s) Validation Failed" $True
            Exit_Script -ErrorRaised $True
        }

        IF (!(ValidateEC2Info $EC2InstanceIds)) {
            LogActivity "** ERROR: EC2 Instance(s) Validation Failed" $True
            Exit_Script -ErrorRaised $True
        }

        IF (!(ValidateRdsInfo $RdsInstances)) {
            LogActivity "** ERROR: RDS Instance(s) Validation Failed" $True
            Exit_Script -ErrorRaised $True
        }

        IF (!(validateFsxInfo $FSxFileSystems)) {
            LogActivity "** ERROR: FSx FileSystem(s) Validation Failed" $True
            Exit_Script -ErrorRaised $True
        }

        IF ( ($CollectConnectionsOnly) -or ($ExportDacPacs) -or ($ExportDDL) -or ($CollectTsqlData) -or ($CleanUpEnvironment) -or ($ValidateResourcesOnly) ) {
            IF (!(ValidateServerConnectivity $EC2InstanceIds $EC2Servers $RdsInstances )) {
                LogActivity "** ERROR: SQL Connectivity Test Failed" $True
                Exit_Script -ErrorRaised $True
            }
        } Else {
            LogActivity "** INFO: No TSQL Data Collection Specified : Skipping SQL Connectivity Test" $True
        }

        ## Parse Database Filters
        $TsqlFilter = ''
        $TsqlInclude = $Null
        $DatabaseTSQL = "SELECT [name] FROM [master].[sys].[databases] 
                    WHERE source_database_id IS NULL 
                        AND LOWER([name]) NOT IN('model','tempdb','master','msdb','distribution','ssisdb','rdsadmin','mva-data-collection') 
                        AND LOWER([name]) NOT LIKE '%reportserver%' 
                        AND state_desc = 'ONLINE' " 

        ForEach ($Filter in $DatabaseFilter) {
            IF (!([string]::IsNullOrWhiteSpace($Filter))) {  
                IF ($Filter -eq 'INCLUDE') {
                    $TsqlInclude = $True
                } ELSEIF ($Filter -eq 'EXCLUDE') {
                    $TsqlInclude = $False
                } ELSE {
                    $TsqlFilter = $TsqlFilter + "'" + $Filter + "',"
                }
            }
        }

        IF (!([string]::IsNullOrWhiteSpace($TsqlFilter))) {  
            $TsqlFilter = ("($TsqlFilter)").Replace(",)",")")
            IF (!([string]::IsNullOrWhiteSpace($TsqlInclude))) {  
                IF ($TsqlInclude) { 
                    $DatabaseTSQL = $DatabaseTSQL + " and [name] IN" + $TsqlFilter + " " 
                } ELSE { 
                    $DatabaseTSQL = $DatabaseTSQL + " and [name] NOT IN" + $TsqlFilter + " " 
                }
            } ELSE {
                LogActivity "** ERROR: Database Filter must be in format 'database:INCLUDE;mydb;' or 'database:EXCLUDE;mydb;'" $True
                Exit_Script -ErrorRaised $True                
            }
        } 
        $DatabaseTSQL = $DatabaseTSQL + " ORDER BY Name" 
        LogActivity "** INFO: Database Tsql Filter Created : $DatabaseTSQL" $False

        ## Install DacPac Utility
        IF ($ExportDacPacs -eq $True) { 
            TRY {
                $SqlpackagePath = FormatString -InputString $("$DefaultScriptRoot\Dacpac")

                IF (!(test-path $(FormatString -InputString $("$SqlpackagePath\sqlpackage.exe")))) {
                    Invoke-Expression "[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12"
                    IF (!(test-path $SqlpackagePath)) { New-Item -ItemType Directory -Force -Path $SqlpackagePath | Out-Null }

                    IF ($IsWindows) {
                        IF (InternetCheck) {
                            Invoke-WebRequest -Uri "https://aka.ms/sqlpackage-windows" -Outfile $(FormatString -InputString $("$SqlpackagePath\sqlpackage.zip"))
                            Expand-Archive -Path $(FormatString -InputString $("$SqlpackagePath\sqlpackage.zip")) -DestinationPath $(FormatString -InputString $("$SqlpackagePath\"))
                        } 
                        Else {
                            LogActivity "** ERROR: Unable to Download DacPac due to no Internet Access - Please manually stage sqlpackage.exe at $SqlpackagePath\" $True
                            Exit_Script -ErrorRaised $True                              
                        }
                    } ElseIf ($IsMacOS -or $IsLinux) {
                        LogActivity "** ERROR: DacPac Export on MacOS not supported - Please contact Evolve for assistance" $True
                        Exit_Script -ErrorRaised $True                        
                    }
                }

                Set-Location $SqlpackagePath
                $version = .\sqlpackage.exe -version
                IF (!([string]::IsNullOrWhiteSpace($version))) { 
                    LogActivity "** INFO: SqlPackage/DacPac Installed: Version $version" $False
                }
                Else {
                    LogActivity "** ERROR: Unable to Install SqlPackage.exe - DacPac being disabled" $True
                    $ExportDacPacs = $false
                }
            } CATCH {
                IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
                    LogActivity "** ERROR: InstallSqlPackage() : $ErrorMsg" $True
                    Exit_Script -ErrorRaised $True
            }

            ForEach ($Server in $global:AllServers)
            {
                IF (!([string]::IsNullOrWhiteSpace($Server))) { 
                    LogActivity "** INFO: Collecting SQL Schema Data: $Server" $True

                    ## Create Logging Directory
                    $FormattedServer = ( FormatServerName $Server )
                    $ExportPath = FormatString -InputString $("$ScriptRoot\Export\$datestamp\$FormattedServer\")
                    IF (!(test-path $ExportPath)) { New-Item -ItemType Directory -Force -Path $ExportPath | Out-Null }

                    TRY {
                        $result = (Invoke-Sql -ServerInstance $Server -Database 'master' -Query "SELECT SERVERPROPERTY('productversion') AS version")
                    } CATCH {
                        IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
                            LogActivity "** ERROR: Determing SQL Version : $ErrorMsg" $True
                        #Exit_Script -ErrorRaised $True
                    }
                    $SqlVersion = $result.version 

                    TRY {
                        $Databases = (Invoke-Sql -ServerInstance $Server -Database 'master' -Query $DatabaseTSQL)

                        ForEach ($Database in $Databases.name) {
                            IF (!([string]::IsNullOrWhiteSpace($Database))) {     
                                IF (!(IsAgSecondary $Server $Database)) {
                                    Set-Location $SqlpackagePath
                                    IF ( ContainsUserObjects $Server $Database ) {
                                        LogActivity "** INFO: Beginning Export : SQL Database Schema : $Database : $Server" $False
                                        $Output = $Null
                                        $SqlMajorVersion = ($SqlVersion).SubString(0,2)

                                        $DiagnosticsFile = FormatString -InputString $("$ExportPath\$Database$FileNameDelimiter"+"Diagnostic.log")
                                        $DiagnosticsSqlFile = FormatString -InputString $("$ExportPath\$Database$FileNameDelimiter"+"Diagnostic.sql.log")

                                        $TargetFile = FormatString -InputString $("$ExportPath\$Database$FileNameDelimiter$SqlMajorVersion.dacpac")
                                        $TargetSqlFile = FormatString -InputString $("$ExportPath\$Database$FileNameDelimiter$SqlMajorVersion.sql")

                                        If (!([string]::IsNullOrWhiteSpace($SqlUser))) {
                                            $Output = .\SqlPackage /Action:Extract /TargetFile:$TargetFile /DiagnosticsFile:$DiagnosticsFile /DiagnosticsLevel:Verbose /p:ExtractAllTableData=false /SourceEncryptConnection:False /SourceTrustServerCertificate:True /SourceServerName:$Server /SourceDatabaseName:$Database /p:CommandTimeout=60 /p:DatabaseLockTimeout=1 /p:LongRunningCommandTimeout=60 /SourceUser:$SqlUser /SourcePassword:$SqlPassword
                                           # $SqlOutput = .\SqlPackage /Action:Extract /TargetFile:$TargetSqlFile /DiagnosticsFile:$DiagnosticsSqlFile /DiagnosticsLevel:Verbose /p:ExtractTarget=File /p:ExtractAllTableData=false /SourceEncryptConnection:False /SourceTrustServerCertificate:True /SourceServerName:$Server /SourceDatabaseName:$Database /p:CommandTimeout=60 /p:DatabaseLockTimeout=1 /p:LongRunningCommandTimeout=60 /SourceUser:$SqlUser /SourcePassword:$SqlPassword         
                                        } Else {
                                            $Output = .\SqlPackage /Action:Extract /TargetFile:$TargetFile /DiagnosticsFile:$DiagnosticsFile /DiagnosticsLevel:Verbose /p:ExtractAllTableData=false  /SourceEncryptConnection:False /SourceTrustServerCertificate:True /SourceServerName:$Server /SourceDatabaseName:$Database /p:CommandTimeout=60 /p:DatabaseLockTimeout=1 /p:LongRunningCommandTimeout=60 
                                           # $SqlOutput = .\SqlPackage /Action:Extract /TargetFile:$TargetSqlFile /DiagnosticsFile:$DiagnosticsSqlFile /DiagnosticsLevel:Verbose /p:ExtractTarget=File /p:ExtractAllTableData=false /SourceEncryptConnection:False /SourceTrustServerCertificate:True /SourceServerName:$Server /SourceDatabaseName:$Database /p:CommandTimeout=60 /p:DatabaseLockTimeout=1 /p:LongRunningCommandTimeout=60 
                                        }

                                        $DacPacConsoleLog = FormatString -InputString $("$ExportPath\$Database$FileNameDelimiter"+"Console.log")
                                        IF ($Output -like '*Failed to generate SSPI context*') {
                                            $Output | Out-File -FilePath $DacPacConsoleLog
                                            LogActivity "** ERROR: Creating DacPac : Failed to generate SSPI context : $Database" $True
                                        } Else {
                                            $Output | Out-File -FilePath $DacPacConsoleLog
                                            LogActivity "** INFO: Export Complete : SQL Database Schema : $Database : $Server" $True
                                        }
                                    } Else {
                                        LogActivity "** INFO: Database $Database has no User Objects : Skipping DacPac Export" $False                                    
                                    }
                                }
                            }
                        }
                    } CATCH {
                        IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
                        LogActivity "** ERROR: Creating DacPac : $Database : $ErrorMsg" $True
                    }
                }
            }
        }

        IF ($ExportDDL -eq $True) {
            $DdlQueryTimeout = [Math]::Max([int]$SqlServerQueryTimeout, 1800)
            $DdlSummary = ExportDdlParallel -Servers @($global:AllServers) -DatabaseQuery $DatabaseTSQL -Threads $ExportDdlThreads -DatabaseThreads $ExportDdlDatabaseThreads -QueryTimeout $DdlQueryTimeout
        }

        IF ($CollectConnectionsOnly -eq $True) { 
            LoadTSqlArray 17
            CollectConnectionInfoOnly
        }

        IF ($CollectTsqlData -eq $True) {
            $PerfCache = @{}
            $PerfSummary1 = CollectPerfCountersParallel -Servers @($global:AllServers) -Sample 1 -Cache $PerfCache
            $TsqlSummary = CollectTsqlParallel -Servers @($global:AllServers) -DatabaseQuery $DatabaseTSQL -Threads $CollectTsqlThreads -DatabaseThreads $CollectTsqlDatabaseThreads
            $PerfSummary2 = CollectPerfCountersParallel -Servers @($PerfCache.Keys) -Sample 2 -Cache $PerfCache
            ## a server that could not be collected used to stop the run at once; now the other servers are collected first, then the run ends
            IF ($TsqlSummary.ServersFailed -gt 0 -or $TsqlSummary.ItemFailures -gt 0 -or $TsqlSummary.QueryFailures -gt 0 -or $PerfSummary1.Failed -gt 0 -or $PerfSummary2.Failed -gt 0) { Exit_Script -ErrorRaised $True }
        }

        ## Collect CloudWatch Data
        IF ($CollectCloudWatchData -eq $True) {

            $ExportPath = FormatString -InputString $("$ScriptRoot\Export\$datestamp\CloudWatch\")
            IF (!(test-path $ExportPath)) { New-Item -ItemType Directory -Force -Path $ExportPath | Out-Null }

            $oFileEC2 = FormatString -InputString $("$ExportPath\EC2~~~~CloudWatch~~~~MetricData.csv")
            IF (Test-Path $oFileEC2) { Remove-Item $oFileEC2 -Force }

            $oFileEBS = FormatString -InputString $("$ExportPath\EBS~~~~CloudWatch~~~~MetricData.csv")
            IF (Test-Path $oFileEBS) { Remove-Item $oFileEBS -Force }

            $oFileFSX = FormatString -InputString $("$ExportPath\FSX~~~~CloudWatch~~~~MetricData.csv")
            IF (Test-Path $oFileFSX) { Remove-Item $oFileFSX -Force }

            $oFileFSXVolume = FormatString -InputString $("$ExportPath\FSXVolume~~~~CloudWatch~~~~MetricData.csv")
            IF (Test-Path $oFileFSXVolume) { Remove-Item $oFileFSXVolume -Force }            
         
            $oFileRDS = FormatString -InputString $("$ExportPath\RDS~~~~CloudWatch~~~~MetricData.csv")
            IF (Test-Path $oFileRDS) { Remove-Item $oFileRDS -Force }

            '"InstanceId"|"InstanceName"|"InstanceType"|"TimeStamp"|"Metric"|"AvgValue"|"MaxValue"|"SumValue"' | Out-File $oFileEC2 -Append -encoding UTF8
            '"InstanceId"|"VolumeId"|"TimeStamp"|"Metric"|"AvgValue"|"MaxValue"|"SumValue"' | Out-File $oFileEBS -Append -encoding UTF8
            '"FileSystemId"|"FileSystemName"|"FileSystemType"|"StorageCapacity"|"StorageType"|"TimeStamp"|"Metric"|"AvgValue"|"MaxValue"|"SumValue"' | Out-File $oFileFSX -Append -encoding UTF8
            '"FileSystemId"|"VolumeId"|"VolumeType"|"TimeStamp"|"Metric"|"AvgValue"|"MaxValue"|"SumValue"' | Out-File $oFileFSXVolume -Append -encoding UTF8
            '"DBInstanceIdentifier"|"DBEngine"|"TimeStamp"|"Metric"|"AvgValue"|"MaxValue"|"SumValue"' | Out-File $oFileRDS -Append -encoding UTF8

            ## The AWS calls (one per metric per day) run in parallel (-CloudWatchThreads); resource discovery and the CSV / JSON layout are unchanged.
            $CwSummary = CollectCloudWatchParallel -Regions $Regions -EC2InstanceIds $EC2InstanceIds -FSxFileSystems $FSxFileSystems -RdsInstances $RdsInstances `
                -OutFiles @{ EC2 = $oFileEC2; EBS = $oFileEBS; FSX = $oFileFSX; FSXVolume = $oFileFSXVolume; RDS = $oFileRDS } -Period $CloudWatchCollectionPeriod -Threads $CloudWatchThreads -QueriesPerCall $CloudWatchQueriesPerCall
        }

        ## Remove Objects created by this Script
        IF ($CleanUpEnvironment -eq $True) { 
            CleanUpEnvironment
        }
        Exit_Script -ErrorRaised $False

    } CATCH {
        IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
        LogActivity "** ERROR: Main() : $ErrorMsg" $True
    }
}

TRY {

    Clear-Host
    $Error.Clear()

    Main -CollectConnectionsOnly $CollectConnectionsOnly        `
        -ExportDacPacs $ExportDacPacs                           `
        -ExportDDL $ExportDDL                                   `
        -ExportDdlThreads $ExportDdlThreads                     `
        -ExportDdlDatabaseThreads $ExportDdlDatabaseThreads     `
        -CollectTsqlThreads $CollectTsqlThreads                 `
        -CollectTsqlDatabaseThreads $CollectTsqlDatabaseThreads `
        -CloudWatchThreads $CloudWatchThreads                   `
        -CloudWatchQueriesPerCall $CloudWatchQueriesPerCall     `
        -CollectCloudWatchData $CollectCloudWatchData           `
        -CollectTsqlData $CollectTsqlData                       `
        -AMAOnly $AMAOnly                                       `
        -CleanUpEnvironment $CleanUpEnvironment                 `
        -SqlServerConnectionTimeout $SqlServerConnectionTimeout `
        -SqlServerQueryTimeout $SqlServerQueryTimeout           `
        -CloudWatchCollectionPeriod $CloudWatchCollectionPeriod `
        -IncludeAllMsgs $IncludeAllMsgs                         `
        -ValidateResourcesOnly $ValidateResourcesOnly           `
        -AWSProfile $AWSProfile                                 `
        -UseSSOLogin $UseSSOLogin                               `
        -SqlUser $SqlUser                                       `
        -SqlPassword $SqlPassword                               `
        -ExportPath $ExportPath                                 `
        -FileNameDelimiter $FileNameDelimiter                   `
        -UsePublicIPs $UsePublicIPs                             `
        -DebugMode $DebugMode 

    # Main -CollectConnectionsOnly $false `
    #     -ExportDacPacs $false `
    #     -ExportDDL $false `
    #     -ExportDdlThreads 4 `
    #     -ExportDdlDatabaseThreads 4 `
    #     -CollectTsqlThreads 4 `
    #     -CollectTsqlDatabaseThreads 4 `
    #     -CloudWatchThreads 4 `
    #     -CloudWatchQueriesPerCall 4 `
    #     -CollectCloudWatchData $false `
    #     -CollectTsqlData $false `
    #     -AMAOnly $false `
    #     -CleanUpEnvironment $false `
    #     -SqlServerConnectionTimeout 30 `
    #     -SqlServerQueryTimeout 300 `
    #     -CloudWatchCollectionPeriod 30 `
    #     -IncludeAllMsgs $false `
    #     -ValidateResourcesOnly $true `
    #     -AWSProfile '' `
    #     -UseSSOLogin $False `
    #     -SqlUser '' `
    #     -SqlPassword '' `
    #     -ExportPath '' `
    #     -FileNameDelimiter '~~~~' `
    #     -UsePublicIPs $false `
    #     -DebugMode $False

} CATCH {
    IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
    LogActivity "** ERROR: Main() : $ErrorMsg" $True
}
