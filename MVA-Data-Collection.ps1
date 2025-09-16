
##########################################################################################################################
#
# Author:  Phil Ekins - Director of Database Services
# Website: www.evolvecloudservices.com
# Email:   pekins@evolvecloudservices.com
#
# Version: 1.0.11
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
#   -ExportDacPacs {default is false}
#     This will create a schema only .dacpac for all user databases
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
#   -SqlServerConnectionTimeout {default is 5 seconds}
#     This may need to be adjusted for your workload
#   -SqlServerQueryTimeout {default is 300 seconds}
#     This may need to be adjusted for your workload
#   -IncludeAllMsgs {default is false}
#     Verbose Mode
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
#   ./MVA-Data-Collection.ps1 -CollectTsqlData -CollectCloudWatchData -ExportDacPacs -SqlUser 'myusername' -SqlPassword 'mypassword'
#   ./MVA-Data-Collection.ps1 -CollectCloudWatchData -ExportPath 'C:\Temp\'
#   ./MVA-Data-Collection.ps1 -CollectTsqlData -UseSSOLogin -AWSProfile 'MyProfileName'
#   ./MVA-Data-Collection.ps1 -CollectConnectionsOnly 
#   ./MVA-Data-Collection.ps1 -CleanUpEnvironment 
#
##########################################################################################################################

param( 
    [Parameter(Mandatory=$false)] [switch] $CollectConnectionsOnly = $false,
    [Parameter(Mandatory=$false)] [switch] $ExportDacPacs = $false,
    [Parameter(Mandatory=$false)] [switch] $CollectCloudWatchData = $false,
    [Parameter(Mandatory=$false)] [switch] $CollectTsqlData = $false,
    [Parameter(Mandatory=$false)] [switch] $CleanUpEnvironment = $false,
    [Parameter(Mandatory=$false)] [int]    $SqlServerConnectionTimeout = 5,   
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
    [Parameter(Mandatory=$false)] [switch] $DebugMode = $false
)
     
Function GetVersion()
{
    TRY {
     
        $Version = "1.0.11"

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

        IF ($Display -or $IncludeAllMsgs -or $OverrideDisplay) {
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
        Start-Sleep -Milliseconds 50 
    } CATCH {
        IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
        throw "** ERROR: Error writing to log file : $ErrorInfo : $ErrorMsg"
    }
}

Function ExportData()
{
    Param ( [string]$server, 
            [string]$tableDesc, 
            [String]$sql, 
            [String]$database = 'master'
    )
    TRY {
        $Delimiter = '|'

        $FileId = $tableDesc.split('~')[0]
        $tableDesc = $tableDesc.split('~')[1].replace("[","").Replace("]","").replace(".","_")
        $sql = $sql.replace("@@@@",$database)

        TRY {
            $path = FormatString -InputString $("$ExportPath\$FileId$FileNameDelimiter$database$FileNameDelimiter$tableDesc$FileNameDelimiter$datestamp.csv")
            (Invoke-Sql -ServerInstance $server -Database $database -Query $sql) | Export-Csv -Path ($Path) -Delimiter $Delimiter -NoTypeInformation
            LogActivity "** INFO: Exported $FileId$FileNameDelimiter$database$FileNameDelimiter$tableDesc : $server" $False
        } CATCH {
            IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
            LogActivity "** ERROR: Collecting $tableDesc : $ErrorMsg" $True
        }
    } CATCH {
        IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
        LogActivity "** ERROR: ExportData() : $ErrorMsg" $True
        Exit_Script -ErrorRaised $True        
    }
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
                    EXEC msdb.dbo.sp_delete_job @job_name= N'CollectConnections' 
                GO"

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
        $AllItems = Get-ChildItem -Path ( $(FormatSting -InputString "$ExportPath\Export")) -Recurse
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
        } ELSEIF (($SqlVersion).SubString(0,2) -in("16","17")) {  
            $global:TsqlInstance.Add("03.007~[sys].[databases]","SELECT @@SERVERNAME AS [SQLInstance], * FROM [master].[sys].[databases]")
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
            $global:TsqlInstance.Add("44.001~[sys_dm_os_performance_counters]","SELECT @@SERVERNAME AS SQLInstance, * FROM [sys].[dm_os_performance_counters]")   
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
            $global:TsqlInstance.Add("46.002~[sql_agent_job_info]","IF NOT EXISTS (SELECT 1 FROM [master].[sys].[databases] WHERE [name] = 'rdsadmin')
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
                    GO"
                        
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
                        IF ($ValidateResourcesOnly) { LogActivity "** INFO: Instance $instance has been validated" $True } ELSE { LogActivity "** INFO: Instance $instance has been validated" $False }
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
                    ## Locate Private IP for InstanceIDs
                    $DnsName = $output.Reservations.Instances.PrivateDnsName  
                    #$DnsName = $output.Reservations.Instances.PublicDnsName  

                    LogActivity "** INFO: Instance $instance Private DNS Name Found: $DnsName" $False
                
                    IF ($output.Reservations.Instances.tags.Key -eq "Name") {
                        $name = $output.Reservations.Instances.tags | Where-Object { $_.Key -eq "Name" } | Select-Object -expand Value
                    } Else { 
                        $name = ''
                    }
                    $InstanceIdDetails.Add($instance, "$DnsName|$name")
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
                        IF ( ($output | ConvertFrom-JSON).DBInstances.Engine -like "sqlserver*") { 
                            $DBInstanceIdentifier = ($output | ConvertFrom-JSON).DBInstances.DBInstanceIdentifier
                            $RdsEndPoint = ($output | ConvertFrom-JSON).DBInstances.Endpoint.Address
                            $RdsPort = ($output | ConvertFrom-JSON).DBInstances.Endpoint.Port
                            IF (!([string]::IsNullOrWhiteSpace($RdsEndPoint))) {
                                $RdsDetails.Add($DBInstanceIdentifier, "$RdsEndPoint|$RdsPort")
                                IF ($ValidateResourcesOnly) { LogActivity "** INFO: RDS Node $rds has been validated" $True } ELSE { LogActivity "** INFO: RDS Node $rds has been validated" $False }
                            } Else {
                                $Pass = $False
                                LogActivity "** ERROR: RDS Node $rds has failed validation : No Endpoint detected" $True
                            }
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
        $sql = "SELECT TOP 1 @@SERVERNAME AS SQLInstance, name FROM [master].[sys].[databases]"

        ForEach ($item in $EC2InstanceIds) {
            IF (!([string]::IsNullOrWhiteSpace($item))) {
                $output = ''
                TRY {
                    $output = (Invoke-Sql -ServerInstance ($InstanceIdDetails."$item").split('|')[0]  -Database 'master' -Query $sql) 
                    IF (!([string]::IsNullOrWhiteSpace($output))) { 
                        IF ($ValidateResourcesOnly) { LogActivity "** INFO: EC2 InstanceId $item SQL Connectivity has been validated" $True } ELSE { LogActivity "** INFO: EC2 InstanceId $item SQL Connectivity has been validated" $False }          
                        $global:AllServers += ($InstanceIdDetails."$item").split('|')[0]
                        $global:AllServersByIp += GetServerIP -Server ($InstanceIdDetails."$item").split('|')[0]
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
                    $output = (Invoke-Sql -ServerInstance $RdsDetails."$item".split("|")[0] -Database 'master' -Query $sql) 
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
                    $output = (Invoke-Sql -ServerInstance $item  -Database 'master' -Query $sql) 
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
            IF ( ($CollectConnectionsOnly) -or ($ExportDacPacs) -or ($CollectCloudWatchData) -or ($CollectTsqlData) ) {
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
        [Parameter(Mandatory=$false)] [String] $ServerInstance,
        [Parameter(Mandatory=$false)] [String] $Database = 'master',
        [Parameter(Mandatory=$false)] [String] $Query
     )
    TRY {
        $output = ''

        If (!([string]::IsNullOrWhiteSpace($SqlUser))) {
            $output = (Invoke-Sqlcmd -ServerInstance $ServerInstance -Database $Database -Query $Query -ConnectionTimeout $SqlServerConnectionTimeout -QueryTimeout $SqlServerQueryTimeout -ErrorAction Stop -Username $SqlUser -Password $SqlPassword)           
        } ELSE {
            $output = (Invoke-Sqlcmd -ServerInstance $ServerInstance -Database $Database -Query $Query -ConnectionTimeout $SqlServerConnectionTimeout -QueryTimeout $SqlServerQueryTimeout -ErrorAction Stop)
        }
        Return $output
    } CATCH {
        IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
        IF ($ErrorMsg -eq 'SQLServerAgent is not currently running so it cannot be notified of this action.') {
            LogActivity "** INFO: SQL Agent is not running on $ServerInstance : CollectConnections Job will not run" $True
        } Else {
            LogActivity "** ERROR: Invoke-Sql() : $ServerInstance : $ErrorMsg" $False
            Exit_Script -ErrorRaised $True
        }
    }
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
        Return $ContainsUserObjects
    } CATCH {
        IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
        LogActivity "** ERROR: ContainsUserObjects() : $ErrorMsg" $True
        Exit_Script -ErrorRaised $True
    }
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

Function ScriptVersionCheck()
{
    TRY {

        $Internet = $False

        $Url = "https://api.github.com/repos/evolvecloudservicesgit/mva-data-collection/releases/latest"

        TRY {
            $response = Invoke-WebRequest -Uri "http://www.msftconnecttest.com/connecttest.txt" -UseBasicParsing -TimeoutSec 5
            IF ($response.StatusCode -eq 200) {
                $Internet = $True
            }
        } CATCH {
            $Internet = $False
        }        

        IF ($Internet) {
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

Function Main 
{
    param( 
        [Parameter(Mandatory=$false)] [bool]   $CollectConnectionsOnly,
        [Parameter(Mandatory=$false)] [bool]   $ExportDacPacs,
        [Parameter(Mandatory=$false)] [bool]   $CollectCloudWatchData,
        [Parameter(Mandatory=$false)] [bool]   $CollectTsqlData,
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
        [Parameter(Mandatory=$false)] [bool]   $DebugMode 
    )

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
        LogActivity "** INFO: Parameter - CollectCloudWatchData: $CollectCloudWatchData" $False
        LogActivity "** INFO: Parameter - CollectTsqlData: $CollectTsqlData" $False
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
        LogActivity "** INFO: Parameter - DebugMode: $DebugMode" $False

        ## Check AWS CLI Version installed
        TRY {
            $awsVersion = aws --version 2>$null
            IF (!([string]::IsNullOrWhiteSpace($awsVersion))) {
                IF ($awsVersion -like 'aws-cli/1.*') {
                    LogActivity "** INFO: AWS CLI v1.x Detected: Version $awsVersion" $False
                } ELSEIF ($awsVersion -like 'aws-cli/2.*') {
                    LogActivity "** INFO: AWS CLI v2.x Detected: Version $awsVersion" $False
                } ELSE {
                    LogActivity "** ERROR: Required - AWS CLI Not Found or Accessible: $ErrorMsg" $True
                    Exit_Script -ErrorRaised $True
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
            $CollectTsqlData = $False
            $CollectCloudWatchData = $False
        } ElseIf ($CollectConnectionsOnly) {
            LogActivity "** INFO: CollectConnectionsOnly Selected : Disabling all other collections" $True
            $ExportDacPacs = $False
            $CollectTsqlData = $False
            $CollectCloudWatchData = $False    
        } ElseIf ($ValidateResourcesOnly) {
            LogActivity "** INFO: ValidateResourcesOnly Selected : Disabling all other collections" $True
            $ExportDacPacs = $False
            $CollectTsqlData = $False
            $CollectCloudWatchData = $False   
        } ElseIf ( ($CollectCloudWatchData) -and ( ([string]::IsNullOrWhiteSpace($FSxFileSystems)) -and ([string]::IsNullOrWhiteSpace($RdsInstances)) -and ([string]::IsNullOrWhiteSpace($EC2InstanceIds)) ) ) {
            LogActivity "** ERROR: CollectCloudWatchData Selected : No AWS Resources Provided" $True
            Exit_Script -ErrorRaised $True
        } ElseIf ( (!($CollectCloudWatchData)) -and (!($ExportDacPacs)) -and (!($CollectTsqlData)) ) {
            LogActivity "** ERROR: No Collection Parameters Provided" $True
            Exit_Script -ErrorRaised $True
        } ElseIf ( $UseSSOLogin -and (!([string]::IsNullOrWhiteSpace($AWSProfile))) ) {        
            LogActivity "** ERROR: SSO Login Selected but No Profile Provided" $True
            Exit_Script -ErrorRaised $True
        } ElseIf ( (!($CollectCloudWatchData)) -and (!($ExportDacPacs)) -and (!($CollectTsqlData)) ) {
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

        IF ( ($CollectConnectionsOnly) -or ($ExportDacPacs) -or ($CollectTsqlData) -or ($CleanUpEnvironment) -or ($ValidateResourcesOnly) ) {
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
                        and [name] NOT IN('model','tempdb','master','msdb','distribution','ssisdb','rdsadmin','MVA-Data-Collection') 
                        and [name] NOT LIKE '%ReportServer%' 
                        and state_desc = 'ONLINE' " 

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
                LogActivity "** ERROR: Database Filter must include INCLUDE or EXCLUDE tag" $True
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
                        Invoke-WebRequest -Uri "https://aka.ms/sqlpackage-windows" -Outfile $(FormatString -InputString $("$SqlpackagePath\sqlpackage.zip"))
                        Expand-Archive -Path $(FormatString -InputString $("$SqlpackagePath\sqlpackage.zip")) -DestinationPath $(FormatString -InputString $("$SqlpackagePath\"))
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
                                        $DiagnosticsFile = FormatString -InputString $("$ExportPath\$Database$FileNameDelimiter"+"DacPacDiagnostic.log")
                                        $TargetFile = FormatString -InputString $("$ExportPath\$Database$FileNameDelimiter$SqlMajorVersion.dacpac")
                                        If (!([string]::IsNullOrWhiteSpace($SqlUser))) {
                                            $Output = .\SqlPackage /Action:Extract /TargetFile:$TargetFile /DiagnosticsFile:$DiagnosticsFile /DiagnosticsLevel:Verbose /p:ExtractAllTableData=false /SourceEncryptConnection:False /SourceTrustServerCertificate:True /SourceServerName:$Server /SourceDatabaseName:$Database /p:CommandTimeout=60 /p:DatabaseLockTimeout=1 /p:LongRunningCommandTimeout=60 /SourceUser:$SqlUser /SourcePassword:$SqlPassword
                                        } Else {
                                            $Output = .\SqlPackage /Action:Extract /TargetFile:$TargetFile /DiagnosticsFile:$DiagnosticsFile /DiagnosticsLevel:Verbose /p:ExtractAllTableData=false  /SourceEncryptConnection:False /SourceTrustServerCertificate:True /SourceServerName:$Server /SourceDatabaseName:$Database /p:CommandTimeout=60 /p:DatabaseLockTimeout=1 /p:LongRunningCommandTimeout=60 
                                        }

                                        $DacPacConsoleLog = FormatString -InputString $("$ExportPath\$Database$FileNameDelimiter"+"DacPacConsole.log")
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

        ## Exports [dbo].[Connections], Updates Zip and Exits
        IF ($CollectConnectionsOnly -eq $True) { 
            LoadTSqlArray 17
            CollectConnectionInfoOnly
        }

        ## Collect Tsql Data
        IF ($CollectTsqlData -eq $True) {

            ForEach ($Server in $global:AllServers)
            {
                $sql = $SqlVersion = $ExportPath = $Databases = $null

                IF (!([string]::IsNullOrWhiteSpace($Server))) { 
                    LogActivity "** INFO: Collecting SQL Data: $Server" $True

                    TRY {
                        $result = (Invoke-Sql -ServerInstance $Server -Database 'master' -Query "SELECT SERVERPROPERTY('productversion') AS version")
                    } CATCH {
                        IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
                        LogActivity "** ERROR: Determing SQL Version : $ErrorMsg" $True
                        Exit_Script -ErrorRaised $True
                    }
                    $SqlVersion = $result.version 

                    IF (($SqlVersion).SubString(0,2) -notin("10","11","12","13","14","15","16","17")) {   ## SQL 2008 forward
                        LogActivity "** INFO: Unsupported SQL Version (Skipping Tsql data collection) : $Server : $SqlVersion" $True
                        #Exit_Script -ErrorRaised $True
                    } 
                    Else {
                        ## Create Logging Directory
                        $FormattedServer = ( FormatServerName $Server )
                        $ExportPath = FormatString -InputString $("$ScriptRoot\Export\$datestamp\$FormattedServer\")
                        IF (!(test-path $ExportPath)) { New-Item -ItemType Directory -Force -Path $ExportPath | Out-Null }
           
                        ## Loading Version Specific Tsql Scripts into Array
                        LoadTSqlArray $SqlVersion 

                        $global:TsqlInstance.GetEnumerator() | ForEach-Object {
                            ExportData $Server $_.Key $_.Value 
                        }

                        TRY {
                            [System.Array]$Databases = (Invoke-Sql -ServerInstance $Server -Database 'master' -Query $DatabaseTSQL)
                        } CATCH {
                            IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
                            LogActivity "** ERROR: Collecting database names : $ErrorMsg" $True
                            Exit_Script -ErrorRaised $True
                        }

                        ## Per Database Metrics
                        ForEach ($Database in $Databases.name) {
                            TRY {
                                IF (!(IsAgSecondary $Server $Database)) {
                                    $global:TsqlDatabase.GetEnumerator() | ForEach-Object {
                                        ExportData $Server $_.Key $_.Value $Database 
                                    }
                                }
                            } CATCH {
                                IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
                                LogActivity "** ERROR: Detecting Secondary Replica State : $Database : $ErrorMsg" $True
                                Exit_Script -ErrorRaised $True
                            }
                        }

                        ## SSRS Metrics
                        TRY {
                            $sql = "SELECT [name] FROM [master].[sys].[databases] WHERE [name] LIKE '%ReportServer%' AND NOT [name] LIKE '%TempDB' AND state_desc = 'ONLINE'"
                            [System.Array]$SSRSDatabases = (Invoke-Sql -ServerInstance $Server -Database 'master' -Query $sql) 

                            ForEach ($Database in $SSRSDatabases.name) {
                                IF (!(IsAgSecondary $Server $Database)) {
                                    $global:TsqlSsrsDatabases.GetEnumerator() | ForEach-Object {
                                        ExportData $Server $_.Key $_.Value $Database 
                                    }
                                }
                            }
                        } CATCH {
                            IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
                            LogActivity "** ERROR: Collecting SSRS Metrics : $ErrorMsg" $True
                            Exit_Script -ErrorRaised $True
                        }

                        ## SSIS Metrics
                        TRY {
                            $sql = "SELECT [name] FROM [master].[sys].[databases] WHERE [name] = 'SSISDB' AND state_desc = 'ONLINE'"
                            [System.Array]$SSISDatabase = (Invoke-Sql -ServerInstance $Server -Database 'master' -Query $sql) 

                            ForEach ($Database in $SSISDatabase.name) {
                                IF (!(IsAgSecondary $Server $Database)) {
                                    $global:TsqlSsisDatabases.GetEnumerator() | ForEach-Object {
                                        ExportData $Server $_.Key $_.Value $Database 
                                    }
                                }
                            }
                        } CATCH {
                            IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
                            LogActivity "** ERROR: Collecting SSIS Metrics : $ErrorMsg" $True
                            Exit_Script -ErrorRaised $True
                        }
                    }
                }
            }
        }

        ## Collect CloudWatch Data
        IF ($CollectCloudWatchData -eq $True) {

            $ExportPath = FormatString -InputString $("$ScriptRoot\Export\$datestamp\$FormattedServer\CloudWatch\")
            IF (!(test-path $ExportPath)) { New-Item -ItemType Directory -Force -Path $ExportPath | Out-Null }

            $oFileEC2 = FormatString -InputString $("$ExportPath\EC2~~~~CloudWatch~~~~MetricData.csv")
            IF (Test-Path $oFileEC2) { Remove-Item $oFileEC2 -Force }

            $oFileEBS = FormatString -InputString $("$ExportPath\EBS~~~~CloudWatch~~~~MetricData.csv")
            IF (Test-Path $oFileEBS) { Remove-Item $oFileEBS -Force }

            $oFileFSX = FormatString -InputString $("$ExportPath\FSX~~~~CloudWatch~~~~MetricData.csv")
            IF (Test-Path $oFileFSX) { Remove-Item $oFileFSX -Force }

            $oFileRDS = FormatString -InputString $("$ExportPath\RDS~~~~CloudWatch~~~~MetricData.csv")
            IF (Test-Path $oFileRDS) { Remove-Item $oFileRDS -Force }

            '"InstanceId"|"InstanceName"|"InstanceType"|"TimeStamp"|"Metric"|"AvgValue"|"MaxValue"|"SumValue"' | Out-File $oFileEC2 -Append -encoding UTF8
            '"InstanceId"|"VolumeId"|"TimeStamp"|"Metric"|"AvgValue"|"MaxValue"|"SumValue"' | Out-File $oFileEBS -Append -encoding UTF8
            '"FileSystemId"|"FileSystemName"|"FileSystemType"|"StorageCapacity"|"StorageType"|"TimeStamp"|"Metric"|"AvgValue"|"MaxValue"|"SumValue"' | Out-File $oFileFSX -Append -encoding UTF8
            '"DBInstanceIdentifier"|"DBEngine"|"TimeStamp"|"Metric"|"AvgValue"|"MaxValue"|"SumValue"' | Out-File $oFileRDS -Append -encoding UTF8

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

                                $oFileEC2Config = FormatString -InputString $("$ScriptRoot\Export\$datestamp\$FormattedServer\EC2-$instanceId.json")
                                ($EC2.Instances | ConvertTo-JSON -Depth 5) | Out-File $oFileEC2Config -encoding UTF8

                                $insTags = $EC2.Instances.Tags
                                $insType = ($EC2.Instances.InstanceType)
                                IF ($insTags.Key -eq "Name") { 
                                    $instanceName =  $insTags | Where-Object { $_.Key -eq "Name" } | Select-Object -expand Value
                                }

                                $EC2MetricsMaxAvg = ("CPUUtilization").split(",")
                                ForEach ($Ec2MetricAvg in $EC2MetricsMaxAvg) {
                                    [datetime]$eTime = Get-Date
                                    $metric = ($Ec2MetricAvg).Trim()
                                    ForEach ($i in 0..$CloudWatchCollectionPeriod) {
                                        [datetime]$eTime = (Get-Date).AddDays(($i*-1))
                                        [datetime]$sTime = $eTime.AddDays(-1)
                                        $Data = (aws cloudwatch get-metric-statistics --namespace AWS/EC2 --metric-name $metric --dimensions "Name=InstanceId,Value=$instanceId" --start-time $sTime --end-time $eTime --period 300 --statistics Average Maximum --region $region) | ConvertFrom-JSON
                                        ForEach($d in $Data.Datapoints){
                                            $timeStamp = ""
                                            $timeStamp = $d.TimeStamp
                                            $AvgValue = $d.Average
                                            $MaxValue = $d.Maximum
                                            '"'+$instanceId+'"|"'+$instanceName+'"|"'+$insType+'"|"'+$timeStamp+'"|"'+$metric+'"|"'+$AvgValue+'"|"'+$MaxValue+'"|"'+0+'"' | Out-File $oFileEC2 -Append -encoding UTF8
                                        }
                                    }
                                    LogActivity "** INFO: Exported EC2 CW Metric $metric : $instanceId" $False
                                }
                        
                                $EC2MetricsSum = ("EBSReadOps, EBSWriteOps, EBSReadBytes, EBSWriteBytes").split(",")
                                ForEach ($EC2MetricSum in $EC2MetricsSum) {
                                    [datetime]$eTime =  Get-Date
                                    $metric = ($EC2MetricSum).Trim()
                                    ForEach ($i in 0..$CloudWatchCollectionPeriod) {
                                        [datetime]$eTime = (Get-Date).AddDays(($i*-1))
                                        [datetime]$sTime = $eTime.AddDays(-1)
                                        $Data = (aws cloudwatch get-metric-statistics --namespace AWS/EC2 --metric-name $metric --dimensions "Name=InstanceId,Value=$instanceId" --start-time $sTime --end-time $eTime --period 300 --statistics Sum --region $region) | ConvertFrom-JSON
                                        ForEach($d in $Data.Datapoints){
                                            $timeStamp = ""
                                            $timeStamp = $d.TimeStamp
                                            $SumValue = $d.Sum
                                            '"'+$instanceId+'"|"'+$instanceName+'"|"'+$insType+'"|"'+$timeStamp+'"|"'+$metric+'"|"'+0+'"|"'+0+'"|"'+$SumValue+'"' | Out-File $oFileEC2 -Append -encoding UTF8
                                        }
                                    }
                                    LogActivity "** INFO: Exported EC2 CW Metric $metric : $instanceId" $False
                                }
                    
                                $EC2MetricsMax = ("NetworkIn, NetworkOut, NetworkPacketsIn, NetworkPacketsOut").split(",")
                                ForEach ($EC2MetricMax in $EC2MetricsMax) {
                                    [datetime]$eTime =  Get-Date
                                    $metric = ($EC2MetricMax).Trim()
                                    ForEach ($i in 0..$CloudWatchCollectionPeriod) {
                                        [datetime]$eTime = (Get-Date).AddDays(($i*-1))
                                        [datetime]$sTime = $eTime.AddDays(-1)
                                        $Data = (aws cloudwatch get-metric-statistics --namespace AWS/EC2 --metric-name $metric --dimensions "Name=InstanceId,Value=$instanceId" --start-time $sTime --end-time $eTime --period 300 --statistics Maximum --region $region) | ConvertFrom-JSON
                                        ForEach($d in $Data.Datapoints){
                                            $timeStamp = ""
                                            $timeStamp = $d.TimeStamp
                                            $MaxValue = $d.Maximum
                                            '"'+$instanceId+'"|"'+$instanceName+'"|"'+$insType+'"|"'+$timeStamp+'"|"'+$metric+'"|"'+0+'"|"'+$MaxValue+'"|"'+0+'"' | Out-File $oFileEC2 -Append -encoding UTF8
                                        }
                                    }
                                    LogActivity "** INFO: Exported EC2 CW Metric $metric : $instanceId" $False
                                }
        
                                $EBS = $Null
                                $EBS = (aws ec2 describe-volumes --filters "Name=attachment.instance-id,Values=$instanceId" --region $region | ConvertFrom-JSON) #| Where-object {$_.Volumes.Attachments.InstanceId -eq $instanceId}
                                ForEach ($volume in $EBS.Volumes) {
                                    $VolumeId = $volume.Attachments.VolumeId

                                    $oFileEBSConfig = FormatString -InputString $("$ScriptRoot\Export\$datestamp\$FormattedServer\$VolumeId.json")
                                    ($volume | ConvertTo-JSON -Depth 4) | Out-File $oFileEBSConfig -encoding UTF8    

                                    $EBSMetricsSum = ("VolumeReadOps, VolumeWriteOps, VolumeReadBytes, VolumeWriteBytes").split(",")
                                    ForEach ($EBSMetricSum in $EBSMetricsSum) {
                                        [datetime]$eTime = Get-Date
                                        $metric = ($EBSMetricSum).Trim()
                                        ForEach ($i in 0..$CloudWatchCollectionPeriod) {
                                            [datetime]$eTime = (Get-Date).AddDays(($i*-1))
                                            [datetime]$sTime = $eTime.AddDays(-1)
                                            $Data = (aws cloudwatch get-metric-statistics --namespace AWS/EBS --metric-name $metric --dimensions "Name=VolumeId,Value=$($volume.Attachments.VolumeId)" --start-time $sTime --end-time $eTime --period 300 --statistics Sum --region $region) | ConvertFrom-JSON
                                            ForEach($d in $Data.Datapoints){
                                                $timeStamp = ""
                                                $timeStamp = $d.TimeStamp
                                                $SumValue = $d.Sum
                                                '"'+$instanceId+'"|"'+$VolumeId+'"|"'+$timeStamp+'"|"'+$metric+'"|"'+0+'"|"'+0+'"|"'+$SumValue+'"' | Out-File $oFileEBS -Append -encoding UTF8
                                            }
                                        }
                                        LogActivity "** INFO: Exported EBS CW Metric $metric : $instanceId : $VolumeId" $False
                                    }
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

            ## FSx Cloudwatch Data Collection
            #1st Gen - one dimension, FileSystemId
            #2nd Gen -  two dimensions, FileSystemId and FileServer
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
                            $FSxSystems = aws fsx describe-file-systems --file-system-ids @($FSXFileSystemArray) --region $region | ConvertFron-JSON -Depth 5

                            ForEach ($FSx in $FSxSystems) {
                                $FileSystemId = $FileSystemType = $StorageCapacity = $StorageType = $Tags = $FileSystemName = ""
                                $FileSystemId = $FSx.FileSystemId
                                $FileSystemType = $FSx.FileSystemType
                                $StorageCapacity = $FSx.StorageCapacity
                                $StorageType = $FSx.StorageType
                                $Tags = $FSx.Tags
                                $FSxWindowsConfig = $FSx.WindowsConfiguration | ConvertTo-JSON
                                $FSxOntapConfig = $FSx.OntapConfiguration | ConvertTo-JSON

                                if($Tags.Key -eq "Name"){$FileSystemName =  $Tags | Where-Object { $_.Key -eq "Name" } | Select-Object -expand Value}
        
                                Switch ($FileSystemType) {
                                    "Windows" {
                                        $oFileFSxConfig = FormatString -InputString $("$ScriptRoot\Export\$datestamp\$FormattedServer\FSx-$FileSystemId.json")
                                        $FSxWindowsConfig | Out-File $oFileFSxConfig -encoding UTF8

                                        $FSxMetricsSum = ("DataReadBytes, DataWriteBytes, DataWriteOperations, DataReadOperations, MetadataOperations").split(",")
                                        ForEach ($FSxMetricSum in $FSxMetricsSum) {
                                            [datetime]$eTime =  Get-Date
                                            $metric = ($FSxMetricSum).Trim()
                                            ForEach ($i in 0..$CloudWatchCollectionPeriod) {
                                                [datetime]$eTime =  (Get-Date).AddDays(($i*-1))
                                                [datetime]$sTime =  $eTime.AddDays(-1)
                                                $Data = (aws cloudwatch get-metric-statistics --namespace AWS/FSx --metric-name $metric --dimensions "Name=FileSystemId,Value=$FileSystemId" --start-time $sTime --end-time $eTime --period 300 --statistics Sum --region $region) | ConvertFrom-JSON
                                                ForEach($d in $Data.Datapoints) {
                                                    $timeStamp = ""
                                                    $timeStamp = $d.TimeStamp
                                                    $SumValue = $d.Sum
                                                    '"'+$FileSystemId+'"|"'+$FileSystemName+'"|"'+$FileSystemType+'"|"'+$StorageCapacity+'"|"'+$StorageType+'"|"'+$timeStamp+'"|"'+$metric+'"|"'+0+'"|"'+0+'"|"'+$SumValue+'"' | Out-File $oFileFSX -Append -encoding UTF8
                                                }
                                            }
                                            LogActivity "** INFO: Exported FSx CW Metric $metric : $FileSystemId" $False
                                        }

                                        $FSxMetricsMaxAvg = ("ClientConnections").split(",")
                                        ForEach ($FSxMetricMaxAvg in $FSxMetricsMaxAvg) {
                                            [datetime]$eTime =  Get-Date
                                            $metric = ($FSxMetricMaxAvg).Trim()
                                            ForEach ($i in 0..$CloudWatchCollectionPeriod) {
                                                [datetime]$eTime =  (Get-Date).AddDays(($i*-1))
                                                [datetime]$sTime =  $eTime.AddDays(-1)
                                                $Data = (aws cloudwatch get-metric-statistics --namespace AWS/FSx --metric-name $metric --dimensions "Name=FileSystemId,Value=$FileSystemId" --start-time $sTime --end-time $eTime --period 300 --statistics Average Maximum --region $region) | ConvertFrom-JSON
                                                ForEach($d in $Data.Datapoints) {
                                                    $timeStamp = ""
                                                    $timeStamp = $d.TimeStamp
                                                    $AvgValue = $d.Average
                                                    $MaxValue = $d.Maximum
                                                    '"'+$FileSystemId+'"|"'+$FileSystemName+'"|"'+$FileSystemType+'"|"'+$StorageCapacity+'"|"'+$StorageType+'"|"'+$timeStamp+'"|"'+$metric+'"|"'+$AvgValue+'"|"'+$MaxValue+'"|"'+0+'"' | Out-File $oFileFSX -Append -encoding UTF8
                                                }
                                                LogActivity "** INFO: Exported FSx CW Metric $metric : $FileSystemId" $False
                                            }
                                        }
                                    }
                                    "ONTAP" {
                                        $oFileFSxConfig = FormatString -InputString $("$ScriptRoot\Export\$datestamp\$FormattedServer\FSx-$FileSystemId.json")
                                        $FSxOntapConfig | Out-File $oFileFSxConfig -encoding UTF8
                
                                        IF (($FSxOntapConfig | ConvertFrom-JSON).DeploymentType.Value -like "*AZ_1") {
                                            ## Gen 1 Ontap
                                            $FSxGen = 1
                                        } Else {
                                            ## Gen 2 Ontap
                                            $FSxGen = 2
                                        }
                
                                        $FSxOntapConfig
                                        $FSxMetricsSum = ("NetworkSentBytes,NetworkReceivedBytes,DataReadBytes,DataWriteBytes,DataReadOperations,DataWriteOperations,MetadataOperations,DataReadOperationTime,DataWriteOperationTime,DiskReadBytes,DiskWriteBytes,DiskReadOperations,DiskWriteOperation").split(",")
                                        ForEach ($FSxMetricSum in $FSxMetricsSum) {
                                            [datetime]$eTime =  Get-Date
                                            $metric = ($FSxMetricSum).Trim()
                                            ForEach ($i in 0..$CloudWatchCollectionPeriod) {
                                                [datetime]$eTime =  (Get-Date).AddDays(($i*-1))
                                                [datetime]$sTime =  $eTime.AddDays(-1)
                                                $Data = (aws cloudwatch get-metric-statistics --namespace AWS/FSx --metric-name $metric --dimensions "Name=FileSystemId,Value=$FileSystemId" --start-time $sTime --end-time $eTime --period 300 --statistics Sum --region $region) | ConvertFrom-JSON
                                                ForEach($d in $Data.Datapoints) {
                                                    $timeStamp = ""
                                                    $timeStamp = $d.TimeStamp
                                                    $SumValue = $d.Sum
                                                    '"'+$FileSystemId+'"|"'+$FileSystemName+'"|"'+$FileSystemType+'"|"'+$StorageCapacity+'"|"'+$StorageType+'"|"'+$timeStamp+'"|"'+$metric+'"|"'+0+'"|"'+0+'"|"'+$SumValue+'"' | Out-File $oFileFSX -Append -encoding UTF8
                                                }
                                            }
                                            LogActivity "** INFO: Exported FSx CW Metric $metric : $FileSystemId" $False
                                        }
                
                                        $FSxMetricsMaxAvg = ("FileServerCacheHitRatio").split(",")
                                        ForEach ($FSxMetricMaxAvg in $FSxMetricsMaxAvg) {
                                            [datetime]$eTime =  Get-Date
                                            $metric = ($FSxMetricMaxAvg).Trim()
                                            ForEach ($i in 0..$CloudWatchCollectionPeriod) {
                                                [datetime]$eTime =  (Get-Date).AddDays(($i*-1))
                                                [datetime]$sTime =  $eTime.AddDays(-1)
                                                $Data = (aws cloudwatch get-metric-statistics --namespace AWS/FSx --metric-name $metric --dimensions "Name=FileSystemId,Value=$FileSystemId" --start-time $sTime --end-time $eTime --period 300 --statistics Average Maximum --region $region) | ConvertFrom-JSON
                                                ForEach($d in $Data.Datapoints) {
                                                    $timeStamp = ""
                                                    $timeStamp = $d.TimeStamp
                                                    $AvgValue = $d.Average
                                                    $MaxValue = $d.Maximum
                                                    '"'+$FileSystemId+'"|"'+$FileSystemName+'"|"'+$FileSystemType+'"|"'+$StorageCapacity+'"|"'+$StorageType+'"|"'+$timeStamp+'"|"'+$metric+'"|"'+$AvgValue+'"|"'+$MaxValue+'"|"'+0+'"' | Out-File $oFileFSX -Append -encoding UTF8
                                                }
                                            }
                                            LogActivity "** INFO: Exported FSx CW Metric $metric : $FileSystemId" $False
                                        }

                                        #Get-FSXVolume -FileSystemId $FileSystemId | ForEach-Object {   ## -VolumeId $FileSystemId
                                        #aws fsx describe-volumes --volume-ids fsvol-0123456789abcdef0
                                        #aws fsx describe-volumes --filters Name=file-system-id,Values=fs-0123456789abcdef0
                                        #
                                        #    $VolumeId = $_.VolumeId
                                        #
                                        #    $oFileFSxVolume = FormatString -InputString $("$($ExportPath)\FSx-$FileSystemId-$VolumeId.json")
                                        #    ($_.OntapConfiguration | ConvertTo-JSON) | Out-File $oFileFSxVolume -encoding UTF8
                                        #
                                        #    $VolumeDimension = New-Object Amazon.CloudWatch.Model.Dimension
                                        #    $VolumeDimension.set_Name("VolumeId")
                                        #    $VolumeDimension.set_Value($VolumeId)
                                        #
                                        #    $FSxVolumeMetricsSum = ("DataReadBytes,DataWriteBytes,DataReadOperations,DataWriteOperations,MetadataOperations,DataReadOperationTime,DataWriteOperationTime,MetadataOperationTime").split(",")
                                        #    $FSxVolumeMetricsSum | % {
                                        #        [datetime]$eTime =  Get-Date
                                        #        $metric = ($_).Trim()
                                        #        ForEach ($i in 0..$CloudWatchCollectionPeriod) {
                                        #            [datetime]$eTime =  (Get-Date).AddDays(($i*-1))
                                        #            [datetime]$sTime =  $eTime.AddDays(-1)
                                        #            #$Data = Get-CWMetricStatistic -Namespace AWS/FSx -MetricName $metric -StartTime $sTime -EndTime $eTime -Period 300 -Statistics @("Sum") -Dimensions @($dimension,$VolumeDimension) -Region $region
                                        #            $Data = (aws cloudwatch get-metric-statistics --namespace AWS/FSx --metric-name $metric --dimensions "Name=FileSystemId,Value=$FileSystemId" --start-time $sTime --end-time $eTime --period 300 --statistics Sum --region $region) | ConvertFrom-JSON
                                        #            ForEach($d in $Data.Datapoints) {
                                        #                $timeStamp = $utlValue = ""
                                        #                $timeStamp = $d.TimeStamp
                                        #                $SumValue = $d.Sum
                                        #              DElimuter  "$FileSystemId,$FileSystemName,$FileSystemType,$StorageCapacity,$StorageType,$timeStamp,$metric,0,0,$SumValue" | Out-File $oFileFSX -Append -encoding UTF8
                                        #            }
                                        #            LogActivity "** INFO: Exported FSx CW Metric $metric : $FileSystemId" $False
                                        #        }
                                        #    }
                                        #}
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

            ## RDS Cloudwatch Data Collection
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

                                    IF ($DBEngine -like "sqlserver*") {
                                        $oFileRDSConfig = FormatString -InputString $("$ScriptRoot\Export\$datestamp\$FormattedServer\RDS-$DBInstanceIdentifier.json")
                                        ($RDSInstance | ConvertTo-JSON -Depth 4) | Out-File $oFileRDSConfig -encoding UTF8

                                        IF (-not $RDSInstance.DBParameterGroups.DBParameterGroupName.StartsWith("default.")) {
                                            $DBParameterGroupName = $RDSInstance.DBParameterGroups.DBParameterGroupName
                                            $oFileCustomParameterGroup = FormatString -InputString $("$ScriptRoot\Export\$datestamp\$FormattedServer\CPG-$DBParameterGroupName.json")

                                            $CustomParameterGroup = aws rds describe-db-parameters --db-parameter-group-name DBParameterGroupName --region $region
                                            $CustomParameterGroup | Out-File $oFileCustomParameterGroup -encoding UTF8
                                        }

                                        $RDSMetricsSum = ("ReadThroughput,WriteThroughput,ReadIOPS,WriteIOPS,ReadLatency,WriteLatency").split(",")
                                        ForEach ($RDSMetricSum in $RDSMetricsSum) {
                                            [datetime]$eTime =  Get-Date
                                            $metric = ($RDSMetricSum).Trim()
                                            ForEach ($i in 0..$CloudWatchCollectionPeriod) {
                                                [datetime]$eTime =  (Get-Date).AddDays(($i*-1))
                                                [datetime]$sTime =  $eTime.AddDays(-1)
                                                $Data = (aws cloudwatch get-metric-statistics --namespace AWS/RDS --metric-name $metric --dimensions "Name=DBInstanceIdentifier,Value=$DBInstanceIdentifier" --start-time $sTime --end-time $eTime --period 300 --statistics Sum --region $region) | ConvertFrom-JSON
                                                ForEach($d in $Data.Datapoints){
                                                    $timeStamp = ""
                                                    $timeStamp = $d.TimeStamp
                                                    $SumValue = $d.Sum
                                                    '"'+$DBInstanceIdentifier+'"|"'+$DBEngine+'"|"'+$timeStamp+'"|"'+$metric+'"|"'+0+'"|"'+0+'"|"'+$SumValue+'"' | Out-File $oFileRDS -Append -encoding UTF8
                                                }
                                            }
                                            LogActivity "** INFO: Exported RDS CW Metric $metric : $DBInstanceIdentifier" $False
                                        }

                                        $RDSMetricsAvgMax = ("DatabaseConnections,CPUUtilization").split(",")
                                        ForEach ($RDSMetricAvgMax in $RDSMetricsAvgMax) {                                
                                            [datetime]$eTime =  Get-Date
                                            $metric = ($RDSMetricAvgMax).Trim()
                                            ForEach ($i in 0..$CloudWatchCollectionPeriod) {
                                                [datetime]$eTime =  (Get-Date).AddDays(($i*-1))
                                                [datetime]$sTime =  $eTime.AddDays(-1)
                                                $Data = (aws cloudwatch get-metric-statistics --namespace AWS/RDS --metric-name $metric --dimensions "Name=DBInstanceIdentifier,Value=$DBInstanceIdentifier" --start-time $sTime --end-time $eTime --period 300 --statistics Average Maximum --region $region) | ConvertFrom-JSON
                                                ForEach($d in $Data.Datapoints){
                                                    $timeStamp = ""
                                                    $timeStamp = $d.TimeStamp
                                                    $AvgValue = $d.Average
                                                    $MaxValue = $d.Maximum
                                                    '"'+$DBInstanceIdentifier+'"|"'+$DBEngine+'"|"'+$timeStamp+'"|"'+$metric+'"|"'+$AvgValue+'"|"'+$MaxValue+'"|"'+0+'"' | Out-File $oFileRDS -Append -encoding UTF8
                                                }
                                            }
                                            LogActivity "** INFO: Exported RDS CW Metric $metric : $DBInstanceIdentifier" $False
                                        }

                                        $RDSMetricsMax = ("NetworkReceiveThroughput,NetworkTransmitThroughput").split(",")
                                        ForEach ($RDSMetricMax in $RDSMetricsMax) {     
                                            [datetime]$eTime =  Get-Date
                                            $metric = ($RDSMetricMax).Trim()
                                            ForEach ($i in 0..$CloudWatchCollectionPeriod) {
                                                [datetime]$eTime =  (Get-Date).AddDays(($i*-1))
                                                [datetime]$sTime =  $eTime.AddDays(-1)
                                                $Data = (aws cloudwatch get-metric-statistics --namespace AWS/RDS --metric-name $metric --dimensions "Name=DBInstanceIdentifier,Value=$DBInstanceIdentifier" --start-time $sTime --end-time $eTime --period 300 --statistics Maximum --region $region) | ConvertFrom-JSON
                                                ForEach($d in $Data.Datapoints){
                                                    $timeStamp = ""
                                                    $timeStamp = $d.TimeStamp
                                                    $MaxValue = $d.Maximum
                                                    '"'+$DBInstanceIdentifier+'"|"'+$DBEngine+'"|"'+$timeStamp+'"|"'+$metric+'"|"'+0+'"|"'+$MaxValue+'"|"'+0+'"' | Out-File $oFileRDS -Append -encoding UTF8
                                                }
                                            }
                                            LogActivity "** INFO: Exported RDS CW Metric $metric : $DBInstanceIdentifier" $False
                                        }
                                    }
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
        -CollectCloudWatchData $CollectCloudWatchData           `
        -CollectTsqlData $CollectTsqlData                       `
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
        -DebugMode $DebugMode 

    # Main -CollectConnectionsOnly $False `
    #     -ExportDacPacs $False `
    #     -CollectCloudWatchData $False `
    #     -CollectTsqlData $False `
    #     -CleanUpEnvironment $False `
    #     -SqlServerConnectionTimeout 300 `
    #     -SqlServerQueryTimeout 5 `
    #     -CloudWatchCollectionPeriod 30 `
    #     -IncludeAllMsgs $False `
    #     -ValidateResourcesOnly $true `
    #     -AWSProfile '' `
    #     -UseSSOLogin $False `
    #     -SqlUser '' `
    #     -SqlPassword '' `
    #     -ExportPath '' `
    #     -FileNameDelimiter '~~~~' `
    #     -DebugMode $False

} CATCH {
    IF ($_.Exception.Message -eq '') { $ErrorMsg = $_ } else { $ErrorMsg = $_.Exception.Message }
    LogActivity "** ERROR: Main() : $ErrorMsg" $True
}
