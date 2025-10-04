USE [msdb]
GO

/****** Object:  Job [TalbotDataMasking]    Script Date: 18/07/2025 11:41:55 ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [[Uncategorized (Local)]]    Script Date: 18/07/2025 11:41:55 ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'TalbotDataMasking', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'No description available.', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'sa', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Setup DataMaskingDB]    Script Date: 18/07/2025 11:41:55 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Setup DataMaskingDB', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'-- Create Database for DataMasking
IF NOT EXISTS (Select * from sys.databases where name = ''DataMasking'')
BEGIN
CREATE DATABASE DataMasking COLLATE Latin1_General_CI_AS;
ALTER DATABASE DataMasking SET ANSI_NULLS ON;
ALTER DATABASE DataMasking SET QUOTED_IDENTIFIER ON;
END
GO

-- Create table to hold information for database, table and column for which we need to perform Datamasking
Use DataMasking
Go
IF NOT EXISTS (Select * from sys.objects where object_id = OBJECT_ID(N''[dbo].[DataMaskingInfo]'') and type in (N''U''))
BEGIN
CREATE TABLE DataMaskingInfo (
    DatabaseName NVARCHAR(128),
    SchemaName NVARCHAR(128),
    TableName NVARCHAR(128),
    ColumnName NVARCHAR(128),
    DataType NVARCHAR(50),
    PrimaryKeyColumn NVARCHAR(50),
    ThreadID INT,
     IsJSONColumn bit,
    JSONColumnKey nvarchar (MAX)
);
END

-- Create Audit Table if it doesn''t exist
Use DataMasking
Go
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = ''DataMaskingAudit'' AND schema_id = SCHEMA_ID(''dbo''))
BEGIN 
	CREATE TABLE Datamasking.dbo.DataMaskingAudit (
		AuditID INT IDENTITY (1,1) PRIMARY KEY,
		DatabaseName	NVARCHAR (128),
		SchemaName		NVARCHAR (128), 
		TableName		NVARCHAR (128), 
		ColumnName		NVARCHAR (128),
		AuditMessage	NVARCHAR(Max),
		AuditDateTime	DATETIME default GETDATE()
	);
END;

-- Create table to identify constraints such Foreign Key, Trigger and CDC on table columns which are candidate for DataMaksing
USE DataMasking;
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N''DataMasking.[dbo].[Constraints]'') AND type in (N''U''))
BEGIN
    CREATE TABLE DataMasking.dbo.Constraints (
        DatabaseName NVARCHAR(128),
        SchemaName NVARCHAR(128),
        TableName NVARCHAR(128),
		EnabledFKConstraints NVARCHAR(MAX), -- Stores comma-separated enabled FK constraint names
        EnabledTriggers NVARCHAR(MAX), -- Stores comma-separated enabled trigger names        
        IsCDCEnabled BIT -- Stores whether CDC was enabled (1) or not (0)
    );
END
GO', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Setup DataMasking StoredProcedure]    Script Date: 18/07/2025 11:41:55 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Setup DataMasking StoredProcedure', 
		@step_id=2, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'USE DataMasking;
GO

IF OBJECT_ID(''dbo.DataMaskingProcedure'', ''P'') IS NOT NULL
    DROP PROCEDURE dbo.DataMaskingProcedure;
GO
SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE dbo.DataMaskingProcedure
    @ThreadID INT,
    @DBName NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE 
        @SQL NVARCHAR(MAX),
        @DatabaseName SYSNAME,
        @SchemaName SYSNAME,
        @TableName SYSNAME,
        @ColumnName SYSNAME,
        @DataType SYSNAME,
        @RowsAffected INT,
        @AuditMessage NVARCHAR(MAX),
        @PrimaryKeyColumn SYSNAME,
        @IsJSONColumn BIT,
        @JSONColumnKey NVARCHAR(255);

    DECLARE cur CURSOR FOR
    SELECT 
        DatabaseName, SchemaName, TableName, ColumnName, DataType, 
        PrimaryKeyColumn, IsJSONColumn, JSONColumnKey
    FROM Datamasking.dbo.DataMaskingInfo
    WHERE ThreadID = @ThreadID AND DatabaseName = @DBName;

    OPEN cur;
    FETCH NEXT FROM cur INTO 
        @DatabaseName, @SchemaName, @TableName, @ColumnName, @DataType, 
        @PrimaryKeyColumn, @IsJSONColumn, @JSONColumnKey;

    WHILE @@FETCH_STATUS = 0
    BEGIN

		IF @IsJSONColumn = 1 AND @JSONColumnKey IS NOT NULL
		BEGIN
			-- JSON Key masking using simple alphanumeric pattern
			SET @SQL = ''
				WITH CTE AS (
					SELECT '' + QUOTENAME(@PrimaryKeyColumn) + '',
						   '' + QUOTENAME(@ColumnName) + '' AS OriginalValue,
						   JSON_VALUE('' + QUOTENAME(@ColumnName) + '', ''''$."'' + @JSONColumnKey + ''"'''') AS JsonKeyValue,
						   ''''XXXX_'''' + 
						   UPPER(LEFT(REPLACE(REPLACE(REPLACE(JSON_VALUE('' + QUOTENAME(@ColumnName) + '', ''''$."'' + @JSONColumnKey + ''"''''), '''' '''', ''''''''), ''''-'''', ''''''''), ''''.'''', ''''''''), 3)) +
						   RIGHT(''''000'''' + CAST(ABS(CHECKSUM(NEWID())) % 1000 AS VARCHAR), 3) AS MaskedValue
					FROM '' + QUOTENAME(@DatabaseName) + ''.'' + QUOTENAME(@SchemaName) + ''.'' + QUOTENAME(@TableName) + ''
					WHERE ISJSON('' + QUOTENAME(@ColumnName) + '') = 1
					  AND JSON_VALUE('' + QUOTENAME(@ColumnName) + '', ''''$."'' + @JSONColumnKey + ''"'''') IS NOT NULL
					  AND LTRIM(RTRIM(JSON_VALUE('' + QUOTENAME(@ColumnName) + '', ''''$."'' + @JSONColumnKey + ''"''''))) <> ''''''''
				)
				UPDATE T
				SET '' + QUOTENAME(@ColumnName) + '' = 
					REPLACE(CTE.OriginalValue,
							'' + ''''''"'' + @JSONColumnKey + ''":"'''''' + '' + CTE.JsonKeyValue + ''''"'''',
							'' + ''''''"'' + @JSONColumnKey + ''":"'''''' + '' + CTE.MaskedValue + ''''"'''')
				FROM '' + QUOTENAME(@DatabaseName) + ''.'' + QUOTENAME(@SchemaName) + ''.'' + QUOTENAME(@TableName) + '' T
				INNER JOIN CTE ON T.'' + QUOTENAME(@PrimaryKeyColumn) + '' = CTE.'' + QUOTENAME(@PrimaryKeyColumn) + '';
			'';
		END
        ELSE IF @DataType LIKE ''varchar%'' OR @DataType LIKE ''nvarchar%''
        BEGIN
            SET @SQL = ''
                WITH CTE AS (
                    SELECT '' + QUOTENAME(@PrimaryKeyColumn) + '',
                           '' + QUOTENAME(@ColumnName) + '',
                           ROW_NUMBER() OVER (PARTITION BY 1 ORDER BY '' + QUOTENAME(@PrimaryKeyColumn) + '') AS RN
                    FROM '' + QUOTENAME(@DatabaseName) + ''.'' + QUOTENAME(@SchemaName) + ''.'' + QUOTENAME(@TableName) + ''
                    WHERE '' + QUOTENAME(@ColumnName) + '' IS NOT NULL
                      AND LTRIM(RTRIM('' + QUOTENAME(@ColumnName) + '')) <> ''''''''
                      AND '' + QUOTENAME(@ColumnName) + '' NOT LIKE ''''%XXXX_%''''
                      AND '' + QUOTENAME(@ColumnName) + '' NOT IN (''''INVALID'''',''''UNCODED'''')
                )
                UPDATE T
                SET '' + QUOTENAME(@ColumnName) + '' = LEFT(CTE.'' + QUOTENAME(@ColumnName) + '', 3) + ''''XXXX_'''' + CONVERT(VARCHAR(20), LEFT(HASHBYTES(''''SHA1'''', CTE.'' + QUOTENAME(@ColumnName) + ''), 5))
                FROM '' + QUOTENAME(@DatabaseName) + ''.'' + QUOTENAME(@SchemaName) + ''.'' + QUOTENAME(@TableName) + '' T
                INNER JOIN CTE ON T.'' + QUOTENAME(@PrimaryKeyColumn) + '' = CTE.'' + QUOTENAME(@PrimaryKeyColumn) + ''
                OPTION (MAXDOP 4);'';
        END
        ELSE IF @DataType LIKE ''int''
        BEGIN
            SET @SQL = ''
                WITH CTE AS (
                    SELECT '' + QUOTENAME(@PrimaryKeyColumn) + '',
                           '' + QUOTENAME(@ColumnName) + '',
                           ROW_NUMBER() OVER (PARTITION BY 1 ORDER BY '' + QUOTENAME(@PrimaryKeyColumn) + '') AS RN
                    FROM '' + QUOTENAME(@DatabaseName) + ''.'' + QUOTENAME(@SchemaName) + ''.'' + QUOTENAME(@TableName) + ''
                    WHERE '' + QUOTENAME(@ColumnName) + '' IS NOT NULL
                      AND ISNUMERIC('' + QUOTENAME(@ColumnName) + '') = 1
                      AND LTRIM(RTRIM('' + QUOTENAME(@ColumnName) + '')) <> ''''''''
                      AND '' + QUOTENAME(@ColumnName) + '' NOT IN (''''INVALID'''',''''UNCODED'''')
                )
                UPDATE T
                SET '' + QUOTENAME(@ColumnName) + '' = CAST(FLOOR(RAND(CHECKSUM(NEWID())) * 100000) AS NVARCHAR(MAX))
                FROM '' + QUOTENAME(@DatabaseName) + ''.'' + QUOTENAME(@SchemaName) + ''.'' + QUOTENAME(@TableName) + '' T
                INNER JOIN CTE ON T.'' + QUOTENAME(@PrimaryKeyColumn) + '' = CTE.'' + QUOTENAME(@PrimaryKeyColumn) + ''
                OPTION (MAXDOP 4);'';
        END
        ELSE IF @DataType LIKE ''date%'' OR @DataType LIKE ''datetime%''
        BEGIN
            SET @SQL = ''
                WITH CTE AS (
                    SELECT '' + QUOTENAME(@PrimaryKeyColumn) + '',
                           '' + QUOTENAME(@ColumnName) + '',
                           ROW_NUMBER() OVER (PARTITION BY 1 ORDER BY '' + QUOTENAME(@PrimaryKeyColumn) + '') AS RN
                    FROM '' + QUOTENAME(@DatabaseName) + ''.'' + QUOTENAME(@SchemaName) + ''.'' + QUOTENAME(@TableName) + ''
                    WHERE '' + QUOTENAME(@ColumnName) + '' IS NOT NULL
                      AND LTRIM(RTRIM('' + QUOTENAME(@ColumnName) + '')) <> ''''''''
                      AND '' + QUOTENAME(@ColumnName) + '' NOT IN (''''INVALID'''',''''UNCODED'''')
                )
                UPDATE T
                SET '' + QUOTENAME(@ColumnName) + '' = CAST(DATEADD(DAY, -ABS(CHECKSUM(NEWID()) % 365), GETDATE()) AS NVARCHAR(MAX))
                FROM '' + QUOTENAME(@DatabaseName) + ''.'' + QUOTENAME(@SchemaName) + ''.'' + QUOTENAME(@TableName) + '' T
                INNER JOIN CTE ON T.'' + QUOTENAME(@PrimaryKeyColumn) + '' = CTE.'' + QUOTENAME(@PrimaryKeyColumn) + ''
                OPTION (MAXDOP 4);'';
        END

        EXEC sp_executesql @SQL;
		
        SET @RowsAffected = @@ROWCOUNT;

        SET @AuditMessage = CONCAT(''Masked '', CAST(@RowsAffected AS NVARCHAR), '' records.'');

        INSERT INTO DataMasking.dbo.DataMaskingAudit (
            DatabaseName, SchemaName, TableName, ColumnName, AuditMessage, AuditDateTime
        )
        VALUES (
            @DatabaseName, @SchemaName, @TableName, @ColumnName, @AuditMessage, GETDATE()
        );

        FETCH NEXT FROM cur INTO 
            @DatabaseName, @SchemaName, @TableName, @ColumnName, @DataType, 
            @PrimaryKeyColumn, @IsJSONColumn, @JSONColumnKey;
    END

    CLOSE cur;
    DEALLOCATE cur;
END;
GO

', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Setup DataMasking Orchestrator Stored Procedure]    Script Date: 18/07/2025 11:41:55 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Setup DataMasking Orchestrator Stored Procedure', 
		@step_id=3, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'-- Datamasking Orchestrator
USE DataMasking;
GO

IF OBJECT_ID (''dbo.DataMaskingOrchestrator'', ''P'') IS NOT NULL
DROP PROCEDURE IF EXISTS  dbo.DataMaskingOrchestrator;
GO
SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE  dbo.DataMaskingOrchestrator
	@DBName NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @ThreadID INT, 
			@DatabaseName NVARCHAR(MAX),
            @JobName NVARCHAR(100),
			@JobID UNIQUEIDENTIFIER,
            @SQL NVARCHAR(MAX),
			@JobDescription NVARCHAR(255);
	
	-- Table to track jobs
	DECLARE @JobTable TABLE (JobName NVARCHAR(100));

    -- Cursor to loop through distinct ThreadIDs
    DECLARE cur CURSOR FOR
    SELECT DISTINCT dmi.ThreadID, dmi.DatabaseName
	FROM DataMasking.dbo.DataMaskingInfo dmi
		INNER JOIN sys.databases sdb
			ON dmi.DatabaseName = sdb.name
			AND dmi.DatabaseName = @DBName;

    OPEN cur;
    FETCH NEXT FROM cur INTO @ThreadID, @DatabaseName;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Generate Unique Job Name
        SET @JobName = ''DataMasking_Thread_'' + CAST(@ThreadID AS NVARCHAR(10));
		SET @JobDescription  = N''DataMasking job for ThreadID '' + CAST(@ThreadID as NVARCHAR(10));

		-- Insert into tracking table
		INSERT INTO @JobTable VALUES (@JobName);

        -- **Check if Job Already Exists**
        IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WHERE name = @JobName)
        BEGIN
            -- **Delete Existing Job Steps (but keep the job)**
            EXEC msdb.dbo.sp_delete_jobstep @job_name = @JobName, @step_id = 0;
        END
        ELSE
        BEGIN
            -- **Create the Job Only If It Doesn''t Exist**
            EXEC msdb.dbo.sp_add_job 
                @job_name = @JobName, 
                @enabled = 1, 
                @notify_level_eventlog = 0, 
                @notify_level_email = 0, 
                @delete_level = 0, 
                @description = @JobDescription

            -- **Assign Job to SQL Server Agent**
            EXEC msdb.dbo.sp_add_jobserver 
                @job_name = @JobName, 
                @server_name = N''(local)'';
        END

        -- **Step 1: Run the DataMaskingProcedure inside DataMasking DB**
        SET @SQL = ''
        USE msdb; 
        EXEC msdb.dbo.sp_add_jobstep 
            @job_name = N'''''' + @JobName + '''''', 
            @step_name = N''''Execute DataMaskingProcedure'''', 
            @subsystem = N''''TSQL'''', 
            @database_name = N''''DataMasking'''', 
            @command = N''''USE DataMasking; EXEC dbo.DataMaskingProcedure '' + CAST(@ThreadID AS NVARCHAR(10)) + '','' + @DatabaseName +'''''', 
            @on_success_action = 1, 
            @on_fail_action = 2;'';

        -- Execute SQL to Add Job Step
        EXEC sp_executesql @SQL;

        -- **Start the Job**
        EXEC msdb.dbo.sp_start_job @job_name = @JobName;

        FETCH NEXT FROM cur INTO @ThreadID, @DatabaseName;
    END

    CLOSE cur;
    DEALLOCATE cur;

	-- Wait until All jobs complete
	DECLARE @RunningJobs INT;

	WHILE 1=1
	BEGIN
		-- Count Running Jobs
		SELECT @RunningJobs = COUNT(*)
		FROM msdb.dbo.sysjobs j
		INNER JOIN msdb.dbo.sysjobactivity ja ON j.job_id = ja.job_id 
		WHERE j.name IN (SELECT JobName FROM @JobTable)
			AND ja.stop_execution_date IS NULL -- Jobs still running
			--AND ja.start_execution_date IS NOT NULL; -- to avoid infinite loop

		-- Exit when no jobs are running
		IF @RunningJobs = 0
		BREAK;

		-- Wait for 5 seconds before checking again
		WAITFOR DELAY ''00:00:05'';
	END

END;
GO
', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Load DataMaskingInfo table]    Script Date: 18/07/2025 11:41:55 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Load DataMaskingInfo table', 
		@step_id=4, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'TRUNCATE TABLE DataMasking.dbo.DataMaskingInfo;

INSERT INTO DataMasking.dbo.DataMaskingInfo
  ( DatabaseName, SchemaName, TableName, ColumnName, DataType,PrimaryKeyColumn, ThreadID, IsJSONColumn ,JSONColumnKey)
VALUES
--(''Console'',''cache'',''LostAndLapsedPolicies'',''InsuredName'',''Nvarchar(max)'',''PolicyID'',1,0,NULL),
--(''Console'',''cache'',''RenewalPolicies'',''InsuredName'',''Nvarchar(max)'',''PolicyID'',1,0,NULL),
--(''Console'',''cache'',''Policies'',''InsdNm'',''Nvarchar(max)'',''PolID'',2,0,NULL),
--(''Console'',''cache'',''PolicyInsureds'',''InsdNm'',''Nvarchar(max)'',''Id'',3,0,NULL),
(''ProForma'',''dbo'',''Proformas'',''UnderwriterComments'',''Nvarchar(max)'',''Id'',1,0,NULL),
--(''ProForma'',''dbo'',''InsuredInformations'',''InsuredName'',''Nvarchar(max)'',''Id'',2,0,NULL),
--(''Spectrum'',''DigitalAssistant'',''DocumentSnapshot'',''SnapshotRequest'',''Nvarchar(max)'',''Id'',2,1,''InsuredName''),
--(''Spectrum'',''DigitalAssistant'',''DocumentSnapshot'',''Snapshot'',''Nvarchar(max)'',''Id'',2,1,''InsuredName''),
(''Lifecycle'',''chat'',''Posts'',''Message'',''Nvarchar(max)'',''Id'',1,0,NULL),
--(''Subscribe'',''dbo'',''PolMain'',''CtcNm'',''Nvarchar(max)'',''PolId'',1,0,NULL),
(''Subscribe'',''dbo'',''PolMain'',''Dsc'',''Nvarchar(max)'',''PolId'',1,0,NULL),
(''Subscribe'',''dbo'',''SCMMain'',''CurrNarrA'',''Nvarchar(max)'',''TrnId'',1,0,NULL),
(''Subscribe'',''dbo'',''SCMMain'',''CurrNarrB'',''Nvarchar(max)'',''TrnId'',1,0,NULL), 
--(''Subscribe'',''dbo'',''SCMMain'',''Insd'',''Nvarchar(max)'',''TrnId'',1,0,NULL),
(''Subscribe'',''dbo'',''SCMMain'',''ItrDsc'',''Nvarchar(max)'',''TrnId'',1,0,NULL),
(''Subscribe'',''dbo'',''SCMMain'',''LossLocn'',''Nvarchar(max)'',''TrnId'',1,0,NULL),
(''Subscribe'',''dbo'',''SCMMain'',''LossNarr1'',''Nvarchar(max)'',''TrnId'',1,0,NULL),
(''Subscribe'',''dbo'',''SCMMain'',''LossNarr2'',''Nvarchar(max)'',''TrnId'',1,0,NULL),
(''Subscribe'',''dbo'',''SCMMain'',''LossNarr3'',''Nvarchar(max)'',''TrnId'',1,0,NULL),
(''Subscribe'',''dbo'',''ImpADV'',''CurrNarrA'',''Nvarchar(max)'',''TrnId'',2,0,NULL),
(''Subscribe'',''dbo'',''ImpADV'',''CurrNarrB'',''Nvarchar(max)'',''TrnId'',2,0,NULL),
(''Subscribe'',''dbo'',''ImpADV'',''Insd'',''Nvarchar(max)'',''TrnId'',2,0,NULL),
(''Subscribe'',''dbo'',''ImpADV'',''ItrDsc'',''Nvarchar(max)'',''TrnId'',2,0,NULL),
(''Subscribe'',''dbo'',''ImpADV'',''LossLocn'',''Nvarchar(max)'',''TrnId'',2,0,NULL),
(''Subscribe'',''dbo'',''ImpADV'',''LossNarr1'',''Nvarchar(max)'',''TrnId'',2,0,NULL),
(''Subscribe'',''dbo'',''ImpADV'',''LossNarr2'',''Nvarchar(max)'',''TrnId'',2,0,NULL),
(''Subscribe'',''dbo'',''ImpADV'',''LossNarr3'',''Nvarchar(max)'',''TrnId'',2,0,NULL),
(''Subscribe'',''dbo'',''Insd'',''InsdAKA'',''Nvarchar(max)'',''InsdId'',3,0,NULL),
--(''Subscribe'',''dbo'',''Insd'',''InsdNm'',''Nvarchar(max)'',''InsdId'',3,0,NULL),
(''Subscribe'',''dbo'',''PolLyr'',''Dsc'',''Nvarchar(max)'',''PolID'',3,0,NULL),
(''Talbot_PSA'',''Spectrum'',''SpectrumCommentary'',''Narration'',''Nvarchar(max)'',''StagingID'',1,0,NULL),
(''Talbot_PSA'',''Subscribe'',''PolLyr'',''CtcNm'',''Nvarchar(max)'',''StagingID'',1,0,NULL),
--(''Talbot_PSA'',''Subscribe'',''PolMain'',''CtcNm'',''Nvarchar(max)'',''StagingID'',1,0,NULL),
(''Talbot_PSA'',''Subscribe'',''PolMain'',''Dsc'',''Nvarchar(max)'',''StagingID'',1,0,NULL),
(''Talbot_PSA'',''Subscribe'',''SCMMainAsAt'',''CurrNarrA'',''Nvarchar(max)'',''StagingID'',1,0,NULL),
(''Talbot_PSA'',''Subscribe'',''SCMMainAsAt'',''CurrNarrB'',''Nvarchar(max)'',''StagingID'',1,0,NULL),
(''Talbot_PSA'',''Subscribe'',''SCMMainAsAt'',''Insd'',''Nvarchar(max)'',''StagingID'',1,0,NULL),
(''Talbot_PSA'',''Subscribe'',''SCMMainAsAt'',''LossLocn'',''Nvarchar(max)'',''StagingID'',1,0,NULL),
(''Talbot_PSA'',''Subscribe'',''SCMMainAsAt'',''LossNarr1'',''Nvarchar(max)'',''StagingID'',1,0,NULL),
(''Talbot_PSA'',''Subscribe'',''SCMMainAsAt'',''LossNarr2'',''Nvarchar(max)'',''StagingID'',1,0,NULL),
(''Talbot_PSA'',''Subscribe'',''SCMMainAsAt'',''LossNarr3'',''Nvarchar(max)'',''StagingID'',1,0,NULL),
(''Talbot_PSA'',''Subscribe'',''Insd'',''InsdAKA'',''Nvarchar(max)'',''StagingID'',2,0,NULL),
--(''Talbot_PSA'',''Subscribe'',''Insd'',''InsdNm'',''Nvarchar(max)'',''StagingID'',2,0,NULL),
(''Talbot_PSA'',''Subscribe'',''SCMMain'',''CurrNarrA'',''Nvarchar(max)'',''StagingID'',2,0,NULL),
(''Talbot_PSA'',''Subscribe'',''SCMMain'',''CurrNarrB'',''Nvarchar(max)'',''StagingID'',2,0,NULL),
--(''Talbot_PSA'',''Subscribe'',''SCMMain'',''Insd'',''Nvarchar(max)'',''StagingID'',2,0,NULL),
(''Talbot_PSA'',''Subscribe'',''SCMMain'',''LossLocn'',''Nvarchar(max)'',''StagingID'',2,0,NULL),
(''Talbot_PSA'',''Subscribe'',''SCMMain'',''LossNarr1'',''Nvarchar(max)'',''StagingID'',2,0,NULL),
(''Talbot_PSA'',''Subscribe'',''SCMMain'',''LossNarr2'',''Nvarchar(max)'',''StagingID'',2,0,NULL),
(''Talbot_PSA'',''Subscribe'',''SCMMain'',''LossNarr3'',''Nvarchar(max)'',''StagingID'',2,0,NULL),
(''Talbot_PSA_Archive'',''Subscribe'',''PolLyr'',''Dsc'',''Nvarchar(max)'',''StagingID'',2,0,NULL),
(''Talbot_PSA_Archive'',''Subscribe'',''PolMain'',''CtcNm'',''Nvarchar(max)'',''StagingID'',2,0,NULL),
--(''Talbot_PSA_Archive'',''Subscribe'',''PolMain'',''Dsc'',''Nvarchar(max)'',''StagingID'',2,0,NULL),
(''Talbot_PSA_Archive'',''Subscribe'',''SCMMainAsAt'',''CurrNarrA'',''Nvarchar(max)'',''StagingID'',2,0,NULL),
(''Talbot_PSA_Archive'',''Subscribe'',''SCMMainAsAt'',''CurrNarrB'',''Nvarchar(max)'',''StagingID'',2,0,NULL),
(''Talbot_PSA_Archive'',''Subscribe'',''SCMMainAsAt'',''Insd'',''Nvarchar(max)'',''StagingID'',2,0,NULL),
(''Talbot_PSA_Archive'',''Subscribe'',''SCMMainAsAt'',''LossLocn'',''Nvarchar(max)'',''StagingID'',2,0,NULL),
(''Talbot_PSA_Archive'',''Subscribe'',''SCMMainAsAt'',''LossNarr1'',''Nvarchar(max)'',''StagingID'',2,0,NULL),
(''Talbot_PSA_Archive'',''Subscribe'',''SCMMainAsAt'',''LossNarr2'',''Nvarchar(max)'',''StagingID'',2,0,NULL),
(''Talbot_PSA_Archive'',''Subscribe'',''SCMMainAsAt'',''LossNarr3'',''Nvarchar(max)'',''StagingID'',2,0,NULL),
(''Talbot_PSA_Archive'',''Subscribe'',''Insd'',''InsdAKA'',''Nvarchar(max)'',''StagingID'',3,0,NULL),
--(''Talbot_PSA_Archive'',''Subscribe'',''Insd'',''InsdNm'',''Nvarchar(max)'',''StagingID'',3,0,NULL),
(''Talbot_PSA_Archive'',''Subscribe'',''OutPolAdvNt'',''Dsc'',''Nvarchar(max)'',''StagingID'',3,0,NULL),
(''Talbot_PSA_Archive'',''Subscribe'',''SCMMain'',''CurrNarrA'',''Nvarchar(max)'',''StagingID'',3,0,NULL),
(''Talbot_PSA_Archive'',''Subscribe'',''SCMMain'',''CurrNarrB'',''Nvarchar(max)'',''StagingID'',3,0,NULL),
--(''Talbot_PSA_Archive'',''Subscribe'',''SCMMain'',''Insd'',''Nvarchar(max)'',''StagingID'',3,0,NULL),
(''Talbot_PSA_Archive'',''Subscribe'',''SCMMain'',''LossLocn'',''Nvarchar(max)'',''StagingID'',3,0,NULL),
(''Talbot_PSA_Archive'',''Subscribe'',''SCMMain'',''LossNarr1'',''Nvarchar(max)'',''StagingID'',3,0,NULL),
(''Talbot_PSA_Archive'',''Subscribe'',''SCMMain'',''LossNarr2'',''Nvarchar(max)'',''StagingID'',3,0,NULL),
(''Talbot_PSA_Archive'',''Subscribe'',''SCMMain'',''LossNarr3'',''Nvarchar(max)'',''StagingID'',3,0,NULL),
--(''Talbot_Warehouse'',''Core'',''DimClaimSection'',''InsdSCM'',''Nvarchar(max)'',''ClaimSectionID'',1,0,NULL),
(''Talbot_Warehouse'',''Core'',''DimClaimSection'',''LossLocn'',''Nvarchar(max)'',''ClaimSectionID'',1,0,NULL),
(''Talbot_Warehouse'',''InterfaceDocosoft'',''ClaimsData'',''CurrNarrA'',''Nvarchar(max)'',''BPR'',1,0,NULL),
(''Talbot_Warehouse'',''InterfaceDocosoft'',''ClaimsData'',''CurrNarrB'',''Nvarchar(max)'',''BPR'',1,0,NULL),
(''Talbot_Warehouse'',''InterfaceDocosoft'',''ClaimsData'',''Insd'',''Nvarchar(max)'',''BPR'',1,0,NULL),
(''Talbot_Warehouse'',''InterfaceDocosoft'',''ClaimsData'',''losslocn'',''Nvarchar(max)'',''BPR'',1,0,NULL),
(''Talbot_Warehouse'',''InterfaceDocosoft'',''ClaimsData'',''LossNarr1'',''Nvarchar(max)'',''BPR'',1,0,NULL),
(''Talbot_Warehouse'',''InterfaceDocosoft'',''ClaimsData'',''LossNarr2'',''Nvarchar(max)'',''BPR'',1,0,NULL),
(''Talbot_Warehouse'',''InterfaceDocosoft'',''ClaimsData'',''LossNarr3'',''Nvarchar(max)'',''BPR'',1,0,NULL),

(''Talbot_Warehouse'',''InterfaceExposureManagement'',''EDW_DimReferenceAll'',''Dsc'',''Nvarchar(max)'',''PolicyReferenceId'',1,0,NULL),
(''Talbot_Warehouse'',''InterfaceExposureManagement'',''WH_DimInsured'',''InsdNm'',''Nvarchar(max)'',''InsdID'',1,0,NULL),
(''Talbot_Warehouse'',''InterfaceDocosoft'',''PolicyData'',''Dsc'',''Nvarchar(max)'',''Policy Reference'',2,0,NULL),
(''Talbot_Warehouse'',''Report'',''PVIIClaimTransaction'',''LossLocn'',''Nvarchar(max)'',''UID'',2,0,NULL),
(''Talbot_Warehouse'',''Report'',''PVIIClaimTransaction'',''LossNarr1'',''Nvarchar(max)'',''UID'',2,0,NULL),
(''Talbot_Warehouse'',''Report'',''PVIIClaimTransaction'',''LossNarr2'',''Nvarchar(max)'',''UID'',2,0,NULL),
(''Talbot_Warehouse'',''Report'',''PVIIClaimTransaction'',''LossNarr3'',''Nvarchar(max)'',''UID'',2,0,NULL),
--(''Talbot_Warehouse'',''Staging'',''DimClaimSection'',''InsdSCM'',''Nvarchar(max)'',''StagingID'',2,0,NULL),
(''Talbot_Warehouse'',''Staging'',''DimClaimSection'',''LossLocn'',''Nvarchar(max)'',''StagingID'',2,0,NULL),
--(''Talbot_Warehouse'',''Staging'',''DimInsured'',''Insd'',''Nvarchar(max)'',''StagingID'',2,0,NULL),
(''Talbot_Warehouse'',''Staging'',''DimInsured'',''SourceInsd'',''Nvarchar(max)'',''StagingID'',2,0,NULL),
--(''Talbot_Warehouse'',''Core'',''DimInsured'',''Insd'',''Nvarchar(max)'',''InsuredID'',3,0,NULL),
--(''Talbot_Warehouse'',''Core'',''DimInsured'',''SourceInsd'',''Nvarchar(max)'',''InsuredID'',3,0,NULL),
--(''Talbot_Warehouse'',''InterfaceJMDCreditControl'',''Policy'',''Insd'',''Nvarchar(max)'',''Policy Section Ref'',3,0,NULL),
(''Talbot_Warehouse'',''InterfaceSequelImpact'',''PolicyDetails'',''Assured'',''Nvarchar(max)'',''PolicyReference'',3,0,NULL),
(''Talbot_Warehouse'',''Report'',''Talbot_PSA_Subscribe_PolMain'',''CtcNm'',''Nvarchar(max)'',''StagingID'',3,0,NULL),
(''Talbot_Warehouse'',''Report'',''Talbot_PSA_Subscribe_PolMain'',''Dsc'',''Nvarchar(max)'',''StagingID'',3,0,NULL),
(''Talbot_Warehouse'',''Report'',''Talbot_PSA_Subscribe_SCMMain'',''CurrNarrA'',''Nvarchar(max)'',''StagingID'',3,0,NULL),
(''Talbot_Warehouse'',''Report'',''Talbot_PSA_Subscribe_SCMMain'',''CurrNarrB'',''Nvarchar(max)'',''StagingID'',3,0,NULL),
--(''Talbot_Warehouse'',''Report'',''Talbot_PSA_Subscribe_SCMMain'',''Insd'',''Nvarchar(max)'',''StagingID'',3,0,NULL),
(''Talbot_Warehouse'',''Report'',''Talbot_PSA_Subscribe_SCMMain'',''LossLocn'',''Nvarchar(max)'',''StagingID'',3,0,NULL),
(''Talbot_Warehouse'',''Report'',''Talbot_PSA_Subscribe_SCMMain'',''LossNarr1'',''Nvarchar(max)'',''StagingID'',3,0,NULL),
(''Talbot_Warehouse'',''Report'',''Talbot_PSA_Subscribe_SCMMain'',''LossNarr2'',''Nvarchar(max)'',''StagingID'',3,0,NULL),
(''Talbot_Warehouse'',''Report'',''Talbot_PSA_Subscribe_SCMMain'',''LossNarr3'',''Nvarchar(max)'',''StagingID'',3,0,NULL),
(''WorldCheckODS'',''UI'',''PostBindMatchReviewStatus'',''InsuredName'',''varchar'',''ReviewId'',3,0,NULL),
(''WorldCheckODS'',''UI'',''PostBindMatchReviewStatus_BKP_20200401'',''InsuredName'',''varchar'',''ReviewId'',3,0,NULL),
(''WorldCheckODS'',''UI'',''TempSearchResults'',''LastName'',''varchar'',''UID'',3,0,NULL),
(''WorldCheckODS'',''UI'',''TempSearchResults'',''FirstName'',''varchar'',''UID'',3,0,NULL),
(''WorldCheckODS'',''WorldCheck'',''Alias'',''Alias'',''varchar'',''AliasId'',3,0,NULL),
(''WorldCheckODS'',''WorldCheck'',''AlternativeSpelling'',''AlternativeSpelling'',''varchar'',''WorldCheckId'',3,0,NULL)
/*
(''WorldCheckODS'',''WorldCheck'',''WorldCheckData'',''LastName'',''varchar'',''WorldCheckId'',1,0,NULL),
(''WorldCheckODS'',''WorldCheck'',''WorldCheckData'',''FirstName'',''varchar'',''WorldCheckId'',1,0,NULL),
(''WorldCheckODS'',''WorldCheck'',''WorldCheckData'',''Aliases'',''varchar'',''WorldCheckId'',1,0,NULL),
(''WorldCheckODS'',''WorldCheck'',''WorldCheckData'',''AlternativeSpelling'',''varchar'',''WorldCheckId'',1,0,NULL),

(''WorldCheckODS'',''WorldCheck'',''worldcheckdata_bkp'',''LastName'',''varchar'',''UID'',3,0,NULL),
(''WorldCheckODS'',''WorldCheck'',''worldcheckdata_bkp'',''FirstName'',''varchar'',''UID'',3,0,NULL),
(''WorldCheckODS'',''WorldCheck'',''worldcheckdata_bkp'',''Aliases'',''varchar'',''UID'',3,0,NULL),
(''WorldCheckODS'',''WorldCheck'',''worldcheckdata_bkp'',''AlternativeSpelling'',''varchar'',''UID'',3,0,NULL),
(''WorldCheckStaging'',''Temp'',''ManualSanctionsData'',''LastName'',''varchar'',''UID'',3,0,NULL),
(''WorldCheckStaging'',''Temp'',''ManualSanctionsData'',''FirstName'',''varchar'',''UID'',3,0,NULL),
(''WorldCheckStaging'',''Temp'',''ManualSanctionsData'',''Aliases'',''varchar'',''UID'',3,0,NULL),

(''WorldCheckStaging'',''WORLDCHECK'',''WorldCheckData'',''LastName'',''varchar'',''UID'',3,0,NULL),
(''WorldCheckStaging'',''WORLDCHECK'',''WorldCheckData'',''FirstName'',''varchar'',''UID'',3,0,NULL),
(''WorldCheckStaging'',''WORLDCHECK'',''WorldCheckData'',''Aliases'',''varchar'',''UID'',3,0,NULL),
(''WorldCheckStaging'',''WORLDCHECK'',''WorldCheckData'',''AlternativeSpelling'',''varchar'',''UID'',3,0,NULL),

(''WorldCheckStaging'',''WORLDCHECK'',''WorldCheckData_bkp'',''LastName'',''varchar'',''UID'',3,0,NULL),
(''WorldCheckStaging'',''WORLDCHECK'',''WorldCheckData_bkp'',''FirstName'',''varchar'',''UID'',3,0,NULL),
(''WorldCheckStaging'',''WORLDCHECK'',''WorldCheckData_bkp'',''Aliases'',''varchar'',''UID'',3,0,NULL),
(''WorldCheckStaging'',''WORLDCHECK'',''WorldCheckData_bkp'',''AlternativeSpelling'',''varchar'',''UID'',3,0,NULL),
(''WorldCheckWarehouse'',''BulkSanctions'',''PreFilteredWorldCheckDataPostBindList'',''LastName'',''varchar'',''WorldCheckId'',3,0,NULL),
(''WorldCheckWarehouse'',''BulkSanctions'',''PreFilteredWorldCheckDataPostBindList'',''FirstName'',''varchar'',''WorldCheckId'',3,0,NULL),
(''WorldCheckWarehouse'',''BulkSanctions'',''PreFilteredWorldCheckDataPostBindList'',''Aliases'',''varchar'',''WorldCheckId'',3,0,NULL),
(''WorldCheckWarehouse'',''BulkSanctions'',''PreFilteredWorldCheckDataPostBindList'',''AlternativeSpelling'',''varchar'',''WorldCheckId'',3,0,NULL),
(''WorldCheckWarehouse'',''Dbo'',''PreFilteredWorldCheckData'',''LastName'',''varchar'',''WorldCheckId'',3,0,NULL),
(''WorldCheckWarehouse'',''Dbo'',''PreFilteredWorldCheckData'',''FirstName'',''varchar'',''WorldCheckId'',3,0,NULL),
(''WorldCheckWarehouse'',''Dbo'',''PreFilteredWorldCheckData'',''Aliases'',''varchar'',''WorldCheckId'',3,0,NULL),
(''WorldCheckWarehouse'',''Dbo'',''PreFilteredWorldCheckData'',''AlternativeSpelling'',''varchar'',''WorldCheckId'',3,0,NULL),

(''WorldCheckWarehouse'',''Dim'',''WorldCheckData'',''LastName'',''varchar'',''WorldCheckId'',2,0,NULL),
(''WorldCheckWarehouse'',''Dim'',''WorldCheckData'',''FirstName'',''varchar'',''WorldCheckId'',2,0,NULL),
(''WorldCheckWarehouse'',''Dim'',''WorldCheckData'',''Aliases'',''varchar'',''WorldCheckId'',2,0,NULL),
(''WorldCheckWarehouse'',''Dim'',''WorldCheckData'',''AlternativeSpelling'',''varchar'',''WorldCheckId'',2,0,NULL),

(''WorldCheckWarehouse'',''Dim'',''WorldCheckData_bkp'',''LastName'',''varchar'',''WorldCheckId'',3,0,NULL),
(''WorldCheckWarehouse'',''Dim'',''WorldCheckData_bkp'',''FirstName'',''varchar'',''WorldCheckId'',3,0,NULL),
(''WorldCheckWarehouse'',''Dim'',''WorldCheckData_bkp'',''Aliases'',''varchar'',''WorldCheckId'',3,0,NULL),
(''WorldCheckWarehouse'',''Dim'',''WorldCheckData_bkp'',''AlternativeSpelling'',''varchar'',''WorldCheckId'',3,0,NULL)
*/;', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Load Constraints table]    Script Date: 18/07/2025 11:41:56 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Load Constraints table', 
		@step_id=5, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'DECLARE @sql NVARCHAR(MAX) = '''';
DECLARE @DatabaseName NVARCHAR(128);

TRUNCATE TABLE DataMasking.dbo.Constraints;

DECLARE db_cursor CURSOR FOR
SELECT DISTINCT DatabaseName
FROM DataMasking.dbo.DataMaskingInfo
INNER JOIN sys.databases ON DataMasking.dbo.DataMaskingInfo.DatabaseName = sys.databases.name COLLATE Latin1_General_CI_AS;

OPEN db_cursor;
FETCH NEXT FROM db_cursor INTO @DatabaseName;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @sql = N''
USE ['' + @DatabaseName + ''];

INSERT INTO DataMasking.dbo.Constraints (DatabaseName, SchemaName, TableName, EnabledFKConstraints,EnabledTriggers,IsCDCEnabled)  
SELECT DISTINCT
    '''''' + @DatabaseName + '''''' AS DatabaseName,
    t.TABLE_SCHEMA AS SchemaName,
    t.TABLE_NAME AS TableName,

    -- Correctly fetching FOREIGN KEYS only
    ISNULL((
        SELECT STRING_AGG(''''['''' + fk.CONSTRAINT_NAME + '''']'''', '''','''') 
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS fk
        WHERE fk.TABLE_SCHEMA = t.TABLE_SCHEMA 
          AND fk.TABLE_NAME = t.TABLE_NAME 
          AND fk.CONSTRAINT_TYPE = ''''FOREIGN KEY''''
    ), '''''''') AS EnabledFKConstraints,

    -- Correctly fetching ENABLED TRIGGERS only
    ISNULL((
        SELECT STRING_AGG(tr.name, '''','''') 
        FROM sys.triggers tr
        JOIN sys.tables tbl ON tr.parent_id = tbl.object_id
        JOIN sys.schemas sch ON tbl.schema_id = sch.schema_id
        WHERE sch.name = t.TABLE_SCHEMA 
          AND tbl.name = t.TABLE_NAME
          AND tr.is_disabled = 0
    ), '''''''') AS EnabledTriggers,

    -- Correctly setting CDC tracking as BIT (1 = Yes, 0 = No)
    ISNULL(
        CAST(CASE WHEN cdc.is_tracked_by_cdc = 1 THEN 1 ELSE 0 END AS BIT), 0
    ) AS IsCDCEnabled

FROM INFORMATION_SCHEMA.TABLES t

-- Correctly joining sys.tables for CDC info
LEFT JOIN sys.tables cdc 
    ON cdc.object_id = OBJECT_ID(QUOTENAME(t.TABLE_SCHEMA) + ''''.'''' + QUOTENAME(t.TABLE_NAME))

-- Filtering tables based on DataMaskingInfo
LEFT JOIN [DataMasking].[dbo].[DataMaskingInfo] dmi
    ON t.TABLE_SCHEMA = dmi.SchemaName
    AND t.TABLE_NAME = dmi.TableName

WHERE t.TABLE_TYPE = ''''BASE TABLE''''
AND dmi.SchemaName IS NOT NULL
AND dmi.TableName IS NOT NULL;'';

    EXEC sp_executesql @sql;
    FETCH NEXT FROM db_cursor INTO @DatabaseName;
END;

CLOSE db_cursor;
DEALLOCATE db_cursor;
Go', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Disable Trigger, Foreign Keys and CDC]    Script Date: 18/07/2025 11:41:56 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Disable Trigger, Foreign Keys and CDC', 
		@step_id=6, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=9, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'DECLARE @Schema NVARCHAR(128);
DECLARE @Table NVARCHAR(128);
DECLARE @DatabaseName NVARCHAR(128);
DECLARE @TriggerList NVARCHAR(MAX);
DECLARE @TriggerName NVARCHAR(128);
DECLARE @ConstraintName NVARCHAR(MAX);
DECLARE @SQL NVARCHAR(MAX);
DECLARE @jobName NVARCHAR(100);

-- Disable Triggers
DECLARE TriggerCursor CURSOR FOR
SELECT SchemaName, TableName, DatabaseName, EnabledTriggers
FROM DataMasking.[dbo].[Constraints]
WHERE EnabledTriggers IS NOT NULL AND EnabledTriggers <> '''';

OPEN TriggerCursor;
FETCH NEXT FROM TriggerCursor INTO @Schema, @Table, @DatabaseName, @TriggerList;

WHILE @@FETCH_STATUS = 0
BEGIN
    -- Ensure TriggerList is not empty before processing
    IF @TriggerList IS NOT NULL AND @TriggerList <> ''''
    BEGIN
        DECLARE TriggerSplitCursor CURSOR FOR 
        SELECT value FROM STRING_SPLIT(@TriggerList, '','');

        OPEN TriggerSplitCursor;
        FETCH NEXT FROM TriggerSplitCursor INTO @TriggerName;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            -- Handle dynamic execution safely
            SET @SQL = ''USE subscribe; DISABLE TRIGGER '' + QUOTENAME(@TriggerName) + '' ON '' + QUOTENAME(@Schema) + ''.'' + QUOTENAME(@Table) + '';'';
            EXEC sp_executesql @SQL;

            FETCH NEXT FROM TriggerSplitCursor INTO @TriggerName;
        END;

        CLOSE TriggerSplitCursor;
        DEALLOCATE TriggerSplitCursor;
    END;

    FETCH NEXT FROM TriggerCursor INTO @Schema, @Table, @DatabaseName, @TriggerList;
END;

CLOSE TriggerCursor;
DEALLOCATE TriggerCursor;

-- Disable Foreign Keys
DECLARE FKCursor CURSOR FOR
SELECT SchemaName, TableName, DatabaseName, EnabledFKConstraints
FROM DataMasking.dbo.Constraints
WHERE EnabledFKConstraints IS NOT NULL;

OPEN FKCursor;
FETCH NEXT FROM FKCursor INTO @Schema, @Table, @DatabaseName, @ConstraintName;

WHILE @@FETCH_STATUS = 0
BEGIN
    IF @ConstraintName IS NOT NULL AND @ConstraintName <> ''''
    BEGIN
        EXEC(''USE '' + @DatabaseName + ''; ALTER TABLE ['' + @Schema + ''].['' + @Table + ''] NOCHECK CONSTRAINT '' + @ConstraintName);
    END
    FETCH NEXT FROM FKCursor INTO @Schema, @Table, @DatabaseName, @ConstraintName;
END

CLOSE FKCursor;
DEALLOCATE FKCursor;

-- Disable CDC
DECLARE @CaptureInstance NVARCHAR(128);

-- Cursor to iterate over databases that have CDC enabled
DECLARE DatabaseCursor CURSOR FOR
SELECT name FROM sys.databases WHERE is_cdc_enabled = 1;

OPEN DatabaseCursor;
FETCH NEXT FROM DatabaseCursor INTO @DatabaseName;

WHILE @@FETCH_STATUS = 0
BEGIN
    -- Cursor to iterate over CDC tables
	--------------------------------------------------------------------------------------------------
	-- Create a temp table to store the data
	IF OBJECT_ID(''tempdb..#CDCData'') IS NOT NULL
		DROP TABLE #CDCData;

	CREATE TABLE #CDCData (
		SchemaName NVARCHAR(128),
		TableName NVARCHAR(128),
		CaptureInstance NVARCHAR(128)
	);

	-- Build the dynamic SQL query to populate the temp table
	SET @SQL = ''
		INSERT INTO #CDCData (SchemaName, TableName, CaptureInstance)
		SELECT 
			cons.SchemaName, 
			cons.TableName, 
			ct.capture_instance 
		FROM DataMasking.dbo.Constraints cons
		INNER JOIN '' + QUOTENAME(@DatabaseName) + ''.cdc.change_tables CT
			ON cons.SchemaName + ''''_'''' + cons.TableName = ct.capture_instance
		WHERE cons.DatabaseName = @DatabaseName
		AND cons.IsCDCEnabled = 1;
	'';

	-- Execute the dynamic SQL
	EXEC sp_executesql @SQL, N''@DatabaseName NVARCHAR(128)'', @DatabaseName;

	-- Declare the cursor on the temp table
	DECLARE CDCCursor CURSOR FOR
	SELECT SchemaName, TableName, CaptureInstance FROM #CDCData;

	-- Open and process the cursor
	OPEN CDCCursor;
	FETCH NEXT FROM CDCCursor INTO @Schema, @Table, @CaptureInstance;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Disable CDC on each table		
		IF @CaptureInstance IS NOT NULL 
		BEGIN
			SET @SQL = ''USE '' + QUOTENAME(@DatabaseName) + 
					   ''; EXEC sys.sp_cdc_disable_table @source_schema = '''''' + @Schema + 
					   '''''', @source_name = '''''' + @Table + 
					   '''''', @capture_instance = '''''' + @CaptureInstance + '''''';'';
			EXEC sp_executesql @SQL;
		END
		-- stop and delete all cdc jobs for this database
		-- Declare a cursor to fetch all CDC jobs
		DECLARE @JobID UNIQUEIDENTIFIER;

		-- Declare a cursor to fetch all CDC jobs
		DECLARE jobCursor CURSOR FOR
		SELECT job_id, name FROM msdb.dbo.sysjobs
		WHERE name LIKE ''cdc.'' + @DatabaseName + ''%'';

		OPEN jobCursor;

		FETCH NEXT FROM jobCursor INTO @JobID, @JobName;

		WHILE @@FETCH_STATUS = 0
		BEGIN
			-- Check if job is running
			IF EXISTS (
				SELECT 1 
				FROM msdb.dbo.sysjobactivity 
				WHERE job_id = @JobID AND stop_execution_date IS NULL
			)
			BEGIN
				PRINT ''Stopping job: '' + @JobName;
				EXEC msdb.dbo.sp_stop_job @job_Name = @JobName;
				WAITFOR DELAY ''00:00:05''; -- Small delay to ensure job stops
			END
			ELSE
			BEGIN
				PRINT ''Job is not running: '' + @JobName;
			END

			-- Now delete the job safely
			PRINT ''Deleting job: '' + @JobName;
			EXEC msdb.dbo.sp_delete_job @job_Name = @JobName;

			FETCH NEXT FROM jobCursor INTO @JobID, @JobName;
		END

		CLOSE jobCursor;
		DEALLOCATE jobCursor;

        FETCH NEXT FROM CDCCursor INTO @Schema, @Table, @CaptureInstance;
    END;

    CLOSE CDCCursor;
    DEALLOCATE CDCCursor;
	DROP TABLE #CDCData;

    FETCH NEXT FROM DatabaseCursor INTO @DatabaseName;
END;

CLOSE DatabaseCursor;
DEALLOCATE DatabaseCursor;
GO', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Execute Data Masking - Console]    Script Date: 18/07/2025 11:41:56 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Execute Data Masking - Console', 
		@step_id=7, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'USE DataMasking; Exec dbo.DataMaskingOrchestrator Console', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Execute Datas Masking - ProForma]    Script Date: 18/07/2025 11:41:56 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Execute Datas Masking - ProForma', 
		@step_id=8, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'USE DataMasking; Exec dbo.DataMaskingOrchestrator ProForma', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Execute Data Masking  - Spectrum]    Script Date: 18/07/2025 11:41:56 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Execute Data Masking  - Spectrum', 
		@step_id=9, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=11, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'USE DataMasking; Exec dbo.DataMaskingOrchestrator Spectrum', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Execute Data Masking - Subscribe]    Script Date: 18/07/2025 11:41:56 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Execute Data Masking - Subscribe', 
		@step_id=10, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'USE DataMasking; Exec dbo.DataMaskingOrchestrator Subscribe', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Execute Data Masking - Talbot_PSA]    Script Date: 18/07/2025 11:41:56 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Execute Data Masking - Talbot_PSA', 
		@step_id=11, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=18, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'USE DataMasking; Exec dbo.DataMaskingOrchestrator Talbot_PSA', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Execute Data Masking  - Talbot_PSA_Archive]    Script Date: 18/07/2025 11:41:56 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Execute Data Masking  - Talbot_PSA_Archive', 
		@step_id=12, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'USE DataMasking; Exec dbo.DataMaskingOrchestrator Talbot_PSA_Archive', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Execute Data Masking - Lifecycle]    Script Date: 18/07/2025 11:41:56 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Execute Data Masking - Lifecycle', 
		@step_id=13, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'USE DataMasking; Exec dbo.DataMaskingOrchestrator LIFECYCLE', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Execute Data Masking -  Talbot_Warehouse]    Script Date: 18/07/2025 11:41:56 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Execute Data Masking -  Talbot_Warehouse', 
		@step_id=14, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'USE DataMasking; Exec dbo.DataMaskingOrchestrator Talbot_Warehouse', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Execute Data Masking - WorldCheckODS]    Script Date: 18/07/2025 11:41:56 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Execute Data Masking - WorldCheckODS', 
		@step_id=15, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'USE DataMasking; Exec dbo.DataMaskingOrchestrator WorldCheckODS', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Execute Data Masking - WorldCheckStaging]    Script Date: 18/07/2025 11:41:56 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Execute Data Masking - WorldCheckStaging', 
		@step_id=16, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'USE DataMasking; Exec dbo.DataMaskingOrchestrator WorldCheckStaging', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Execute Data Masking - WorldCheckWarehouse]    Script Date: 18/07/2025 11:41:56 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Execute Data Masking - WorldCheckWarehouse', 
		@step_id=17, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'USE DataMasking; Exec dbo.DataMaskingOrchestrator WorldCheckWarehouse', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Enable Triggers Foreign key and CDC]    Script Date: 18/07/2025 11:41:56 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Enable Triggers Foreign key and CDC', 
		@step_id=18, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'DECLARE 
  @Database NVARCHAR(128)
, @Schema NVARCHAR(128)
, @Table NVARCHAR(128)
, @TriggerList NVARCHAR(MAX)
, @FKList NVARCHAR(MAX)
, @CDCEnabled BIT
, @SQL NVARCHAR(MAX);

DECLARE ReEnableCursor CURSOR FOR
SELECT DatabaseName, SchemaName, TableName, EnabledTriggers, EnabledFKConstraints, IsCDCEnabled
FROM DataMasking.dbo.Constraints;

OPEN ReEnableCursor;
FETCH NEXT FROM ReEnableCursor INTO @Database, @Schema, @Table, @TriggerList, @FKList, @CDCEnabled;

WHILE @@FETCH_STATUS = 0
BEGIN
    -- Re-enable Triggers
    IF @TriggerList IS NOT NULL AND @TriggerList <> ''''
    BEGIN
        EXEC(''USE '' + @Database + ''; ENABLE TRIGGER '' + @TriggerList + '' ON ['' + @Schema + ''].['' + @Table + '']'');
    END

    -- Re-enable Foreign Keys
    IF @FKList IS NOT NULL AND @FKList <> ''''
    BEGIN
        EXEC(''USE '' + @Database + ''; ALTER TABLE ['' + @Schema + ''].['' + @Table + ''] CHECK CONSTRAINT '' + @FKList);
    END

    -- Re-enable CDC
    IF @CDCEnabled = 1
    BEGIN
		
		SET @SQL = ''USE '' + QUOTENAME(@Database) + 
					   ''; EXEC sys.sp_cdc_enable_table @source_schema = '''''' + @Schema + 
					   '''''', @source_name = '''''' + @Table + 
					   '''''', @filegroup_name = ''''FG_''+ @Database +''_CDC'' +
					   '''''', @role_name = ''''cdc_reader'''', @capture_instance = NULL;'';
			EXEC sp_executesql @SQL;

		SET @SQL = ''USE '' + QUOTENAME(@Database) + 
					   ''; EXEC sys.sp_cdc_add_job ''''capture''''
					    ; EXEC sys.sp_cdc_add_job ''''cleanup''''
						; EXEC sys.sp_cdc_change_job @job_type = N''''cleanup'''', @retention = 10080
					    ;
					   '';
			EXEC sp_executesql @SQL;
    END

    FETCH NEXT FROM ReEnableCursor INTO @Database, @Schema, @Table, @TriggerList, @FKList, @CDCEnabled;
END

CLOSE ReEnableCursor;
DEALLOCATE ReEnableCursor;', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO


