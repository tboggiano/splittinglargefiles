----------------------------------------------------------------------------------
-- Procedure Name: dbo.FilegroupMgt_AddFiles
--
-- Desc: This procedure checks to see if the last file in a filegroup has reached
--	a certain size.  If so it adds another file to the filegroup if there is enough
--	space on the MP, if not and email will be sent to DBOPs.   
--
-- Parameters: 
--	INPUT
--		@FileSizeGBs - What size to make the files
--		@Debug - Defaults to print information instead of performing actions
--
--	OUTPUT
--
-- Auth: Tracy Boggiano
-- Date: 01/23/2015
--
-- Change History 
-- --------------------------------
-- Date - Auth: 9/29/2015 Tracy Boggiano
-- Description: Fix bug to limit number of files created when 4 or more already
--				exist to 4.
----------------------------------------------------------------------------------
CREATE PROCEDURE [dbo].[FilegroupMgt_AddFiles] 
(
	@FileSizeGBs TINYINT = 50,
        @mailprofile nvarchar(128), 
        @email nvarchar(128),
	@Debug BIT = 0
)
AS
SET NOCOUNT ON;

DECLARE @SQL NVARCHAR(MAX),
	@DatabaseID INT,
	@DataSpaceID INT,
	@DataAvailableGB INT,
	@DataVolumeMountPoint NVARCHAR(512),
	@Body NVARCHAR(MAX),
	@Subject NVARCHAR(255),
	@LogicalName NVARCHAR(128),
	@NumFiles TINYINT,
	@Folder NVARCHAR(260),
	@FileName NVARCHAR(260),
	@IsPercentGrowth BIT,
	@Growth INT,
	@FilegroupName NVARCHAR(128),
	@TotalSizeGBs DECIMAL(6, 1),
	@NumFilesNeeded INT,
	@FileLoop TINYINT,
	@FileNum TINYINT,
	@GBsNeeded INT,
	@CallRebuilds BIT = 0,
	@FileNumChar NVARCHAR(2),
	@FileID INT;

CREATE TABLE #Filegroups
(
	DatabaseName NVARCHAR(128) NOT NULL,
	FilegroupName NVARCHAR(128) NOT NULL,
	DataSpaceID INT NOT NULL,
	FileName NVARCHAR(128) NOT NULL,
	NumFiles INT NOT NULL,
	TotalSizeGB DECIMAL(6, 1) NOT NULL
);

CREATE TABLE #DatabaseFile
(
	DatabaseName NVARCHAR(128) NOT NULL,
	FilegroupName NVARCHAR(128) NOT NULL,
	PhysicalFilename NVARCHAR(260) NOT NULL,
	FileID INT NOT NULL,
	SizeGB DECIMAL(6, 2) NOT NULL,
	FreeSpaceGB DECIMAL(6,2) NOT NULL,
	LogicalName NVARCHAR(128),
	NumFiles INT NOT NULL,
	TotalSizeGB DECIMAL(6, 1) NOT NULL,
	Growth INT NOT NULL,
	IsPercentGrowth BIT NOT NULL,
	DataSpaceID INT NOT NULL
);

CREATE TABLE #SQL
(
	ExecSQL NVARCHAR(MAX) NOT NULL
);

CREATE TABLE #SpaceNeeded
(
	DatabaseName NVARCHAR(128) NOT NULL,
	FilegroupName NVARCHAR(128) NOT NULL,
	TempFile INT NOT NULL DEFAULT(0),
	TotalFGFiles INT NOT NULL DEFAULT (0),
	LogFile INT NOT NULL DEFAULT (0)
);

--Get max file ID and total size in filegroup
SET @SQL = (
	SELECT
		REPLACE('INSERT INTO #Filegroups 
						(
							DatabaseName, 
							FilegroupName, 
							DataSpaceID,

FileName,
NumFiles,
TotalSizeGB
)
SELECT ''' + d.name + ''' AS DatabaseName,
fg.name AS FilegroupName,
MAX(sdf.data_space_id) AS DataSpaceID,
MAX(Sdf.name) AS FileName,
COUNT(*) AS NumFiles,
SUM(sdf.size) / 128.0 / 1024 AS TotalSizeGB
FROM
' + d.name + '.sys.database_files sdf
INNER JOIN ' + d.name + '.sys.filegroups fg
ON sdf.data_space_id = fg.data_space_id
WHERE fg.type = ''FG''
GROUP BY
fg.name;', CHAR(13), '')
FROM dbo.databases AS d
FOR XML PATH('')
);

EXEC(@SQL);

--Get file details
SET @SQL = (
SELECT
REPLACE('USE ' + fg.DatabaseName + ';
INSERT INTO #DatabaseFile
(
DatabaseName,
FilegroupName,
PhysicalFilename,
FileID,
SizeGB,
FreeSpaceGB,
LogicalName,
NumFiles,
TotalSizeGB,
Growth,
IsPercentGrowth,
DataSpaceID
)
SELECT
tfg.DatabaseName,
tfg.FilegroupName,
msdf.physical_name AS PhysicalFilename,
msdf.file_id AS FileID,
msdf.size / 128.0 / 1024 AS SizeGB,
msdf.size / 128.0 / 1024 - CAST(FILEPROPERTY(msdf.name, ''SpaceUsed'') AS INT) / 128.0 / 1024 AS FreeSpaceGB,
msdf.name AS LogicalName,
tfg.NumFiles,
tfg.TotalSizeGB,
sdf.Growth,
sdf.is_percent_growth,
tfg.DataSpaceID
FROM #Filegroups tfg
INNER JOIN ' + fg.DatabaseName + '.sys.filegroups fg
ON tfg.FilegroupName = fg.name
INNER JOIN ' + fg.DatabaseName + '.sys.database_files sdf
ON sdf.Data_Space_ID = tfg.DataSpaceID AND ''' + fg.FilegroupName + ''' = tfg.FilegroupName
INNER JOIN (SELECT
physical_name,
file_id,
size,
name,
Data_Space_ID
FROM ' + fg.DatabaseName + '.sys.database_files sdf ) msdf
ON tfg.DataSpaceID = msdf.Data_Space_ID AND msdf.name = tfg.FileName
WHERE tfg.DatabaseName = ''' + fg.DatabaseName + '''
GROUP BY
tfg.DatabaseName,
tfg.FilegroupName,
msdf.physical_name,
msdf.file_id,
msdf.name,
msdf.size,
tfg.NumFiles,
tfg.TotalSizeGB,
sdf.Growth,
sdf.is_percent_growth,
tfg.DataSpaceID;', CHAR(13), '')
FROM #FileGroups AS fg
FOR XML PATH('')
);

EXEC (@SQL);

--Generate ALTER command and check for free space before creating, if not enough free space send email
--Put in cursor to be able to execute DMF sys.dm_os_volume_stats
DECLARE dfcursor CURSOR
READ_ONLY
FOR
SELECT d.database_id AS DatabaseID ,
df.DataSpaceID ,
df.LogicalName ,
NumFiles ,
REVERSE(RIGHT(REVERSE(PhysicalFilename), ( LEN(PhysicalFilename) - CHARINDEX('\', REVERSE(PhysicalFilename), 1) ) + 1)) AS Folder ,
LEFT(REVERSE(LEFT(REVERSE(PhysicalFilename), CHARINDEX('\', REVERSE(PhysicalFilename)) - 1)), LEN(REVERSE(LEFT(REVERSE(PhysicalFilename), CHARINDEX('\', REVERSE(PhysicalFilename)) - 1))) - 4) AS Filename ,
IsPercentGrowth ,
Growth / 128 Growth ,
df.FilegroupName ,
TotalSizeGB,
df.FileID
FROM #DatabaseFile df
INNER JOIN dbo.databases d ON d.name = df.DatabaseName
INNER JOIN ( SELECT DatabaseName ,
df.FilegroupName ,
MIN(df.FileID) FileID
FROM #DatabaseFile df
INNER JOIN dbo.vAccessibleChangeableUserDBs d ON d.name = df.DatabaseName
GROUP BY DatabaseName ,
df.FilegroupName
) t ON d.NAME = t.DatabaseName
AND df.FilegroupName = t.FilegroupName
AND df.FileID = t.FileID
WHERE ( SizeGB = @FileSizeGBs
AND FreeSpaceGB / SizeGB <= .1
)
OR SizeGB > @FileSizeGBs

OPEN dfcursor

FETCH NEXT FROM dfcursor INTO @DatabaseID, @DataSpaceID, @LogicalName, @NumFiles, @Folder, @FileName, @IsPercentGrowth,
@Growth, @FilegroupName, @TotalSizeGBs, @FileID;
WHILE (@@fetch_status <> -1)
BEGIN
IF (@@fetch_status <> -2)
BEGIN
IF @NumFiles > 1
BEGIN
--Find number of files and double to avoid off balance files
SET @NumFilesNeeded = @NumFiles * 2

IF @NumFiles >= 4
BEGIN
SET @NumFilesNeeded = 4
END
END
ELSE
BEGIN
--Get number of files plus one because we are dropping the intial file
SET @NumFilesNeeded = CEILING(@TotalSizeGBs / @FileSizeGBs)
SET @CallRebuilds = 1
END

SET @GBsNeeded = @NumFilesNeeded * @FileSizeGBs

SELECT
@DataAvailableGB = (available_bytes / 1024 / 1024 / 1024) - (total_bytes / 1024 /1024 / 1024 * 0.10),
@DataVolumeMountPoint = volume_mount_point
FROM sys.dm_os_volume_stats(@DatabaseID, @FileID) AS dovs;

IF @DataAvailableGB >= @GBsNeeded
BEGIN
IF @Debug = 0
BEGIN
INSERT INTO dbo.FilegroupMGt
(
DatabaseName,
FilegroupName,
DropFile
)
VALUES
(
DB_NAME(@DatabaseID),
@FilegroupName,
CASE @NumFiles
WHEN 1 THEN 1
ELSE 0
END
);
END

--Add enough files to distribute data
IF @NumFilesNeeded + @NumFiles <> @NumFiles AND @NumFilesNeeded > 1
BEGIN
SET @FileLoop = @NumFiles + 1
SET @FileLoop = 1

IF @NumFiles > 1
BEGIN
SET @FileNum = @NumFiles + 2
END
ELSE
BEGIN
SET @FileNum = 2
END

WHILE @FileLoop <= @NumFilesNeeded
BEGIN
SET @FileNumChar = CASE LEN(@FileNum)
WHEN 1 THEN '0' + CAST(@FileNum AS NVARCHAR(2))
ELSE CAST(@FileNum AS NVARCHAR(2))
END

SET @SQL = 'ALTER DATABASE [' + DB_NAME(@DatabaseID) + '] ADD FILE (NAME = N'''
+ CASE @NumFiles
WHEN 1 THEN @LogicalName
ELSE LEFT(@LogicalName, LEN(@LogicalName) - 3)
END
+ '_' + @FileNumChar
+ ''', '
+ 'FILENAME = N''' + @Folder
+ CASE @NumFiles
WHEN 1 THEN @FileName
ELSE LEFT(@Filename, LEN(@FileName) - 3)
END
+ '_' + @FileNumChar + '.NDF'', '
+ 'SIZE = ' + CAST(CAST(@FileSizeGBs AS TINYINT) AS NVARCHAR(5)) + ' GB, FILEGROWTH = ' + CAST(@Growth as NVARCHAR(8))
+ CASE @IsPercentGrowth
WHEN 1 THEN '%'
ELSE 'MB'
END
+ ' ) TO FILEGROUP [' + @FilegroupName + ']; '

IF @Debug = 1
BEGIN
PRINT @SQL;
END
ELSE
BEGIN
EXEC (@SQL);
END

SET @FileNum = @FileNum + 1
SET @FileLoop = @FileLoop + 1
END
END
END
ELSE
BEGIN
IF @GBsNeeded > @FileSizeGBs
BEGIN
IF @Debug = 1
BEGIN
PRINT 'Mount Point or Drive ' + @DataVolumeMountPoint + ' on ' + @@SERVERNAME
+ ' for database ' + DB_NAME(@DatabaseID) + ' and Filegroup ' + @FilegroupName
+ ' needs to be expanded to add new ' + CAST(@GBsNeeded as NVARCHAR(4))
+ ' GBs for rebalancing.';
END
ELSE
BEGIN
SET @Body = 'Mount Point or Drive ' + @DataVolumeMountPoint + ' on ' + @@SERVERNAME
+ ' for database ' + DB_NAME(@DatabaseID) + ' and Filegroup ' + @FilegroupName
+ ' needs to be expanded to add new ' + CAST(@GBsNeeded as NVARCHAR(4))
+ ' GBs for rebalancing.';
SET @Subject = 'Mount Point or Drive Expansion Needed on ' + @@SERVERNAME

EXEC msdb.dbo.sp_send_dbmail
@profile_name = @mailprofile,
@recipients = @email,
@body = @Body,
@subject = @Subject;
END
END
END
END
FETCH NEXT FROM dfcursor INTO @DatabaseID, @DataSpaceID, @LogicalName, @NumFiles, @Folder, @FileName, @IsPercentGrowth,
@Growth, @FilegroupName, @TotalSizeGBs, @FileID;
END

CLOSE dfcursor;
DEALLOCATE dfcursor;

DROP TABLE #FileGroups;
DROP TABLE #DatabaseFile;
DROP TABLE #SQL;
DROP TABLE #SpaceNeeded;

IF @CallRebuilds = 1
BEGIN
EXEC dbo.FilegroupMgt_CreateRebuildStmts @Debug = @Debug
END
GO