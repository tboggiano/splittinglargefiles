----------------------------------------------------------------------------------
-- Procedure Name: dbo.FilegroupMgt_RunRebuildStmts
--
-- Desc: This procedure shrinks runs created index statements for rebalance.
--
-- Parameters: 
--	INPUT
--			@Debug BIT = 0
--
--	OUTPUT
--
-- Auth: Tracy Boggiano
-- Date: 01/23/2015
--
-- Change History 
-- --------------------------------
-- Date - Auth: 
-- Description: 
----------------------------------------------------------------------------------
CREATE PROCEDURE [dbo].[FilegroupMgt_RunRebuildStmts]
	@Debug BIT = 0
AS
SET NOCOUNT ON

DECLARE
	@Size TINYINT = 50,
	@FilegroupMgtRebuildsID INT,
	@DatabaseName NVARCHAR(128),
	@FilegroupName NVARCHAR(128),
	@SQL NVARCHAR(MAX),
	@OldDatabaseName NVARCHAR(128) = '',
	@OldFilegroupName NVARCHAR(128) = '',
	@Count INT=0,
	@ShrinkComplete BIT = 0,
	@DropFile BIT

CREATE TABLE #File (FileName NVARCHAR(128))

DECLARE fg_cursor CURSOR READ_ONLY
FOR
SELECT
	fgr.FilegroupMgtRebuildsID,
	fgr.DatabaseName,
	fgr.FilegroupName,
	fgr.SQL
FROM
	dbo.FilegroupMgtRebuilds AS fgr
INNER JOIN dbo.FilegroupMgt AS fm 
	ON fm.DatabaseName = fgr.DatabaseName AND fm.FilegroupName = fgr.FilegroupName
WHERE RebuildComplete = 0

OPEN fg_cursor

FETCH NEXT FROM fg_cursor INTO @FilegroupMgtRebuildsID, @DatabaseName, @FilegroupName, @SQL
WHILE (@@fetch_status <> -1)
BEGIN
	IF (@@fetch_status <> -2)
	BEGIN
		--Reset if DatabaseName or FilegroupName has changed
		IF @OldDatabaseName <> @DatabaseName OR @OldFilegroupName <> @FilegroupName
		BEGIN
			SET @Count = 0
			SET @ShrinkComplete = 0
			
			--If databasename or filegroup name changed mark rebuild complete
			IF @OldDatabaseName <> '' AND @Debug = 0
			BEGIN
				UPDATE dbo.FilegroupMgt SET RebuildComplete = 1 
				WHERE DatabaseName = @OldDatabaseName 
				AND FilegroupName = @OldFilegroupName 
			END

			--if filgroupname changed shrink file 
			IF @OldFilegroupName <> '' AND @Debug = 0
			BEGIN
				EXEC dbo.FilegroupMgt_ShrinkFile @DatabaseName = @OldDatabaseName,
					@FilegroupName = @OldFilegroupName,
					@Size = @Size,
					@Debug = @Debug,
					@ShrinkComplete = @ShrinkComplete OUTPUT
			END
			ELSE IF @Debug = 0 --if filegroupname is new then shrink for first run
			BEGIN
				EXEC dbo.FilegroupMgt_ShrinkFile @DatabaseName = @DatabaseName,
					@FilegroupName = @FilegroupName,
					@Size = @Size,
					@Debug = @Debug,
					@ShrinkComplete = @ShrinkComplete OUTPUT
			END

			IF @OldDatabaseName <> '' AND @ShrinkComplete = 1 AND @Debug = 0
			BEGIN
				UPDATE dbo.FilegroupMgt SET ResizeComplete = 1 
				WHERE DatabaseName = @OldDatabaseName 
				AND FilegroupName = @OldFilegroupName 
			END

			IF @Debug = 0
			BEGIN
				UPDATE dbo.FilegroupMgt SET RebuildComplete = 1 
				WHERE DatabaseName = @OldDatabaseName 
				AND FilegroupName = @OldFilegroupName 
			END

			SET @OldDatabaseName = @DatabaseName
			SET @OldFilegroupName = @FilegroupName
		END

		--Remove empty space before starting when changing databases or filegroups
		--OR IF processed 10 indexes and the file is not completely shrunk
		IF @Count <> 0 AND @Count % 10 = 0 AND @ShrinkComplete = 0 AND @Debug = 0--Every 10th index if Shrink not complete
		BEGIN
			EXEC dbo.FilegroupMgt_ShrinkFile @DatabaseName = @DatabaseName,
				@FilegroupName = @FilegroupName,
				@Size = @Size,
				@Debug = @Debug,
				@ShrinkComplete = @ShrinkComplete OUTPUT

			IF @ShrinkComplete = 1 
			BEGIN
				UPDATE dbo.FilegroupMgt SET ResizeComplete = 1 
				WHERE DatabaseName = @DatabaseName 
					AND FilegroupName = @FilegroupName  
			END
		END

		SET @SQL = 'USE ' + @DatabaseName + '; ' + @SQL

		IF @Debug = 0
		BEGIN
			EXEC (@SQL)
			
			DELETE FROM dbo.FilegroupMgtRebuilds 
			WHERE FilegroupMgtRebuildsID = @FilegroupMgtRebuildsID
		END
		ELSE
		BEGIN
			PRINT @SQL
		END

		SET @OldDatabaseName = @DatabaseName
		SET @OldFilegroupName = @FilegroupName

		SET @Count = @Count + 1
	END
	FETCH NEXT FROM fg_cursor INTO @FilegroupMgtRebuildsID, @DatabaseName, @FilegroupName, @SQL
END

CLOSE fg_cursor
DEALLOCATE fg_cursor

IF @ShrinkComplete = 0 AND @Debug = 0
BEGIN
	EXEC dbo.FilegroupMgt_ShrinkFile @DatabaseName = @DatabaseName,
		@FilegroupName = @FilegroupName,
		@Size = @Size,
		@Debug = @Debug,
		@ShrinkComplete = @ShrinkComplete OUTPUT
END

IF @Debug = 0
BEGIN
	UPDATE dbo.FilegroupMgt SET RebuildComplete = 1 
	WHERE DatabaseName = @OldDatabaseName 

	IF @ShrinkComplete = 1 
	BEGIN
		UPDATE dbo.FilegroupMgt SET ResizeComplete = 1 
		WHERE DatabaseName = @DatabaseName 
		AND FilegroupName = @FilegroupName 
	END
END

--Final Shrinks if Needed 
DECLARE CursorFinalShrink CURSOR
READ_ONLY
FOR 
SELECT 
	DatabaseName, 
	FilegroupName,
	DropFile 
FROM dbo.FilegroupMgt
WHERE (ResizeComplete = 0
	OR DropFile = 1)
	AND DroppedFile = 0

OPEN CursorFinalShrink

FETCH NEXT FROM CursorFinalShrink INTO @DatabaseName, @FilegroupName, @DropFile
WHILE (@@fetch_status <> -1)
BEGIN
	IF (@@fetch_status <> -2)
	BEGIN
		IF @DropFile = 1
		BEGIN
			TRUNCATE TABLE #File

			SET @SQL = REPLACE(REPLACE(REPLACE(CAST( 
					'USE {{DatabaseName}};
					SELECT TOP 1
						df.name
					FROM
						sys.database_Files df
					INNER JOIN sys.filegroups fg
						ON df.data_space_id = fg.data_space_id
					WHERE fg.name = "{{FilegroupName}}"
					ORDER BY df.name;
				' AS NVARCHAR(MAX)) 
					,'{{DatabaseName}}', @DatabaseName)
					,'{{FilegroupName}}', @FilegroupName)
					,'"','''')

			INSERT INTO #File
			EXEC (@SQL);

			SELECT @SQL = REPLACE(REPLACE(REPLACE(CAST(
				'USE {{DatabaseName}}; DBCC SHRINKFILE ("{{FileName}}", EMPTYFILE);
				ALTER DATABASE {{DatabaseName}} REMOVE FILE {{FileName}};
				' AS NVARCHAR(MAX))
					,'{{DatabaseName}}', @DatabaseName)
					,'{{Filename}}', FILENAME)
					,'"', '''')
			FROM #File 

			IF @Debug = 0
			BEGIN
				EXEC (@SQL);

				UPDATE dbo.FilegroupMgt SET ResizeComplete = 1, 
					DroppedFile = 1,
					RebuildComplete = 1 
				WHERE DatabaseName = @DatabaseName 
				AND FilegroupName = @FilegroupName 
			END
			ELSE
			BEGIN
				PRINT @SQL
			END
		END
		ELSE IF @Debug = 0
		BEGIN
			EXEC dbo.FilegroupMgt_ShrinkFile @DatabaseName = @DatabaseName,
				@FilegroupName = @FilegroupName,
				@Size = @Size,
				@Debug = @Debug,
				@ShrinkComplete = @ShrinkComplete OUTPUT
		
			IF @ShrinkComplete = 1 
			BEGIN
				UPDATE dbo.FilegroupMgt SET ResizeComplete = 1 
				WHERE DatabaseName = @DatabaseName 
				AND FilegroupName = @FilegroupName 
			END
		END
	END
	FETCH NEXT FROM CursorFinalShrink INTO @DatabaseName, @FilegroupName, @DropFile
END

CLOSE CursorFinalShrink
DEALLOCATE CursorFinalShrink
GO