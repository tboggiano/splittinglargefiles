----------------------------------------------------------------------------------
-- Procedure Name: dbo.FilegroupMgt_CreateRebuildStmts
--
-- Desc: This procedure creates create index statements to rebalance files.
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
-- Date - Auth: 09/14/2015 - Tracy Boggiano
-- Description: Add QUOTENAME around column names
----------------------------------------------------------------------------------
CREATE PROC [dbo].[FilegroupMgt_CreateRebuildStmts]
	@Debug BIT = 0
AS 
SET NOCOUNT ON

DECLARE
	@FileGroupMgtID INT,
	@DatabaseName NVARCHAR(128),
	@FilegroupName NVARCHAR(128),
	@SQL NVARCHAR(MAX),
	@FilegroupMgtRebuildsID INT

DECLARE CursorFilegroup CURSOR
FOR
SELECT 
	fgm.FilegroupMgtID,
	fgm.DatabaseName,
	fgm.FilegroupName
FROM
	dbo.FilegroupMgt fgm
WHERE
	RebuildComplete = 0
	OR ResizeComplete = 0

OPEN CursorFilegroup

FETCH NEXT FROM CursorFilegroup INTO @FileGroupMgtID, @DatabaseName, @FilegroupName

WHILE ( @@fetch_status = 0 )
BEGIN
	SET @SQL = REPLACE(REPLACE(REPLACE(CAST( -- <<--- Added this CAST
		'
		USE {{DatabaseName}};
	 
		DECLARE 
		@SchemaName VARCHAR(100),
		@TableName VARCHAR(256),
		@IndexName VARCHAR(256),
		@ColumnName VARCHAR(100),
		@is_unique VARCHAR(100),
		@IndexTypeDesc VARCHAR(100),
		@FileGroupName VARCHAR(100),
		@IndexOptions VARCHAR(MAX),
		@IndexColumnId INT,
		@IsDescendingKey INT,
		@IsIncludedColumn INT,
		@TSQLScripCreationIndex VARCHAR(MAX),
		@PartitionScheme AS NVARCHAR(128),
		@PartitionColumn AS NVARCHAR(128),
		@IndexType AS TINYINT,
		@ObjectID AS INT,
		@IndexID AS INT,
		@LOB AS TINYINT,
		@SkipXMLSpatial TINYINT,
		@IsFiltered BIT,
		@FilteredDefinition NVARCHAR(MAX),
		@DataCompressionDesc NVARCHAR(4);

	DECLARE CursorIndex CURSOR
	FOR
	SELECT
		SCHEMA_NAME(t.schema_id) SchemaName,
		t.name TableName,
		ix.name IndexName,
		ISNULL(ps.name, "") PartitionName,
		c.name PartitionColumn,
		CASE	WHEN ix.is_unique = 1 THEN "UNIQUE "
				ELSE ""
		END,
		ix.type_desc,
		CASE	WHEN ix.is_padded = 1 THEN "PAD_INDEX = ON, "
				ELSE "PAD_INDEX = OFF, "
		END + CASE	WHEN ix.allow_page_locks = 1 THEN "ALLOW_PAGE_LOCKS = ON, "
					ELSE "ALLOW_PAGE_LOCKS = OFF, "
				END + CASE	WHEN ix.allow_row_locks = 1 THEN "ALLOW_ROW_LOCKS = ON, "
							ELSE "ALLOW_ROW_LOCKS = OFF, "
						END
		+ CASE	WHEN INDEXPROPERTY(t.object_id, ix.name, "IsStatistics") = 1 THEN "STATISTICS_NORECOMPUTE = ON, "
				ELSE "STATISTICS_NORECOMPUTE = OFF, "
			END + CASE	WHEN ix.ignore_dup_key = 1 THEN "IGNORE_DUP_KEY = ON, "
						ELSE "IGNORE_DUP_KEY = OFF, "
					END + "SORT_IN_TEMPDB = OFF, FILLFACTOR = " + CAST(CASE ix.fill_factor
																		WHEN 0 THEN 100
																		ELSE ix.fill_factor
																		END AS VARCHAR(3)) AS IndexOptions,
		fg.name FileGroupName,
		ix.type,
		t.object_id,
		ix.index_id,
		ix.has_filter,
		ix.filter_definition,
		p.data_compression_desc
	FROM
		sys.tables t
	INNER JOIN sys.indexes ix
		ON t.object_id = ix.object_id
	INNER JOIN sys.partitions p
		ON ix.object_id = p.object_id
			AND ix.index_id = p.index_id
	LEFT OUTER JOIN sys.partition_schemes ps
		ON ix.data_space_id = ps.data_space_id
	LEFT OUTER JOIN sys.destination_data_spaces dds
		ON ps.data_space_id = dds.partition_scheme_id
			AND p.partition_number = dds.destination_id
	LEFT OUTER JOIN sys.index_columns ic 
			ON (ic.partition_ordinal > 0) 
			AND (ic.index_id = ix.index_id AND ic.object_id = CAST(t.object_id AS INT))
	LEFT OUTER JOIN sys.columns c 
			ON c.object_id = ic.object_id 
			AND c.column_id = ic.column_id 
	INNER JOIN sys.filegroups fg
		ON COALESCE(dds.data_space_id, ix.data_space_id) = fg.data_space_id
	WHERE
		ix.type > 0
		AND t.is_ms_shipped = 0
		AND fg.name = "{{FilegroupName}}"
		AND ix.is_unique_constraint = 0
	ORDER BY
		SCHEMA_NAME(t.schema_id),
		t.name,
		ix.name

	OPEN CursorIndex

	FETCH NEXT FROM CursorIndex INTO @SchemaName, @TableName, @IndexName, @PartitionScheme, @PartitionColumn,
		@is_unique, @IndexTypeDesc, @IndexOptions, @FileGroupName, @IndexType, @ObjectID, @IndexID, @IsFiltered,
		@FilteredDefinition, @DataCompressionDesc

	WHILE ( @@fetch_status = 0 )
	BEGIN
		DECLARE	@IndexColumns VARCHAR(MAX)
		DECLARE	@IncludedColumns VARCHAR(MAX)
 
		SET @LOB = 0
		SET @IndexColumns = ""
		SET @IncludedColumns = ""
		SET @SkipXMLSpatial = 0
 
		DECLARE CursorIndexColumn CURSOR
		FOR
		SELECT
			col.name,
			ixc.is_descending_key,
			ixc.is_included_column
		FROM
			sys.tables tb
		INNER JOIN sys.indexes ix
			ON tb.object_id = ix.object_id
		INNER JOIN sys.index_columns ixc
			ON ix.object_id = ixc.object_id
				AND ix.index_id = ixc.index_id
		INNER JOIN sys.columns col
			ON ixc.object_id = col.object_id
				AND ixc.column_id = col.column_id
		WHERE
			ix.type > 0
			AND SCHEMA_NAME(tb.schema_id) = @SchemaName
			AND tb.name = @TableName
			AND ix.name = @IndexName
			AND (ixc.partition_ordinal <> ixc.key_ordinal  --Lead column (fixes boo-boo)
			AND (ixc.partition_ordinal <> 1 and ixc.key_ordinal > 0) --PartitionID or other partitioned field 
				OR (ixc.partition_ordinal = 1 and ixc.key_ordinal > 0) 
				OR ixc.is_included_column = 1)  -- But Include if included column
		ORDER BY
			ixc.key_ordinal
 
		OPEN CursorIndexColumn 
		FETCH NEXT FROM CursorIndexColumn INTO @ColumnName, @IsDescendingKey, @IsIncludedColumn
 
		WHILE ( @@fetch_status = 0 )
		BEGIN
			IF @IsIncludedColumn = 0
				SET @IndexColumns = @IndexColumns + QUOTENAME(@ColumnName) + CASE	WHEN @IsDescendingKey = 1 THEN " DESC, "
																		ELSE " ASC, "
																	END
			ELSE
				SET @IncludedColumns = @IncludedColumns + QUOTENAME(@ColumnName) + ", "

			FETCH NEXT FROM CursorIndexColumn INTO @ColumnName, @IsDescendingKey, @IsIncludedColumn
		END

		CLOSE CursorIndexColumn
		DEALLOCATE CursorIndexColumn

		SET @IndexColumns = SUBSTRING(@IndexColumns, 1, LEN(@IndexColumns) - 1)
		SET @IncludedColumns = CASE	WHEN LEN(@IncludedColumns) > 0
									THEN SUBSTRING(@IncludedColumns, 1, LEN(@IncludedColumns) - 1)
									ELSE ""
								END

		/* Determine if the table contains LOBs */
		IF @IndexType = 1
		BEGIN
			SELECT @LOB = COUNT(*)
			FROM {{DatabaseName}}.sys.columns COLUMNS
			INNER JOIN {{DatabaseName}}.sys.types types 
				ON columns.system_type_id = types.user_type_id OR (columns.user_type_id = types.user_type_id AND types.is_assembly_type = 1)  
			WHERE [object_id] = @ObjectID
			AND types.name IN("image", "text", "ntext") ;

			SELECT @SkipXMLSpatial = COUNT(*)
			FROM {{DatabaseName}}.sys.columns COLUMNS
			INNER JOIN {{DatabaseName}}.sys.types types 
				ON columns.system_type_id = types.user_type_id OR (columns.user_type_id = types.user_type_id AND types.is_assembly_type = 1)  
			WHERE [object_id] = @ObjectID
			AND types.name IN("xml", "geometry", "geography") ;
		END
		ELSE IF @indexType = 2
		BEGIN
			SELECT @LOB = COUNT(*) 
			FROM {{DatabaseName}}.sys.index_columns index_columns 
			INNER JOIN {{DatabaseName}}.sys.columns columns 
				ON index_columns.[object_id] = columns.[object_id] AND index_columns.column_id = columns.column_id 
			INNER JOIN {{DatabaseName}}.sys.types types 
				ON columns.system_type_id = types.user_type_id OR (columns.user_type_id = types.user_type_id AND types.is_assembly_type = 1) 
			WHERE index_columns.[object_id] = @ObjectID 
			AND index_columns.index_id = @indexID 
			AND types.name IN("image", "text", "ntext") ;
		END

		SET @TSQLScripCreationIndex = ""

		SET @TSQLScripCreationIndex = "CREATE " + @is_unique + @IndexTypeDesc + " INDEX " + QUOTENAME(@IndexName)
			+ " ON " + QUOTENAME(@SchemaName) + "." + QUOTENAME(@TableName) + "(" + @IndexColumns + ") "
			+ CASE	WHEN LEN(@IncludedColumns) > 0 THEN CHAR(13) + "INCLUDE (" + @IncludedColumns + ") "
					ELSE " "
				END + CHAR(13) 
			+ CASE WHEN @IsFiltered = 1 THEN "WHERE " + @FilteredDefinition + " "
				ELSE " "
				END  
			+ "WITH ( ONLINE = " + CASE @LOB WHEN 0 THEN "ON" ELSE "OFF" END + ", DROP_EXISTING = ON, " 
			+ "DATA_COMPRESSION = " + @DataCompressionDesc + ", "
			+ @IndexOptions + ")" 
			+	CASE	@PartitionScheme
						WHEN "" THEN " ON " + QUOTENAME(@FileGroupName)
						ELSE " ON " + @PartitionScheme + "(" + QUOTENAME(@PartitionColumn) + ")"
				END
			+ ";"  

		IF @indexType IN (1, 2) AND @SkipXMLSpatial = 0
		BEGIN
			INSERT	INTO DBA.dbo.FilegroupMgtRebuilds
					( DatabaseName,
						FilegroupName,
						SQL )
			VALUES
					( DB_NAME(),
						"{{FilegroupName}}",
						@TSQLScripCreationIndex )
		END

		FETCH NEXT FROM CursorIndex INTO @SchemaName, @TableName, @IndexName, @PartitionScheme, @PartitionColumn,
			@is_unique, @IndexTypeDesc, @IndexOptions, @FileGroupName, @IndexType, @ObjectID, @IndexID, @IsFiltered,
			@FilteredDefinition, @DataCompressionDesc
	END

	CLOSE CursorIndex
	DEALLOCATE CursorIndex' AS NVARCHAR(MAX)) 
		,'{{DatabaseName}}', @DatabaseName)
		,'{{FilegroupName}}', @FilegroupName)
		,'"','''') 

	IF @Debug = 0
	BEGIN
		EXEC (@SQL);
	END
	ELSE
	BEGIN
		PRINT @SQL;
	END

	FETCH NEXT FROM CursorFilegroup INTO @FileGroupMgtID, @DatabaseName, @FilegroupName
END
CLOSE CursorFilegroup
DEALLOCATE CursorFilegroup

--Remove duplicate due to partitioning
DECLARE CursorDuplicates CURSOR
READ_ONLY
FOR 
SELECT 
	DatabaseName,
	MIN(FilegroupMgtRebuildsID)
FROM dbo.FilegroupMgtRebuilds
GROUP BY DatabaseName,
	SQL
HAVING COUNT(*)>1

OPEN CursorDuplicates

FETCH NEXT FROM CursorDuplicates INTO @DatabaseName, @FilegroupMgtRebuildsID
WHILE (@@fetch_status <> -1)
BEGIN
	IF (@@fetch_status <> -2)
	BEGIN
		DELETE FROM
			dbo.FilegroupMgtRebuilds
		WHERE
			SQL IN ( SELECT
							SQL
						FROM
							dbo.FilegroupMgtRebuilds
						WHERE
							FilegroupMgtRebuildsID = @FilegroupMgtRebuildsID
							AND DatabaseName = @DatabaseName )
			AND FilegroupMgtRebuildsID <> @FilegroupMgtRebuildsID
	END
	FETCH NEXT FROM CursorDuplicates INTO @DatabaseName, @FilegroupMgtRebuildsID
END

CLOSE CursorDuplicates
DEALLOCATE CursorDuplicates
GO