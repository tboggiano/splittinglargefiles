CREATE TABLE dbo.FilegroupMgt(
	FilegroupMgtID int IDENTITY(1,1) NOT NULL,
	DatabaseName nvarchar(128) NOT NULL,
	FilegroupName nvarchar(128) NOT NULL,
	AddedDatatimeGMT datetime NOT NULL CONSTRAINT DF_FilegroupMgt_AddedDatatimeGMT  DEFAULT (getutcdate()),
	RebuildComplete bit NOT NULL CONSTRAINT DF_FilegroupMgt_RebuildComplete  DEFAULT ((0)),
	ResizeComplete bit NOT NULL CONSTRAINT DF_FilegroupMgt_ResizeComplete  DEFAULT ((0)),
	DropFile bit NOT NULL CONSTRAINT DF_FilegroupMgt_DropFile  DEFAULT ((0)),
	DroppedFile bit NOT NULL CONSTRAINT DF_FilegroupMgt_DroppedFile  DEFAULT ((0)),
 CONSTRAINT PK_FilegroupMgt PRIMARY KEY CLUSTERED 
(
	FilegroupMgtID ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON))