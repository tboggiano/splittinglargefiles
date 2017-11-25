CREATE TABLE dbo.FilegroupMgtRebuilds(
	FilegroupMgtRebuildsID int IDENTITY(1,1) NOT NULL,
	DatabaseName nvarchar(128) NOT NULL,
	FilegroupName nvarchar(128) NOT NULL,
	SQL nvarchar(max) NOT NULL,
 CONSTRAINT PK_FilegroupMgtRebuilds PRIMARY KEY CLUSTERED 
(
	FilegroupMgtRebuildsID ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON))