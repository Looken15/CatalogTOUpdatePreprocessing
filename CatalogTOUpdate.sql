--объявляем необходимые переменные
declare @string nvarchar(MAX)
declare @count int
declare @path nvarchar(MAX)
declare @oldPath nvarchar(MAX)
declare @bulk nvarchar(MAX)
declare @maxId int
declare @minId int

--очищаем таблицы от старых данных
delete from CatalogTO.gc_maintenance_details
delete from CatalogTO.gc_maintenance_vehicles
delete from CatalogTO.gc_maintenance_vehicle_images_assoc
delete from CatalogTO.gc_maintenance_modifications
delete from CatalogTO.gc_maintenance_brands

----------------------------------------------BRANDS--------------------------------------------------
set @path = 'L:\\IntermediateTemporaryFiles\CatalogTOUpdate\brandsPreprocessed.txt'

create table #longStrings1(
	string nvarchar(MAX)
);

--читаем данные одной строкой из файла
create table #temp1 (res nvarchar(MAX))
set @bulk = 'insert into #temp1 SELECT SUBSTRING(BulkColumn, CHARINDEX(''('', BulkColumn, 0) + 1, LEN(BulkColumn) - 47) as string
	FROM OPENROWSET (BULK ''' +  @path + ''' , SINGLE_CLOB) as correlation_name;'
exec(@bulk)

--получаем строку
select @string = res from #temp1

--считаем сколько позиций в строке
select @count = (select count(*) from split(@string, '),('))

--разбиваем по позициям в таблицу
insert into #longStrings1 (string)
	select top(@count) *
	from split(@string, '),(');

--запихиваем в конечную таблицу
SET IDENTITY_INSERT [CatalogTO].[gc_maintenance_brands] ON
INSERT INTO [CatalogTO].[gc_maintenance_brands]
           ([id]
		   ,[name]
           ,[createdAt]
           ,[updatedAt]
           ,[deletedAt]
           ,[isDeleted]
           ,[isPublished]
           ,[isS4AB])
SELECT DISTINCT
 S.a.value('(/H/r)[1]', 'INT')  AS Id
,S.a.value('(/H/r)[2]', 'VARCHAR(MAX)') AS Name
,TRY_CONVERT(datetime, S.a.value('(/H/r)[3]', 'VARCHAR(MAX)'), 120) AS createdAt
,TRY_CONVERT(datetime, S.a.value('(/H/r)[4]', 'VARCHAR(MAX)'), 120) AS updatedAt
,TRY_CONVERT(datetime, S.a.value('(/H/r)[5]', 'VARCHAR(MAX)'), 120) AS deletedAt
,S.a.value('(/H/r)[6]', 'VARCHAR(MAX)') AS isDeleted
,S.a.value('(/H/r)[7]', 'VARCHAR(MAX)') AS isPublished,
0 as isS4AB
FROM
(SELECT *,CAST (N'<H><r>' + REPLACE(REPLACE(string, '''', ''), '@', '</r><r>')  + '</r></H>' AS XML) AS [vals]
FROM #longStrings1) d 
CROSS APPLY d.[vals].nodes('/H/r') S(a) order by Id 
SET IDENTITY_INSERT [CatalogTO].[gc_maintenance_brands] OFF

drop table #temp1
drop table #longStrings1

----------------------------------------------MODIFICATIONS_NEW--------------------------------------------------

set @path = 'L:\\IntermediateTemporaryFiles\CatalogTOUpdate\modificationsPreprocessed.txt'

create table #longStrings2(
	string nvarchar(MAX)
);

create table #temp2 (res nvarchar(MAX))
set @bulk = 'insert into #temp2 SELECT SUBSTRING(BulkColumn, CHARINDEX(''('', BulkColumn, 0) + 1, LEN(BulkColumn) - 47) as string
	FROM OPENROWSET (BULK ''' +  @path + ''' , SINGLE_CLOB) as correlation_name;'
exec(@bulk)

select @string = res from #temp2

select @count = (select count(*) from split(@string, '),('))

insert into #longStrings2 (string)
	select top(@count) *
	from split(@string, '),(');

SET IDENTITY_INSERT [CatalogTO].[gc_maintenance_modifications] ON
INSERT INTO [CatalogTO].[gc_maintenance_modifications]
           ([id]
		   ,[name]
           ,[code]
           ,[brandId]
           ,[productionYearFrom]
           ,[productionYearTo]
           ,[createdAt]
           ,[updatedAt]
           ,[deletedAt]
           ,[isDeleted]
           ,[isPublished]
           ,[isS4AB])
SELECT DISTINCT
 S.a.value('(/H/r)[1]', 'INT')  AS id
,S.a.value('(/H/r)[2]', 'VARCHAR(MAX)') AS name
,S.a.value('(/H/r)[3]', 'VARCHAR(MAX)') AS code
,S.a.value('(/H/r)[4]', 'INT') AS brandId
,S.a.value('(/H/r)[5]', 'INT') AS productionYearFrom
,S.a.value('(/H/r)[6]', 'INT') AS productionYearTo
,try_convert(datetime, S.a.value('(/H/r)[7]', 'VARCHAR(MAX)'), 120) AS createdAt
,try_convert(datetime, S.a.value('(/H/r)[8]', 'VARCHAR(MAX)'), 120) AS updatedAt
,try_convert(datetime, S.a.value('(/H/r)[9]', 'VARCHAR(MAX)'), 120) AS deletedAt
,S.a.value('(/H/r)[10]', 'BIT') AS isDeleted
,S.a.value('(/H/r)[11]', 'BIT') AS isPublished,
0 as isS4AB
FROM
(SELECT *,CAST (N'<H><r>' + REPLACE(REPLACE(string, '''', ''), '@', '</r><r>')  + '</r></H>' AS XML) AS [vals]
FROM #longStrings2) d 
CROSS APPLY d.[vals].nodes('/H/r') S(a)
SET IDENTITY_INSERT [CatalogTO].[gc_maintenance_modifications] OFF

drop table #temp2
drop table #longStrings2

----------------------------------------------MODIFICATIONS_OLD--------------------------------------------------
set @oldPath = 'L:\\IntermediateTemporaryFiles\CatalogTOUpdate\modificationsOld.csv'

create table #temp3 (res nvarchar(MAX))
set @bulk = 'insert into #temp3 select SUBSTRING(BulkColumn, 4, LEN(BulkColumn)) as string
	FROM OPENROWSET (BULK ''' +  @oldPath + ''' , SINGLE_CLOB) as correlation_name;'
exec(@bulk)

select @string = res from #temp3

create table #longStrings3(
	string nvarchar(MAX)
);

select @count = (select count(*) from split(@string, CHAR(10)));

with abc as
(
	select Item, ROW_NUMBER() OVER(ORDER BY Item) as rownum
	from split(@string, CHAR(10))
)
insert into #longStrings3 (string)
select Item
from abc
where rownum > 1 and rownum < @count

select @maxId = (select max(id) from [CatalogTO].[gc_maintenance_modifications]) + 1;

CREATE TABLE #splitted3(
	[id] [int] NOT NULL,
	[name] [nvarchar](55) NOT NULL,
	[code] [nvarchar](100) NOT NULL,
	[brandId] [int] NOT NULL,
	[productionYearFrom] [int] NOT NULL,
	[productionYearTo] [int] NULL,
	[createdAt] [nvarchar](50) NOT NULL,
	[updatedAt] [nvarchar](50) NULL,
	[deletedAt] [nvarchar](50) NULL,
	[isDeleted] [bit] NOT NULL,
	[isPublished] [bit] NOT NULL,
	[isS4AB] [bit] NULL)

insert into #splitted3
SELECT DISTINCT
 S.a.value('(/H/r)[1]', 'INT') AS id
,S.a.value('(/H/r)[2]', 'VARCHAR(MAX)') AS name
,S.a.value('(/H/r)[3]', 'VARCHAR(MAX)') AS code
,S.a.value('(/H/r)[4]', 'INT') AS brandId
,S.a.value('(/H/r)[5]', 'INT') AS productionYearFrom
,S.a.value('(/H/r)[6]', 'INT') AS productionYearTo
,try_convert(datetime, S.a.value('(/H/r)[7]', 'VARCHAR(MAX)'), 120) AS createdAt
,try_convert(datetime, S.a.value('(/H/r)[8]', 'VARCHAR(MAX)'), 120) AS updatedAt
,try_convert(datetime, S.a.value('(/H/r)[9]', 'VARCHAR(MAX)'), 120) AS deletedAt
,S.a.value('(/H/r)[10]', 'BIT') AS isDeleted
,S.a.value('(/H/r)[11]', 'BIT') AS isPublished
,1 as isS4AB
FROM
(SELECT *,CAST (N'<H><r>' + REPLACE(string, ';', '</r><r>')  + '</r></H>' AS XML) AS [vals]
FROM #longStrings3) d 
CROSS APPLY d.[vals].nodes('/H/r') S(a)

select @minId = (select min(id) from #splitted3);

create table #modificationDict(
	oldId int,
	newId int)

insert into #modificationDict
select id ,id - @minId + @maxId as newId
from #splitted3

SET IDENTITY_INSERT [CatalogTO].[gc_maintenance_modifications] ON
INSERT INTO [CatalogTO].[gc_maintenance_modifications]
           ([id]
		   ,[name]
           ,[code]
           ,[brandId]
           ,[productionYearFrom]
           ,[productionYearTo]
           ,[createdAt]
           ,[updatedAt]
           ,[deletedAt]
           ,[isDeleted]
           ,[isPublished]
           ,[isS4AB])
select id - @minId + @maxId as id, name, code, brandId, productionYearFrom, productionYearTo, createdAt, updatedAt, deletedAt, isDeleted, isPublished, isS4AB
from #splitted3
SET IDENTITY_INSERT [CatalogTO].[gc_maintenance_modifications] OFF

drop table #temp3
drop table #longStrings3
drop table #splitted3


----------------------------------------------IMAGES_NEW--------------------------------------------------
set @path = 'L:\\IntermediateTemporaryFiles\CatalogTOUpdate\imagesPreprocessed.txt'

create table #longStrings4(
	string nvarchar(MAX)
);

create table #temp4 (res nvarchar(MAX))

set @bulk = 'insert into #temp4 SELECT SUBSTRING(BulkColumn, CHARINDEX(''('', BulkColumn, 0) + 1, LEN(BulkColumn) - 47) as string
	FROM OPENROWSET (BULK ''' +  @path + ''' , SINGLE_CLOB) as correlation_name;'
exec(@bulk)

select @string = res from #temp4

select @count = (select count(*) from split(@string, '),('))

insert into #longStrings4 (string)
	select top(@count) *
	from split(@string, '),(');

create table #splitted4(
	id int,
	modificationId int,
	imageDestFolder nvarchar(MAX),
	imageHash nvarchar(MAX),
	updatedAt Datetime,
	isS4AB bit)

insert into #splitted4
SELECT DISTINCT
 S.a.value('(/H/r)[1]', 'INT') AS id
,NullIf(S.a.value('(/H/r)[2]', 'VARCHAR(MAX)'), 'NULL') AS modificationId
,S.a.value('(/H/r)[3]', 'VARCHAR(MAX)') AS imageDestFolder
,S.a.value('(/H/r)[4]', 'VARCHAR(MAX)') AS imageHash
,try_convert(datetime, S.a.value('(/H/r)[5]', 'VARCHAR(MAX)'), 120) AS updatedAt
,0 as isS4AB
FROM
(SELECT *,CAST (N'<H><r>' + REPLACE(REPLACE(string, '''', ''), '@', '</r><r>')  + '</r></H>' AS XML) AS [vals]
FROM #longStrings4) d 
CROSS APPLY d.[vals].nodes('/H/r') S(a)

ALTER TABLE [CatalogTO].[gc_maintenance_modifications] NOCHECK CONSTRAINT ALL
SET IDENTITY_INSERT [CatalogTO].[gc_maintenance_vehicle_images_assoc] ON
INSERT INTO [CatalogTO].[gc_maintenance_vehicle_images_assoc]
           ([id]
		   ,[modificationId]
           ,[imageDestFolder]
           ,[imageHash]
           ,[updatedAt]
           ,[isS4AB])
select id, case
			when 1 = (select cast(count(1) as bit) from #modificationDict where oldId = modificationId)
				then (select newId from #modificationDict where oldId = modificationId)
			else
				modificationId
		end as modificationId, imageDestFolder, imageHash, updatedAt, isS4AB
from #splitted4 order by id
SET IDENTITY_INSERT [CatalogTO].[gc_maintenance_vehicle_images_assoc] OFF
ALTER TABLE [CatalogTO].[gc_maintenance_modifications] WITH CHECK CHECK CONSTRAINT ALL

drop table #longStrings4
drop table #temp4
drop table #splitted4



----------------------------------------------IMAGES_OLD--------------------------------------------------
set @oldPath = 'L:\\IntermediateTemporaryFiles\CatalogTOUpdate\imagesOld.csv'

create table #temp5 (res nvarchar(MAX))
set @bulk = 'insert into #temp5 select SUBSTRING(BulkColumn, 4, LEN(BulkColumn)) as string
	FROM OPENROWSET (BULK ''' +  @oldPath + ''' , SINGLE_CLOB) as correlation_name;'
exec(@bulk)

select @string = res from #temp5

create table #longStrings5(
	string nvarchar(MAX)
);

select @count = (select count(*) from split(@string, CHAR(10)));

with abc as
(
	select Item, ROW_NUMBER() OVER(ORDER BY Item) as rownum
	from split(@string, CHAR(10))
)
insert into #longStrings5 (string)
select Item
from abc
where rownum > 1 and rownum < @count

create table #splitted5(
	id int,
	modificationId int,
	imageDestFolder nvarchar(MAX),
	imageHash nvarchar(MAX),
	updatedAt Datetime,
	isS4AB bit)

insert into #splitted5
SELECT DISTINCT
 S.a.value('(/H/r)[1]', 'INT') AS id
,NullIf(S.a.value('(/H/r)[2]', 'VARCHAR(MAX)'), 'NULL') AS modificationId
,S.a.value('(/H/r)[3]', 'VARCHAR(MAX)') AS imageDestFolder
,S.a.value('(/H/r)[4]', 'VARCHAR(MAX)') AS imageHash
,try_convert(datetime, S.a.value('(/H/r)[5]', 'VARCHAR(MAX)'), 120) AS updatedAt
,1 as isS4AB
FROM
(SELECT *,CAST (N'<H><r>' + REPLACE(REPLACE(string, '''', ''), ';', '</r><r>')  + '</r></H>' AS XML) AS [vals]
FROM #longStrings5) d 
CROSS APPLY d.[vals].nodes('/H/r') S(a)

select @maxId = (select max(id) from [CatalogTO].gc_maintenance_vehicle_images_assoc) + 1;
select @minId = (select min(id) from #splitted5);

ALTER TABLE [CatalogTO].[gc_maintenance_modifications] NOCHECK CONSTRAINT ALL
ALTER TABLE [CatalogTO].[gc_maintenance_vehicle_images_assoc] NOCHECK CONSTRAINT ALL
SET IDENTITY_INSERT [CatalogTO].[gc_maintenance_vehicle_images_assoc] ON
INSERT INTO [CatalogTO].[gc_maintenance_vehicle_images_assoc]
           ([id]
		   ,[modificationId]
           ,[imageDestFolder]
           ,[imageHash]
           ,[updatedAt]
           ,[isS4AB])
select id - @minId + @maxId, case
			when 1 = (select cast(count(1) as bit) from #modificationDict where oldId = modificationId)
				then (select newId from #modificationDict where oldId = modificationId)
			else
				modificationId
		end as modificationId, imageDestFolder, imageHash, updatedAt, isS4AB
from #splitted5 order by id
SET IDENTITY_INSERT [CatalogTO].[gc_maintenance_vehicle_images_assoc] OFF

drop table #splitted5
drop table #temp5
drop table #longStrings5




----------------------------------------------VEHICLES_NEW--------------------------------------------------
set @path = 'L:\\IntermediateTemporaryFiles\CatalogTOUpdate\vehiclesPreprocessed.txt'

create table #longStrings6(
	string nvarchar(MAX)
);

create table #temp6 (res nvarchar(MAX))

set @bulk = 'insert into #temp6 SELECT SUBSTRING(BulkColumn, CHARINDEX(''('', BulkColumn, 0) + 1, LEN(BulkColumn) - 47) as string
	FROM OPENROWSET (BULK ''' +  @path + ''' , SINGLE_CLOB) as correlation_name;'
exec(@bulk)

select @string = res from #temp6

select @count = (select count(*) from split(@string, '),('))

insert into #longStrings6 (string)
	select top(@count) *
	from split(@string, '),(');

create table #splitted6(
	[id] [int] NOT NULL,
	[modificationId] [int] NOT NULL,
	[modify] [nvarchar](MAX) NOT NULL,
	[engineCode] [nvarchar](MAX) NOT NULL,
	[engineVolume] [decimal](3, 1) NULL,
	[enginePower] [nvarchar](MAX) NOT NULL,
	[engineFuel] [nvarchar](MAX) NOT NULL,
	[productionYearFrom] [int] NOT NULL,
	[productionYearTo] [int] NULL,
	[createdAt] [datetime] NOT NULL,
	[updatedAt] [datetime] NULL,
	[deletedAt] [datetime] NULL,
	[isDeleted] [bit] NOT NULL,
	[isPublished] [bit] NOT NULL,
	[MPId] [int] NULL,
	[modelSpecialCode] [nvarchar](MAX) NULL,
	[isS4AB] [bit] NULL,
	)


insert into #splitted6
SELECT DISTINCT
 S.a.value('(/H/r)[1]', 'INT') AS id
,NullIf(S.a.value('(/H/r)[2]', 'VARCHAR(MAX)'), 'NULL') AS modificationId
,NullIf(S.a.value('(/H/r)[3]', 'VARCHAR(MAX)'), 'NULL') AS modify
,NullIf(S.a.value('(/H/r)[4]', 'VARCHAR(MAX)'), 'NULL') AS engineCode
,NullIf(S.a.value('(/H/r)[5]', 'VARCHAR(MAX)'), 'NULL') AS engineVolume
,NullIf(S.a.value('(/H/r)[6]', 'VARCHAR(MAX)'), 'NULL') AS enginePower
,NullIf(S.a.value('(/H/r)[7]', 'VARCHAR(MAX)'), 'NULL') AS engineFuel
,NullIf(S.a.value('(/H/r)[8]', 'VARCHAR(MAX)'), 'NULL') AS productionYearFrom
,NullIf(S.a.value('(/H/r)[9]', 'VARCHAR(MAX)'), 'NULL') AS productionYearTo
,try_convert(datetime, S.a.value('(/H/r)[10]', 'VARCHAR(MAX)'), 120) AS createdAt
,try_convert(datetime, S.a.value('(/H/r)[11]', 'VARCHAR(MAX)'), 120) AS updatedAt
,try_convert(datetime, S.a.value('(/H/r)[12]', 'VARCHAR(MAX)'), 120) AS deletedAt
,NullIf(S.a.value('(/H/r)[13]', 'VARCHAR(MAX)'), 'NULL') AS isDeleted
,NullIf(S.a.value('(/H/r)[14]', 'VARCHAR(MAX)'), 'NULL') AS isPublished
,NullIf(S.a.value('(/H/r)[15]', 'VARCHAR(MAX)'), 'NULL') AS MPId
,NullIf(S.a.value('(/H/r)[16]', 'VARCHAR(MAX)'), 'NULL') AS modelSpecialCode
,0 as isS4AB
FROM
(SELECT *,CAST (N'<H><r>' + REPLACE(REPLACE(string, '''', ''), '@', '</r><r>')  + '</r></H>' AS XML) AS [vals]
FROM #longStrings6) d 
CROSS APPLY d.[vals].nodes('/H/r') S(a)

ALTER TABLE [CatalogTO].[gc_maintenance_modifications] NOCHECK CONSTRAINT ALL
SET IDENTITY_INSERT [CatalogTO].[gc_maintenance_vehicles] ON
INSERT INTO [CatalogTO].[gc_maintenance_vehicles]
           ([id]
		   ,[modificationId]
           ,[modify]
           ,[engineCode]
           ,[engineVolume]
           ,[enginePower]
           ,[engineFuel]
           ,[productionYearFrom]
           ,[productionYearTo]
           ,[createdAt]
           ,[updatedAt]
           ,[deletedAt]
           ,[isDeleted]
           ,[isPublished]
           ,[MPId]
           ,[modelSpecialCode]
           ,[isS4AB])
select id, case
			when 1 = (select cast(count(1) as bit) from #modificationDict where oldId = modificationId)
				then (select newId from #modificationDict where oldId = modificationId)
			else
				modificationId
		end as modificationId, modify, engineCode, engineVolume, enginePower, engineFuel, productionYearFrom, productionYearTo, createdAt, updatedAt, deletedAt, isDeleted, isPublished, MPId, modelSpecialCode, isS4AB
from #splitted6 order by id
SET IDENTITY_INSERT [CatalogTO].[gc_maintenance_vehicles] OFF
ALTER TABLE [CatalogTO].[gc_maintenance_modifications] WITH CHECK CHECK CONSTRAINT ALL

drop table #splitted6
drop table #longStrings6
drop table #temp6


----------------------------------------------VEHICLES_OLD--------------------------------------------------
set @oldPath = 'L:\\IntermediateTemporaryFiles\CatalogTOUpdate\vehiclesOld.csv'

create table #temp7 (res nvarchar(MAX))
set @bulk = 'insert into #temp7 select SUBSTRING(BulkColumn, 4, LEN(BulkColumn)) as string
	FROM OPENROWSET (BULK ''' +  @oldPath + ''' , SINGLE_CLOB) as correlation_name;'
exec(@bulk)

select @string = res from #temp7

create table #longStrings7(
	string nvarchar(MAX)
);

select @count = (select count(*) from split(@string, CHAR(10)));

with abc as
(
	select Item, ROW_NUMBER() OVER(ORDER BY Item) as rownum
	from split(@string, CHAR(10))
)
insert into #longStrings7 (string)
select Item
from abc
where rownum > 1 and rownum < @count

create table #splitted7(
	[id] [int] NOT NULL,
	[modificationId] [int] NOT NULL,
	[modify] [nvarchar](MAX) NOT NULL,
	[engineCode] [nvarchar](MAX) NOT NULL,
	[engineVolume] [decimal](3, 1) NULL,
	[enginePower] [nvarchar](MAX) NOT NULL,
	[engineFuel] [nvarchar](MAX) NOT NULL,
	[productionYearFrom] [int] NOT NULL,
	[productionYearTo] [int] NULL,
	[createdAt] [datetime] NOT NULL,
	[updatedAt] [datetime] NULL,
	[deletedAt] [datetime] NULL,
	[isDeleted] [bit] NOT NULL,
	[isPublished] [bit] NOT NULL,
	[MPId] [int] NULL,
	[modelSpecialCode] [nvarchar](MAX) NULL,
	[isS4AB] [bit] NULL,
	)

insert into #splitted7
SELECT DISTINCT
 S.a.value('(/H/r)[1]', 'INT') AS id
,NullIf(S.a.value('(/H/r)[2]', 'VARCHAR(MAX)'), 'NULL') AS modificationId
,NullIf(S.a.value('(/H/r)[3]', 'VARCHAR(MAX)'), 'NULL') AS modify
,NullIf(S.a.value('(/H/r)[4]', 'VARCHAR(MAX)'), 'NULL') AS engineCode
,NullIf(S.a.value('(/H/r)[5]', 'VARCHAR(MAX)'), 'NULL') AS engineVolume
,NullIf(S.a.value('(/H/r)[6]', 'VARCHAR(MAX)'), 'NULL') AS enginePower
,NullIf(S.a.value('(/H/r)[7]', 'VARCHAR(MAX)'), 'NULL') AS engineFuel
,NullIf(S.a.value('(/H/r)[8]', 'VARCHAR(MAX)'), 'NULL') AS productionYearFrom
,NullIf(S.a.value('(/H/r)[9]', 'VARCHAR(MAX)'), 'NULL') AS productionYearTo
,try_convert(datetime, S.a.value('(/H/r)[10]', 'VARCHAR(MAX)'), 120) AS createdAt
,try_convert(datetime, S.a.value('(/H/r)[11]', 'VARCHAR(MAX)'), 120) AS updatedAt
,try_convert(datetime, S.a.value('(/H/r)[12]', 'VARCHAR(MAX)'), 120) AS deletedAt
,NullIf(S.a.value('(/H/r)[13]', 'VARCHAR(MAX)'), 'NULL') AS isDeleted
,NullIf(S.a.value('(/H/r)[14]', 'VARCHAR(MAX)'), 'NULL') AS isPublished
,NullIf(S.a.value('(/H/r)[15]', 'VARCHAR(MAX)'), 'NULL') AS MPId
,NullIf(S.a.value('(/H/r)[16]', 'VARCHAR(MAX)'), 'NULL') AS modelSpecialCode
,1 as isS4AB
FROM
(SELECT *,CAST (N'<H><r>' + REPLACE(REPLACE(string, '''', ''), ';', '</r><r>')  + '</r></H>' AS XML) AS [vals]
FROM #longStrings7) d 
CROSS APPLY d.[vals].nodes('/H/r') S(a)

select @maxId = (select max(id) from [CatalogTO].gc_maintenance_vehicles) + 1;
select @minId = (select min(id) from #splitted7);

ALTER TABLE [CatalogTO].[gc_maintenance_modifications] NOCHECK CONSTRAINT ALL
SET IDENTITY_INSERT [CatalogTO].[gc_maintenance_vehicles] ON
INSERT INTO [CatalogTO].[gc_maintenance_vehicles]
           ([id]
		   ,[modificationId]
           ,[modify]
           ,[engineCode]
           ,[engineVolume]
           ,[enginePower]
           ,[engineFuel]
           ,[productionYearFrom]
           ,[productionYearTo]
           ,[createdAt]
           ,[updatedAt]
           ,[deletedAt]
           ,[isDeleted]
           ,[isPublished]
           ,[MPId]
           ,[modelSpecialCode]
           ,[isS4AB])
select id - @minId + @maxId, case
			when 1 = (select cast(count(1) as bit) from #modificationDict where oldId = modificationId)
				then (select newId from #modificationDict where oldId = modificationId)
			else
				modificationId
		end as modificationId, modify, engineCode, engineVolume, enginePower, engineFuel, productionYearFrom, productionYearTo, createdAt, updatedAt, deletedAt, isDeleted, isPublished, MPId, modelSpecialCode, isS4AB
from #splitted7 order by id
SET IDENTITY_INSERT [CatalogTO].[gc_maintenance_vehicles] OFF
ALTER TABLE [CatalogTO].[gc_maintenance_modifications] WITH CHECK CHECK CONSTRAINT ALL

create table #vehicleDict(
	oldId int,
	newId int)

insert into #vehicleDict
select id ,id - @minId + @maxId as newId
from #splitted7

drop table #splitted7
drop table #longStrings7
drop table #temp7
drop table #modificationDict

--select * from #vehicleDict

----------------------------------------------DETAILS_NEW--------------------------------------------------

set @path = 'L:\\IntermediateTemporaryFiles\CatalogTOUpdate\detailsPreprocessed.txt'

create table #longStrings8(
	string nvarchar(MAX)
);

create table #temp8 (res nvarchar(MAX))

set @bulk = 'insert into #temp8 SELECT SUBSTRING(BulkColumn, CHARINDEX(''('', BulkColumn, 0) + 1, LEN(BulkColumn) - 0) as string
	FROM OPENROWSET (BULK ''' +  @path + ''' , SINGLE_CLOB) as correlation_name;'
exec(@bulk)

select @string = res from #temp8

select @count = (select count(*) from split(@string, '),('))

insert into #longStrings8 (string)
	select top(@count) *
	from split(@string, '),(');

create table #splitted8(
	[id] [int] NOT NULL,
	[vehicleId] [int] NOT NULL,
	[name] [nvarchar](100) NOT NULL,
	[count] [int] NOT NULL,
	[partCode] [nvarchar](50) NOT NULL,
	[commentary] [nvarchar](max) NULL,
	[createdAt] [datetime] NOT NULL,
	[updatedAt] [datetime] NULL,
	[deletedAt] [datetime] NULL,
	[isDeleted] [bit] NOT NULL,
	[isPublished] [bit] NOT NULL,
	[isS4AB] [bit] NULL
	)


insert into #splitted8
SELECT DISTINCT
 S.a.value('(/H/r)[1]', 'INT') AS id
,NullIf(S.a.value('(/H/r)[2]', 'VARCHAR(MAX)'), 'NULL') AS vehicleId
,NullIf(S.a.value('(/H/r)[3]', 'VARCHAR(MAX)'), 'NULL') AS name
,NullIf(S.a.value('(/H/r)[4]', 'VARCHAR(MAX)'), 'NULL') AS count
,NullIf(S.a.value('(/H/r)[5]', 'VARCHAR(MAX)'), 'NULL') AS partCode
,NullIf(S.a.value('(/H/r)[6]', 'VARCHAR(MAX)'), 'NULL') AS commentary
,try_convert(datetime, S.a.value('(/H/r)[7]', 'VARCHAR(MAX)'), 120) AS createdAt
,try_convert(datetime, S.a.value('(/H/r)[8]', 'VARCHAR(MAX)'), 120) AS updatedAt
,try_convert(datetime, S.a.value('(/H/r)[9]', 'VARCHAR(MAX)'), 120) AS deletedAt
,NullIf(S.a.value('(/H/r)[10]', 'VARCHAR(MAX)'), 'NULL') AS isDeleted
,NullIf(S.a.value('(/H/r)[11]', 'VARCHAR(MAX)'), 'NULL') AS isPublished
,0 as isS4AB
FROM
(SELECT *,CAST (N'<H><r>' + REPLACE(REPLACE(string, '''', ''), '@', '</r><r>')  + '</r></H>' AS XML) AS [vals]
FROM #longStrings8) d 
CROSS APPLY d.[vals].nodes('/H/r') S(a)

ALTER TABLE [CatalogTO].[gc_maintenance_modifications] NOCHECK CONSTRAINT ALL
ALTER TABLE [CatalogTO].[gc_maintenance_vehicles] NOCHECK CONSTRAINT ALL
SET IDENTITY_INSERT [CatalogTO].[gc_maintenance_details] ON
INSERT INTO [CatalogTO].[gc_maintenance_details]
           ([id]
		   ,[vehicleId]
           ,[name]
           ,[count]
           ,[partCode]
           ,[commentary]
           ,[createdAt]
           ,[updatedAt]
           ,[deletedAt]
           ,[isDeleted]
           ,[isPublished]
           ,[isS4AB])
select id, case
			when 1 = (select cast(count(1) as bit) from #vehicleDict where oldId = vehicleId)
				then (select newId from #vehicleDict where oldId = vehicleId)
			else
				vehicleId
		end as vehicleId, name, count, partCode, commentary, createdAt, updatedAt, deletedAt, isDeleted, isPublished, isS4AB
from #splitted8 order by id
SET IDENTITY_INSERT [CatalogTO].[gc_maintenance_details] OFF
ALTER TABLE [CatalogTO].[gc_maintenance_modifications] WITH CHECK CHECK CONSTRAINT ALL

drop table #splitted8
drop table #longStrings8
drop table #temp8


----------------------------------------------DETAILS_OLD--------------------------------------------------
set @oldPath = 'L:\\IntermediateTemporaryFiles\CatalogTOUpdate\detailsOld.csv'

create table #temp9 (res nvarchar(MAX))
set @bulk = 'insert into #temp9 select SUBSTRING(BulkColumn, 4, LEN(BulkColumn)) as string
	FROM OPENROWSET (BULK ''' +  @oldPath + ''' , SINGLE_CLOB) as correlation_name;'
exec(@bulk)

select @string = res from #temp9

create table #longStrings9(
	string nvarchar(MAX)
);

select @count = (select count(*) from split(@string, CHAR(10)));

with abc as
(
	select Item, ROW_NUMBER() OVER(ORDER BY Item) as rownum
	from split(@string, CHAR(10))
)
insert into #longStrings9 (string)
select Item
from abc
where rownum > 1 and rownum < @count

create table #splitted9(
	[id] [int] NOT NULL,
	[vehicleId] [int] NOT NULL,
	[name] [nvarchar](100) NOT NULL,
	[count] [int] NOT NULL,
	[partCode] [nvarchar](50) NOT NULL,
	[commentary] [nvarchar](max) NULL,
	[createdAt] [datetime] NOT NULL,
	[updatedAt] [datetime] NULL,
	[deletedAt] [datetime] NULL,
	[isDeleted] [bit] NOT NULL,
	[isPublished] [bit] NOT NULL,
	[isS4AB] [bit] NULL
	)

insert into #splitted9
SELECT DISTINCT
 S.a.value('(/H/r)[1]', 'INT') AS id
,NullIf(S.a.value('(/H/r)[2]', 'VARCHAR(MAX)'), 'NULL') AS vehicleId
,NullIf(S.a.value('(/H/r)[3]', 'VARCHAR(MAX)'), 'NULL') AS name
,NullIf(S.a.value('(/H/r)[4]', 'VARCHAR(MAX)'), 'NULL') AS count
,NullIf(S.a.value('(/H/r)[5]', 'VARCHAR(MAX)'), 'NULL') AS partCode
,NullIf(S.a.value('(/H/r)[6]', 'VARCHAR(MAX)'), 'NULL') AS commentary
,try_convert(datetime, S.a.value('(/H/r)[7]', 'VARCHAR(MAX)'), 120) AS createdAt
,try_convert(datetime, S.a.value('(/H/r)[8]', 'VARCHAR(MAX)'), 120) AS updatedAt
,try_convert(datetime, S.a.value('(/H/r)[9]', 'VARCHAR(MAX)'), 120) AS deletedAt
,NullIf(S.a.value('(/H/r)[10]', 'VARCHAR(MAX)'), 'NULL') AS isDeleted
,NullIf(S.a.value('(/H/r)[11]', 'VARCHAR(MAX)'), 'NULL') AS isPublished
,1 as isS4AB
FROM
(SELECT *,CAST (N'<H><r>' + REPLACE(REPLACE(string, '''', ''), ';', '</r><r>')  + '</r></H>' AS XML) AS [vals]
FROM #longStrings9) d 
CROSS APPLY d.[vals].nodes('/H/r') S(a)

select @maxId = (select max(id) from [CatalogTO].gc_maintenance_details) + 1;
select @minId = (select min(id) from #splitted9);

ALTER TABLE [CatalogTO].[gc_maintenance_modifications] NOCHECK CONSTRAINT ALL
ALTER TABLE [CatalogTO].[gc_maintenance_vehicles] NOCHECK CONSTRAINT ALL
SET IDENTITY_INSERT [CatalogTO].[gc_maintenance_details] ON
INSERT INTO [CatalogTO].[gc_maintenance_details]
           ([id]
		   ,[vehicleId]
           ,[name]
           ,[count]
           ,[partCode]
           ,[commentary]
           ,[createdAt]
           ,[updatedAt]
           ,[deletedAt]
           ,[isDeleted]
           ,[isPublished]
           ,[isS4AB])
select id - @minId + @maxId, case
			when 1 = (select cast(count(1) as bit) from #vehicleDict where oldId = vehicleId)
				then (select newId from #vehicleDict where oldId = vehicleId)
			else
				vehicleId
		end as vehicleId, name, count, partCode, commentary, createdAt, updatedAt, deletedAt, isDeleted, isPublished, isS4AB
from #splitted9 order by id
SET IDENTITY_INSERT [CatalogTO].[gc_maintenance_details] OFF
ALTER TABLE [CatalogTO].[gc_maintenance_modifications] WITH CHECK CHECK CONSTRAINT ALL

drop table #splitted9
drop table #longStrings9
drop table #temp9

drop table #vehicleDict



----------------------------------------------DETAILS_UPDATE--------------------------------------------------

set @oldPath = 'L:\\IntermediateTemporaryFiles\CatalogTOUpdate\detailsUpdate.csv'

create table #temp10 (res nvarchar(MAX))
set @bulk = 'insert into #temp10 select SUBSTRING(BulkColumn, 4, LEN(BulkColumn)) as string
	FROM OPENROWSET (BULK ''' +  @oldPath + ''' , SINGLE_CLOB) as correlation_name;'
exec(@bulk)

select @string = res from #temp10

create table #longStrings10(
	string nvarchar(MAX)
);

select @count = (select count(*) from split(@string, CHAR(10)));

with abc as
(
	select Item, ROW_NUMBER() OVER(ORDER BY Item) as rownum
	from split(@string, CHAR(10))
)
insert into #longStrings10 (string)
select Item
from abc
where rownum > 1 and rownum < @count

create table #splitted10(
	[id] [int] NOT NULL,
	[vehicleId] [int] NOT NULL,
	[name] [nvarchar](100) NOT NULL,
	[count] [int] NOT NULL,
	[partCode] [nvarchar](50) NOT NULL,
	[commentary] [nvarchar](max) NULL,
	[createdAt] [datetime] NOT NULL,
	[updatedAt] [datetime] NULL,
	[deletedAt] [datetime] NULL,
	[isDeleted] [bit] NOT NULL,
	[isPublished] [bit] NOT NULL,
	[isS4AB] [bit] NULL
	)

insert into #splitted10
SELECT DISTINCT
 S.a.value('(/H/r)[1]', 'INT') AS id
,NullIf(S.a.value('(/H/r)[2]', 'VARCHAR(MAX)'), 'NULL') AS vehicleId
,NullIf(S.a.value('(/H/r)[3]', 'VARCHAR(MAX)'), 'NULL') AS name
,NullIf(S.a.value('(/H/r)[4]', 'VARCHAR(MAX)'), 'NULL') AS count
,NullIf(S.a.value('(/H/r)[5]', 'VARCHAR(MAX)'), 'NULL') AS partCode
,NullIf(S.a.value('(/H/r)[6]', 'VARCHAR(MAX)'), 'NULL') AS commentary
,try_convert(datetime, S.a.value('(/H/r)[7]', 'VARCHAR(MAX)'), 120) AS createdAt
,try_convert(datetime, S.a.value('(/H/r)[8]', 'VARCHAR(MAX)'), 120) AS updatedAt
,try_convert(datetime, S.a.value('(/H/r)[9]', 'VARCHAR(MAX)'), 120) AS deletedAt
,NullIf(S.a.value('(/H/r)[10]', 'VARCHAR(MAX)'), 'NULL') AS isDeleted
,NullIf(S.a.value('(/H/r)[11]', 'VARCHAR(MAX)'), 'NULL') AS isPublished
,1 as isS4AB
FROM
(SELECT *,CAST (N'<H><r>' + REPLACE(REPLACE(string, '"', ''), ';', '</r><r>')  + '</r></H>' AS XML) AS [vals]
FROM #longStrings10) d 
CROSS APPLY d.[vals].nodes('/H/r') S(a)

UPDATE [CatalogTO].[gc_maintenance_details]
   SET [vehicleId] = fr.vehicleId
      ,[name] = fr.name
      ,[count] = fr.count
      ,[partCode] = fr.partCode
      ,[commentary] = fr.commentary
      ,[createdAt] = fr.createdAt
      ,[updatedAt] = fr.updatedAt
      ,[deletedAt] = fr.deletedAt
      ,[isDeleted] = fr.isDeleted
      ,[isPublished] = fr.isPublished
      ,[isS4AB] = fr.isS4AB
from
	#splitted10 as fr
where [CatalogTO].[gc_maintenance_details].id = fr.id


drop table #longStrings10
drop table #temp10
drop table #splitted10



----------------------------------------------VEHICLES_UPDATE--------------------------------------------------
set @oldPath = 'L:\\IntermediateTemporaryFiles\CatalogTOUpdate\vehiclesUpdate.csv'

create table #temp11 (res nvarchar(MAX))
set @bulk = 'insert into #temp11 select SUBSTRING(BulkColumn, 4, LEN(BulkColumn)) as string
	FROM OPENROWSET (BULK ''' +  @oldPath + ''' , SINGLE_CLOB) as correlation_name;'
exec(@bulk)

select @string = res from #temp11

create table #longStrings11(
	string nvarchar(MAX)
);

select @count = (select count(*) from split(@string, CHAR(10)));

with abc as
(
	select Item, ROW_NUMBER() OVER(ORDER BY Item) as rownum
	from split(@string, CHAR(10))
)
insert into #longStrings11 (string)
select Item
from abc
where rownum > 1 and rownum < @count

select * from #longStrings11

create table #splitted11(
	[id] [int] NOT NULL,
	[modificationId] [int] NOT NULL,
	[modify] [nvarchar](MAX) NOT NULL,
	[engineCode] [nvarchar](MAX) NOT NULL,
	[engineVolume] [decimal](3, 1) NULL,
	[enginePower] [nvarchar](MAX) NOT NULL,
	[engineFuel] [nvarchar](MAX) NOT NULL,
	[productionYearFrom] [int] NOT NULL,
	[productionYearTo] [int] NULL,
	[createdAt] [datetime] NOT NULL,
	[updatedAt] [datetime] NULL,
	[deletedAt] [datetime] NULL,
	[isDeleted] [bit] NOT NULL,
	[isPublished] [bit] NOT NULL,
	[MPId] [int] NULL,
	[modelSpecialCode] [nvarchar](MAX) NULL,
	[isS4AB] [bit] NULL,
	)

insert into #splitted11
SELECT DISTINCT
 S.a.value('(/H/r)[1]', 'INT') AS id
,NullIf(S.a.value('(/H/r)[2]', 'VARCHAR(MAX)'), 'NULL') AS modificationId
,NullIf(S.a.value('(/H/r)[3]', 'VARCHAR(MAX)'), 'NULL') AS modify
,NullIf(S.a.value('(/H/r)[4]', 'VARCHAR(MAX)'), 'NULL') AS engineCode
,NullIf(S.a.value('(/H/r)[5]', 'VARCHAR(MAX)'), 'NULL') AS engineVolume
,NullIf(S.a.value('(/H/r)[6]', 'VARCHAR(MAX)'), 'NULL') AS enginePower
,NullIf(S.a.value('(/H/r)[7]', 'VARCHAR(MAX)'), 'NULL') AS engineFuel
,NullIf(S.a.value('(/H/r)[8]', 'VARCHAR(MAX)'), 'NULL') AS productionYearFrom
,NullIf(S.a.value('(/H/r)[9]', 'VARCHAR(MAX)'), 'NULL') AS productionYearTo
,try_convert(datetime, S.a.value('(/H/r)[10]', 'VARCHAR(MAX)'), 120) AS createdAt
,try_convert(datetime, S.a.value('(/H/r)[11]', 'VARCHAR(MAX)'), 120) AS updatedAt
,try_convert(datetime, S.a.value('(/H/r)[12]', 'VARCHAR(MAX)'), 120) AS deletedAt
,NullIf(S.a.value('(/H/r)[13]', 'VARCHAR(MAX)'), 'NULL') AS isDeleted
,NullIf(S.a.value('(/H/r)[14]', 'VARCHAR(MAX)'), 'NULL') AS isPublished
,NullIf(S.a.value('(/H/r)[15]', 'VARCHAR(MAX)'), 'NULL') AS MPId
,NullIf(S.a.value('(/H/r)[16]', 'VARCHAR(MAX)'), 'NULL') AS modelSpecialCode
,1 as isS4AB
FROM
(SELECT *,CAST (N'<H><r>' + REPLACE(REPLACE(string, '''', ''), '@', '</r><r>')  + '</r></H>' AS XML) AS [vals]
FROM #longStrings11) d 
CROSS APPLY d.[vals].nodes('/H/r') S(a)

UPDATE [CatalogTO].[gc_maintenance_vehicles]
   SET [modificationId] = fr.modificationId
      ,[modify] = fr.modify
      ,[engineCode] = fr.engineCode
      ,[engineVolume] = fr.engineVolume
      ,[enginePower] = fr.enginePower
      ,[engineFuel] = fr.engineFuel
      ,[productionYearFrom] = fr.productionYearFrom
      ,[productionYearTo] = fr.productionYearTo
      ,[createdAt] = fr.createdAt
      ,[updatedAt] = fr.updatedAt
      ,[deletedAt] = fr.deletedAt
      ,[isDeleted] = fr.isDeleted
      ,[isPublished] = fr.isPublished
      ,[MPId] = fr.MPId
      ,[modelSpecialCode] = fr.modelSpecialCode
      ,[isS4AB] = fr.isS4AB
from #splitted11 as fr
 WHERE [CatalogTO].[gc_maintenance_vehicles].id = fr.id

drop table #longStrings11
drop table #temp11
drop table #splitted11



----------------------------------------------IMAGES_UPDATE--------------------------------------------------
--set @oldPath = 'L:\\IntermediateTemporaryFiles\CatalogTOUpdate\imagesUpdate.csv'

--create table #temp12 (res nvarchar(MAX))
--set @bulk = 'insert into #temp12 select SUBSTRING(BulkColumn, 4, LEN(BulkColumn)) as string
--	FROM OPENROWSET (BULK ''' +  @oldPath + ''' , SINGLE_CLOB) as correlation_name;'
--exec(@bulk)

--select @string = res from #temp12

--create table #longStrings12(
--	string nvarchar(MAX)
--);

--select @count = (select count(*) from split(@string, CHAR(10)));

--with abc as
--(
--	select Item, ROW_NUMBER() OVER(ORDER BY Item) as rownum
--	from split(@string, CHAR(10))
--)
--insert into #longStrings12 (string)
--select Item
--from abc
--where rownum > 1 and rownum < @count

--create table #splitted12(
--	id int,
--	modificationId int,
--	imageDestFolder nvarchar(MAX),
--	imageHash nvarchar(MAX),
--	updatedAt Datetime,
--	isS4AB bit)

--insert into #splitted12
--SELECT DISTINCT
-- S.a.value('(/H/r)[1]', 'INT') AS id
--,NullIf(S.a.value('(/H/r)[2]', 'VARCHAR(MAX)'), 'NULL') AS modificationId
--,S.a.value('(/H/r)[3]', 'VARCHAR(MAX)') AS imageDestFolder
--,S.a.value('(/H/r)[4]', 'VARCHAR(MAX)') AS imageHash
--,try_convert(datetime, S.a.value('(/H/r)[5]', 'VARCHAR(MAX)'), 120) AS updatedAt
--,1 as isS4AB
--FROM
--(SELECT *,CAST (N'<H><r>' + REPLACE(REPLACE(string, '''', ''), ';', '</r><r>')  + '</r></H>' AS XML) AS [vals]
--FROM #longStrings12) d 
--CROSS APPLY d.[vals].nodes('/H/r') S(a)

--UPDATE [CatalogTO].[gc_maintenance_vehicle_images_assoc]
--   SET [modificationId] = fr.modificationId
--      ,[imageDestFolder] = fr.imageDestFolder
--      ,[imageHash] = fr.imageHash
--      ,[updatedAt] = fr.updatedAt
--      ,[isS4AB] = fr.isS4AB
--from #splitted12 as fr
-- WHERE [CatalogTO].[gc_maintenance_vehicle_images_assoc].id = fr.id

-- drop table #longStrings12
--drop table #temp12
--drop table #splitted12




----------------------------------------------MODIFICATIONS_UPDATE--------------------------------------------------
--set @oldPath = 'L:\\IntermediateTemporaryFiles\CatalogTOUpdate\modificationsUpdate.csv'

--create table #temp13 (res nvarchar(MAX))
--set @bulk = 'insert into #temp13 select SUBSTRING(BulkColumn, 4, LEN(BulkColumn)) as string
--	FROM OPENROWSET (BULK ''' +  @oldPath + ''' , SINGLE_CLOB) as correlation_name;'
--exec(@bulk)

--select @string = res from #temp13

--create table #longStrings13(
--	string nvarchar(MAX)
--);

--select @count = (select count(*) from split(@string, CHAR(10)));

--with abc as
--(
--	select Item, ROW_NUMBER() OVER(ORDER BY Item) as rownum
--	from split(@string, CHAR(10))
--)
--insert into #longStrings13 (string)
--select Item
--from abc
--where rownum > 1 and rownum < @count

--select @maxId = (select max(id) from [CatalogTO].[gc_maintenance_modifications]) + 1;

--CREATE TABLE #splitted13(
--	[id] [int] NOT NULL,
--	[name] [nvarchar](55) NOT NULL,
--	[code] [nvarchar](100) NOT NULL,
--	[brandId] [int] NOT NULL,
--	[productionYearFrom] [int] NOT NULL,
--	[productionYearTo] [int] NULL,
--	[createdAt] [nvarchar](50) NOT NULL,
--	[updatedAt] [nvarchar](50) NULL,
--	[deletedAt] [nvarchar](50) NULL,
--	[isDeleted] [bit] NOT NULL,
--	[isPublished] [bit] NOT NULL,
--	[isS4AB] [bit] NULL)

--insert into #splitted13
--SELECT DISTINCT
-- S.a.value('(/H/r)[1]', 'INT') AS id
--,S.a.value('(/H/r)[2]', 'VARCHAR(MAX)') AS name
--,S.a.value('(/H/r)[3]', 'VARCHAR(MAX)') AS code
--,S.a.value('(/H/r)[4]', 'INT') AS brandId
--,S.a.value('(/H/r)[5]', 'INT') AS productionYearFrom
--,S.a.value('(/H/r)[6]', 'INT') AS productionYearTo
--,try_convert(datetime, S.a.value('(/H/r)[7]', 'VARCHAR(MAX)'), 120) AS createdAt
--,try_convert(datetime, S.a.value('(/H/r)[8]', 'VARCHAR(MAX)'), 120) AS updatedAt
--,try_convert(datetime, S.a.value('(/H/r)[9]', 'VARCHAR(MAX)'), 120) AS deletedAt
--,S.a.value('(/H/r)[10]', 'BIT') AS isDeleted
--,S.a.value('(/H/r)[11]', 'BIT') AS isPublished
--,1 as isS4AB
--FROM
--(SELECT *,CAST (N'<H><r>' + REPLACE(string, ';', '</r><r>')  + '</r></H>' AS XML) AS [vals]
--FROM #longStrings13) d 
--CROSS APPLY d.[vals].nodes('/H/r') S(a)

--UPDATE [CatalogTO].[gc_maintenance_modifications]
--   SET [name] = fr.name
--      ,[code] = fr.code
--      ,[brandId] = fr.brandId
--      ,[productionYearFrom] = fr.productionYearFrom
--      ,[productionYearTo] = fr.productionYearTo
--      ,[createdAt] = fr.createdAt
--      ,[updatedAt] = fr.updatedAt
--      ,[deletedAt] = fr.deletedAt
--      ,[isDeleted] = fr.isDeleted
--      ,[isPublished] = fr.isPublished
--      ,[isS4AB] = fr.isS4AB
--from #splitted13 as fr
-- WHERE [CatalogTO].[gc_maintenance_modifications].id = fr.id

--  drop table #longStrings13
--drop table #temp13
--drop table #splitted13
