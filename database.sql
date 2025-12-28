/*

==========================
CREATE DATABASE AND SCHEMA
==========================
SCRIPT:
	This script is used  to create an entire new database DataWarehouse and
	the script creates 3 schemas of bronze, silver, gold layers.

Note:
	The database wilbe deletd and recreated is exists make sure data will be 
	permanently deleted. Make sure you have backup.

*/
use master;
go
-- drop and recreate new database DataWarehouse

if exists (select 1 from sys.databases where name = 'DataWarehouse')

Begin
	alter database DataWarehouse set single_user with ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
End;
go

--create database
create database DataWarehouse;
go

use DataWarehouse;
go

--create 3 schemas
create schema bronze;
go

create schema silver;
go

create schema gold;
go