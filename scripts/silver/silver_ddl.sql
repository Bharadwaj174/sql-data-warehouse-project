/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

use DataWarehouse;

if object_id('silver.crm_cust_info','U') is not null
	DROP table silver.crm_cust_info;

create table silver.crm_cust_info (
	cst_id int,
	cst_key nvarchar(55),
	cst_firstname nvarchar(255),
	cst_lastname nvarchar(255),
	cst_marital_status nvarchar(55),
	cst_gndr nvarchar(55),
	cst_create_date date,
	dwh_create_date datetime2 default getdate()
);

if object_id('silver.crm_prd_info','U') is not null
	DROP table silver.crm_prd_info;
create table silver.crm_prd_info(
	prd_id int,
	cat_id nvarchar(55),
	prd_key NVARCHAR(55),
	prd_nm NVARCHAR(255),
	prd_cost int,
	prd_line NVARCHAR(55),
	prd_start_dt DATETIME,
	prd_end_dt DATETIME,
	dwh_create_date datetime2 default getdate()
);

if object_id('silver.crm_sales_details','U') is not null
	DROP table silver.crm_sales_details;
create table silver.crm_sales_details(
	sls_ord_num nvarchar(55),
	sls_prd_key	nvarchar(55),
	sls_cust_id	int,
	sls_order_dt date,
	sls_ship_dt	date,
	sls_due_dt	date,
	sls_sales	int,
	sls_quantity int,
	sls_price int,
	dwh_create_date datetime2 default getdate()
);

if object_id('silver.erp_cust_az12','U') is not null
	DROP table silver.erp_cust_az12;
create table silver.erp_cust_az12(
	cid nvarchar(55),
	bdate date,
	gen nvarchar(55),
	dwh_create_date datetime2 default getdate()
);


if object_id('silver.erp_loc_a101','U') is not null
	DROP table silver.erp_loc_a101;
create table silver.erp_loc_a101(
	cid nvarchar(55),
	cntry nvarchar(55),
	dwh_create_date datetime2 default getdate()
);

if object_id('silver.erp_px_cat_g1v2','U') is not null
	DROP table silver.erp_px_cat_g1v2;
create table silver.erp_px_cat_g1v2(
	 ID nvarchar(55),
	 CAT nvarchar(55),
	 SUBCAT nvarchar(55),
	 MAINTENANCE NVARCHAR(55),
	dwh_create_date datetime2 default getdate()
);
