/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
use DataWarehouse;
go

IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO
create view gold.dim_customers as 
select
	row_number() over(order by cci.cst_id) as customer_key,
	cci.cst_id as customer_id,
	cci.cst_key as customer_number,
	cci.cst_firstname as first_name,
	cci.cst_lastname as last_name,
	ela.cntry as country,
	cci.cst_marital_status as marital_status,
	case 
		when cci.cst_gndr <> 'N/A' then cci.cst_gndr
		else coalesce(eca.gen,'N/A') 
	end as gender,  -- 
	cci.cst_create_date as create_date,
	eca.bdate as dob
from silver.crm_cust_info as cci
left join silver.erp_cust_az12 as eca
	on cci.cst_key = eca.cid
left join silver.erp_loc_a101 as ela
	on cci.cst_key=ela.cid;
go

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
if object_id('gold.dim_products','V') is not null
	Drop view gold.dim_products;
go
create or alter view gold.dim_products as
select 
	row_number() over(order by cpi.prd_start_dt,cpi.prd_key) as product_key,
	cpi.prd_id as product_id,
	cpi.prd_key as product_number,
	cpi.prd_nm as product_name,
	cpi.cat_id as category_id,
	pcg.cat as categoty,
	pcg.subcat as sub_category,
	pcg.maintenance as maintenance,
	cpi.prd_cost as cost,
	cpi.prd_line as product_line,
	cpi.prd_start_dt as start_date
from silver.crm_prd_info as cpi
	left join silver.erp_px_cat_g1v2 as pcg
		on cpi.cat_id= pcg.id
	WHERE cpi.prd_end_dt IS NULL; -- Filter out all historical data
go

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
if object_id('gold.fact_sales','V')is not null
	drop view gold.fact_sales;
go

create view gold.fact_sales as
select 
	csd.sls_ord_num as order_number,
	gp.product_key,
	gc.customer_key,
	csd.sls_order_dt as order_date,
	csd.sls_ship_dt as shipping_date,
	csd.sls_due_dt as due_date,
	csd.sls_sales as sales_amount,
	csd.sls_quantity as quantity,
	csd.sls_price as price
from silver.crm_sales_details as csd
left join gold.dim_customers as gc 
	on gc.customer_id = csd.sls_cust_id
left join gold.dim_products as gp
	on gp.product_number = csd.sls_prd_key;

go