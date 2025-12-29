/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

use DataWarehouse;
go
create or alter procedure silver.load_silver as
begin
	declare @start_time datetime, @end_time datetime;
	Begin try
		
		set @start_time = getdate();

		print '###############################################';
		print 'Loading Silver layer.....';
		print '###############################################';

		print '--------------------------------------';
		print 'Loading CRM silver tables....';
		print '--------------------------------------';

		print '>>> Truncating & Inserting into silver.crm_cust_info <<<<';

		truncate table silver.crm_cust_info;
		
		insert into silver.crm_cust_info(
			cst_id,
			cst_key ,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
		)
		select 
			cst_id,
			cst_key,
			trim(cst_firstname) as cst_firstname, 
			trim(cst_lastname) as cst_lastname,  -- trimming extra spaces
			case when upper(trim(cst_marital_status)) = 'M' then 'Married'
				 when upper(trim(cst_marital_status)) = 'S' then 'Single'
				 else 'N/A' 
				 end 
			as cst_marital_status, -- normalizing marital status to readable format
			case when upper(trim(cst_gndr)) = 'M' then 'Male'
				 when upper(trim(cst_gndr)) = 'F' then 'Female'
				 else 'N/A' 
				end 
			as cst_gndr ,  -- normalizing gender to readable format
			cst_create_date from 
		(select *, row_number() over(partition by cst_id order by cst_create_date desc) as cst_flag
		from bronze.crm_cust_info
		where cst_id is not null) as unique_cutomers
		where cst_flag=1; -- finding customer record with recent create date which contain most of the values
		


		print '>>> Truncating & Inserting into silver.crm_prd_info <<<<';

		Truncate table silver.crm_prd_info
		insert into silver.crm_prd_info(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		select prd_id,
			replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id, -- extracting category id 
			substring(prd_key,7,len(prd_key)) as prd_key, -- extracting product id
			trim(prd_nm) as prd_nm,                      -- handling trailing spaces 
			isnull(prd_cost,0) as prd_cost,              -- null price to 0
			case upper(trim(prd_line))
				when 'S' then 'Other Sales'
				when 'M' then 'Mountain'
				when 'R' then 'Road'
				when 'T' then 'Touring'
				else 'N/A'
			end as prd_line,            -- mapping product line codes todescriptive values
			cast(prd_start_dt as date) as prd_start_dt, -- extracting date 
			cast(lead(prd_start_dt) over(partition by prd_key order by prd_start_dt) -1 as date) as prd_end_dt -- handling end date by setting 1 day before the next start date
		from bronze.crm_prd_info;
		

		print '>>> Truncating & Inserting into silver.crm_sales_details <<<<';
		Truncate table silver.crm_sales_details
		Insert into silver.crm_sales_details(
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)
		select 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			case 
				when sls_order_dt <=0 or len(sls_order_dt) <> 8 then Null
				else cast(cast(sls_order_dt as varchar) as date) end 
			as sls_order_dt,
			case 
				when sls_ship_dt <=0 or len(sls_order_dt) <> 8 or sls_ship_dt < sls_order_dt then Null
				else cast(cast(sls_order_dt as varchar) as date) end 
			as sls_ship_dt,
			case 
				when sls_due_dt <=0 or len(sls_due_dt) <> 8 then Null 
				else cast(cast(sls_due_dt as varchar) as date) 
			end as sls_due_dt,
			case 
				when sls_sales is null or sls_sales <0 or sls_sales != (sls_quantity*abs(sls_price))
				then sls_quantity*abs(sls_price)
				else sls_sales
			end as sls_sales,
			case
				when sls_quantity is null or sls_quantity <=0 then abs(sls_sales)/abs(sls_price)
				else sls_quantity
			end as sls_quantity,
			case 
				when sls_price <= 0 or sls_price is null
				then sls_sales/nullif(sls_quantity,0)
				else sls_price
			end as sls_price
		from bronze.crm_sales_details;
		

		print '--------------------------------------';
		print 'Loading ERP silver tables....';
		print '--------------------------------------';

		print '>>> Truncating & Inserting into silver.erp_cust_az12 <<<<';

		truncate table silver.erp_cust_az12
		insert into silver.erp_cust_az12(
			cid,
			bdate,
			gen
		)
		select 
			case 
				when cid like 'NAS%' then substring(cid,4,len(cid)) 
				else cid
			end as cid,
			case 
				when bdate >getdate()  then Null
				else bdate
			end as bdate,
			case 
				when lower(trim(gen)) in ('m','male')then 'Male'
				when lower(trim(gen)) in ('f','female')then 'Female'
				else 'N/A'
			end as gen
		from bronze.erp_cust_az12;
		

		print '>>> Truncating & Inserting into silver.erp_loc_a101 <<<<';

		Truncate table silver.erp_loc_a101
		insert into silver.erp_loc_a101(
			cid,
			cntry
		)
		select 
			replace(cid,'-','') as cid,
			case
				when trim(cntry) in ('USA','US','United States') then 'United States of America'
				when trim(cntry) = 'DE' then 'Germany'
				when trim(cntry) is null or trim(cntry) = '' then 'N/A'
				else trim(cntry)
			end as cntry
		from bronze.erp_loc_a101;
		

		print '>>> Truncating & Inserting into silver.erp_loc_a101 <<<<';

		truncate table silver.erp_px_cat_g1v2;
		
		insert into silver.erp_px_cat_g1v2(
			ID,
			CAT,
			SUBCAT,
			MAINTENANCE
		)
		Select 
			ID,
			CAT,
			SUBCAT,
			MAINTENANCE
		from bronze.erp_px_cat_g1v2;
		
		set @end_time = getdate();

		print '###############################################';
		print 'Silver layer loaded';
		print 'Execution Time: '+ cast(datediff(second, @start_time, @end_time) as nvarchar)+' seconds';
		print '###############################################';

	end try

	begin catch
		print ' !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!'
		print ' ERROR OCCURED WHILE LOADING SILVER LAYER  ';
		print 'Error Message: ' + error_message();
		print 'Error Number: ' + cast(error_number() as nvarchar);
		print 'Error State: '  + cast(error_state() as nvarchar);
		print ' !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!';
	end catch
end
go

exec silver.load_silver
