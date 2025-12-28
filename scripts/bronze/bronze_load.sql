/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================

Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the 'BULK INSERT' command to load data from csv Files to bronze tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

/*
============
FULL LOAD
============
*/

use DataWarehouse;
go

create or alter procedure bronze.load_bronze as
begin
	declare @start_time datetime, @end_time datetime
	BEGIN TRY
		set @start_time = getdate();
		PRINT '~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~';
		PRINT 'Loading Bronze layer......';
		PRINT '~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~';

		PRINT '--------------------------------------';
		PRINT 'Loading CRM tables....';
		PRINT '--------------------------------------';

		print '>>Truncating and inserting data into bronze.crm_cut_info...'
		truncate table bronze.crm_cust_info;
		bulk insert bronze.crm_cust_info
		from 'C:\Users\BharadwajVenkataSriR\Desktop\Problems\CT-DBX-PRJ-100\source_data\datasets\datasets\source_crm\cust_info.csv'
		with (
			firstrow = 2,
			fieldterminator= ',',
			tablock
		);

		print '>>Truncating and inserting data into bronze.crm_prd_info...'
		truncate table bronze.crm_prd_info;
		bulk insert bronze.crm_prd_info
		from 'C:\Users\BharadwajVenkataSriR\Desktop\Problems\CT-DBX-PRJ-100\source_data\datasets\datasets\source_crm\prd_info.csv'
		with(
			firstrow=2,
			fieldterminator = ',',
			tablock
		);

		print '>>Truncating and inserting data into bronze.crm_sales_details...'
		truncate table bronze.crm_sales_details
		bulk insert bronze.crm_sales_details
		from 'C:\Users\BharadwajVenkataSriR\Desktop\Problems\CT-DBX-PRJ-100\source_data\datasets\datasets\source_crm\sales_details.csv'
		with(
			firstrow=2,
			fieldterminator=',',
			tablock
		);
		PRINT '--------------------------------------';
		PRINT 'Loading ERP tables......';
		PRINT '--------------------------------------';

		print '>>Truncating and inserting data into bronze.erp_cust_az12...'
		truncate table [bronze].[erp_cust_az12];
		bulk insert [bronze].[erp_cust_az12]
		from 'C:\Users\BharadwajVenkataSriR\Desktop\Problems\CT-DBX-PRJ-100\source_data\datasets\datasets\source_erp\CUST_AZ12.csv'
		with(
			firstrow=2,
			fieldterminator=',',
			tablock
		);

		print '>>Truncating and inserting data into bronze.erp_loc_a101...'
		truncate table [bronze].[erp_loc_a101];
		bulk insert [bronze].[erp_loc_a101]
		from 'C:\Users\BharadwajVenkataSriR\Desktop\Problems\CT-DBX-PRJ-100\source_data\datasets\datasets\source_erp\LOC_A101.csv'
		with(
			firstrow=2,
			fieldterminator=',',
			tablock
		);


		print '>>Truncating and inserting data into bronze.px_cat_g1v2...'
		truncate table [bronze].[px_cat_g1v2];
		bulk insert [bronze].[px_cat_g1v2]
		from 'C:\Users\BharadwajVenkataSriR\Desktop\Problems\CT-DBX-PRJ-100\source_data\datasets\datasets\source_erp\PX_CAT_G1V2.csv'
		with(
			firstrow=2,
			fieldterminator=',',
			tablock
		);

		set @end_time = getdate()
		print '==========================================================';
		print 'Total time taken ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';
		print '==========================================================';

	END TRY
	BEGIN CATCH
	print '========================================================';
	print '!!! ERROR LOADING DATA INTO BRONZE LAYER !!!';
	print 'Error Message' + Error_message();
	print 'Error Number' + cast(error_number() as nvarchar);
	print 'Error Status' + cast(error_state() as nvarchar);
	print '========================================================';
	END CATCH

END
go


exec bronze.load_bronze