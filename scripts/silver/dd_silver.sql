/*
=============================================================
DDL Script: Create Silver Layer Tables
=============================================================
Script Purpose:
      Creates tables in 'silver' Schema.
=============================================================
*/

/* tables for silver layer*/

use DataWarehouse;

create TABLE silver_crm_cst_info(
	cst_id int,
    cst_key varchar(50),
    cst_firstname varchar(50),
    cst_lastname varchar(50),
    cst_marital_status varchar(50),
    cst_gndr varchar(50),
    cst_create_date date,
    dwh_create_date datetime default current_timestamp
);

create TABLE silver_crm_prd_info(
	prd_id int,
    prd_key varchar(50),
    prd_name varchar(50),
    prd_cost int,
    prd_line varchar(50),
    prd_start_dt date,
    prd_end_dt date,
    dwh_create_date datetime default current_timestamp
);

create TABLE silver_crm_sales_details(
	sls_ord_num varchar (50),
	sls_prd_key varchar (50),
	sls_cust_id int,
	sls_ord_dt date,
	sls_ship_dt date,
	sls_due_dt date,
	sls_sales int,
    sls_quantity int,
    sls_price int,
    dwh_create_date datetime default current_timestamp
);

create TABLE silver_erp_CUST_AZ12(
	CID varchar(50),
    BDATE DATE,
    GEN varchar(50),
    dwh_create_date datetime default current_timestamp
);

create TABLE silver_erp_LOC_A101(
	CID varchar(50),
    CNTRY varchar(50),
    dwh_create_date datetime default current_timestamp
    );
    
create TABLE silver_erp_PX_CAT_G1V2(
	ID varchar(50),
    CAT varchar(50),
    SUBCAT varchar(50),
    MAINTANANCE varchar(50),
    dwh_create_date datetime default current_timestamp
    ); 
        
/*
drop table if exists silver_erp_PX_CAT_G1V2;
drop table if exists silver_erp_LOC_A101;
drop table if exists silver_erp_CUST_AZ12;
drop table if exists silver_crm_sales_details;
drop table if exists silver_crm_prd_info;
drop table if exists silver_crm_cust_info;
*/
