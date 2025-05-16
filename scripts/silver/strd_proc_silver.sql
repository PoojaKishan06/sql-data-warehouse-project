/*
=================================================================================
Stored Procedure: Load silver Layer (Bronze -> Silver)
=================================================================================
Script Purpose:
  This Stored Procedure performs the ETL (Extract, Transform, Load) process to 
  add data to the Silver Schema tables from the Bronze Schema.
Actions Performed:
  - Truncate Silver Tables
  - Insert transformed and cleaned data from Bronze to Silver tables.

Parameters:
  None
  This stored procedure does not accept any paramenters or return any value.

Usage example:
  CALL silver_load_silver;
=================================================================================
*/

-- >> Truncating Table: silver_crm_cst_info
TRUNCATE TABLE silver_crm_cst_info;
SELECT '>> Table silver_crm_cst_info truncated' AS message;

INSERT INTO silver_crm_cst_info (
    cst_id, 
    cst_key, 
    cst_firstname, 
    cst_lastname, 
    cst_marital_status, 
    cst_gndr,
    cst_create_date
)
SELECT 
  cst_id,
  cst_key,
  TRIM(cst_firstname) as first_name,
  TRIM(cst_lastname) as last_name,
case when cst_marital_status = 'S' then 'Single'
	   when cst_marital_status = 'M' then 'Married'
       else 'n/a'
end cst_marital_status,
case when cst_gndr = 'F' then 'Female'
	   when cst_gndr = 'M' then 'Male'
       else 'n/a'
end cst_gndr,
cst_create_date
FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
  FROM bronze_crm_cst_info
  WHERE cst_id IS NOT NULL and cst_id != 0
) AS ranked
WHERE flag_last = 1;


-- >> Truncating Table: silver_crm_prd_info
TRUNCATE TABLE silver_crm_prd_info;
SELECT '>> Table silver_crm_prd_info truncated' AS message;

insert into silver_crm_prd_info(
prd_id,
prd_key,
cat_id,
prd_name,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
)

SELECT 
prd_id,
replace(substring(prd_key,1,5), '-','_') as cat_id, -- extract category id
substring(prd_key, 7, length(prd_key)) as prd_key,  -- extract product key
prd_name,
prd_cost, -- if value is null: use isnull(prd_cost,0)
case upper(trim(prd_line))
	when 'R' then 'Road'
    when 'M' then 'Mountain'
    when 'S' then 'Other Sales'
    when 'T' then 'Touring'
    else 'n/a'
end prd_line,
cast(prd_start_dt as date) as prd_start_dt,
cast(date_sub(lead(prd_start_dt) over 
(partition by prd_key order by prd_start_dt),interval 1 day) as date) 
as prd_end_dt -- calculate end date as one day before the next start date
FROM bronze_crm_prd_info;


-- >> Truncating Table: silver_crm_sales_details
TRUNCATE TABLE silver_crm_sales_details;
SELECT '>> Table silver_crm_sales_details truncated' AS message;

insert into silver_crm_sales_details(
	sls_ord_num ,
	sls_prd_key ,
	sls_cust_id ,
	sls_ord_dt ,
	sls_ship_dt ,
	sls_due_dt ,
	sls_sales ,
    sls_quantity ,
    sls_price
)
select 
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE 
  WHEN sls_ord_dt = 0 OR CHAR_LENGTH(sls_ord_dt) != 8 THEN NULL
  ELSE STR_TO_DATE(sls_ord_dt, '%Y%m%d')
END AS sls_ord_dt,
CASE 
  WHEN sls_ship_dt = 0 OR CHAR_LENGTH(sls_ship_dt) != 8 THEN NULL
  ELSE STR_TO_DATE(sls_ship_dt, '%Y%m%d')
END AS sls_ship_dt,
CASE
	when sls_due_dt = 0 or char_length(sls_due_dt) != 8 then null
    else str_to_date(sls_due_dt, '%Y%m%d')
    end as sls_due_dt,
 CASE 
    WHEN sls_sales IS NULL 
         OR sls_sales <= 0 
         OR sls_quantity IS NULL 
         OR sls_quantity <= 0 
         OR sls_price IS NULL 
         OR sls_price <= 0 
         OR sls_sales != sls_quantity * ABS(sls_price)
    THEN sls_quantity * ABS(
      CASE 
        WHEN sls_price IS NULL OR sls_price <= 0 
        THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE sls_price
      END
    )
    ELSE sls_sales
  END AS sls_sales,
sls_quantity,
 CASE 
    WHEN sls_price IS NULL OR sls_price <= 0 
    THEN sls_sales / NULLIF(sls_quantity, 0)
    ELSE sls_price
  END AS sls_price
from bronze_crm_sales_details;


-- >> Truncating Table: silver_erp_CUST_AZ12
TRUNCATE TABLE silver_erp_CUST_AZ12;
SELECT '>> Table silver_erp_CUST_AZ12 truncated' AS message;

insert into silver_erp_cust_az12(cid,bdate,gen)
SELECT 
substring(CID,4, length(CID)) as cid, -- extract customer id
case when bdate > current_date() then null
     else bdate
end as bdate, -- set future birthdate to NULL
CASE 
    WHEN UPPER(TRIM(REPLACE(REPLACE(gen, '\r', ''), '\n', ''))) = 'M' THEN 'Male'
    WHEN UPPER(TRIM(REPLACE(REPLACE(gen, '\r', ''), '\n', ''))) = 'F' THEN 'Female'
    WHEN UPPER(TRIM(REPLACE(REPLACE(gen, '\r', ''), '\n', ''))) = 'MALE' THEN 'Male'
    WHEN UPPER(TRIM(REPLACE(REPLACE(gen, '\r', ''), '\n', ''))) = 'FEMALE' THEN 'Female'
    ELSE 'n/a' -- normalize Gender values and handle unknowns
END AS gen
FROM bronze_erp_CUST_AZ12;


-- >> Truncating Table: silver_erp_LOC_A101
TRUNCATE TABLE silver_erp_LOC_A101;
SELECT '>> Table silver_erp_LOC_A101 truncated' AS message;

insert into silver_erp_LOC_A101 ( cid, cntry)
SELECT 
replace (cid,'-','') as cid,
case when upper(trim(REPLACE(cntry, '\r', ''))) = 'DE' then 'Germany'
	 when upper(trim(REPLACE(cntry, '\r', ''))) in ('US', 'USA') then 'United States'
     when cntry is null or trim(REPLACE(cntry, '\r', '')) = '' then 'n/a'
     else trim(REPLACE(cntry, '\r', ''))
end as cntry -- normalize and handle missing or null country values/codes
FROM bronze_erp_LOC_A101;


-- >> Truncating Table: silver_erp_PX_CAT_G1V2
TRUNCATE TABLE silver_erp_PX_CAT_G1V2;
SELECT '>> Table silver_erp_PX_CAT_G1V2 truncated' AS message;

insert into silver_erp_PX_CAT_G1V2
(id, cat, subcat, maintenance)

select 
id, 
cat, 
subcat, 
maintenance
from bronze_erp_PX_CAT_G1V2;
