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

drop view if exists gold_dim_customers;

create view gold_dim_customers as
select 
	row_number() over(order by cst_id) as customer_key,
	ci.cst_id as customer_id,
	ci.cst_key as customer_number,
	ci.cst_firstname as first_name,
    ci.cst_lastname as last_name,
    la.cntry as country,
	ci.cst_marital_status as marital_status,
	case when ci.cst_gndr != 'n/a' then ci.cst_gndr -- CRM is the master for gender integration 
		 else coalesce(ca.gen, 'n/a')
    end as gender,
    ca.bdate as birth_date,
	ci.cst_create_date as create_date 
from silver_crm_cst_info ci
left join silver_erp_cust_az12 ca
on ci.cst_key = ca.cid
left join silver_erp_loc_a101 la
on ci.cst_key = la.cid;

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
drop view if exists gold_dim_products;

create view gold_dim_products as
select
	row_number() over (order by prd_start_dt,pn.cat_id) as product_key,
	pn.prd_id as product_id,
    pn.cat_id as product_number,
	pn.prd_name as product_name,
    pn.prd_key as category_id,
	pc.cat as category,
    pc.subcat as subcategory,
    pc.maintenance as maintenance,
    pn.prd_cost as cost,
    pn.prd_line as product_line,
    pn.prd_start_dt as start_date
from silver_crm_prd_info pn
left join silver_erp_px_cat_g1v2 pc
on pn.prd_key = pc.id
where prd_end_dt is null;

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================

drop view if exists gold_fact_sales;

create view gold_fact_sales as
select 
	sd.sls_ord_num as order_number,
	pr.product_key,
	cu.customer_key,
	sd.sls_ord_dt as order_date,
	sd.sls_ship_dt as shipping_date,
	sd.sls_due_dt as due_date,
	sd.sls_sales as sales_amount,
	sd.sls_quantity as quantity,
	sd.sls_price as price
from silver_crm_sales_details sd
left join gold_dim_products pr
on sd.sls_prd_key = pr.product_number
left join gold_dim_customers cu
on sd.sls_cust_id = cu.customer_id;
