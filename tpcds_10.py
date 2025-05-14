import requests
import os

# https://sparkdltrigger.web.cern.ch/sparkdltrigger/TPCDS/

# Create directory if it doesn't exist
os.makedirs('/lakehouse/default/Files/tpcds10', exist_ok=True)
try:
    url = 'https://sparkdltrigger.web.cern.ch/sparkdltrigger/TPCDS/tpcds_10.zip'
    response = requests.get(url)
    with open('/lakehouse/default/Files/tpcds10/tpcds_10.zip', 'wb') as f:
        f.write(response.content)
    print("File downloaded successfully")    
except:
    print("check URL")



##unzip
!unzip /lakehouse/default/Files/tpcds10/tpcds_10.zip -d /lakehouse/default/Files/tpcds10/

# Path to your TPCDS dataset
data_path = "Files/tpcds10/tpcds_10"

# List of TPCDS tables from the repository
tpcds_tables = [
    "catalog_returns", "catalog_sales", "inventory", "store_returns",
    "store_sales", "web_returns", "web_sales", "call_center",
    "catalog_page", "customer", "customer_address",
    "customer_demographics", "date_dim", "household_demographics",
    "income_band", "item", "promotion", "reason", "ship_mode",
    "store", "time_dim", "warehouse", "web_page", "web_site"
]

# Create temporary views for each table
for table in tpcds_tables:
    table_path = f"{data_path}/{table}"  # Path to each table's Parquet file
    df = spark.read.parquet(table_path)  # Load Parquet file into DataFrame
    df.createOrReplaceTempView(table)   # Create a temporary view
    print(f"Created temp view for table: {table}")

## run queries
import time
import pandas as pd
import os

def execute_tpcds_queries(configuration_name="default_spark"):
    """
    Execute TPC-DS queries and save the results as Delta tables.
    Fixes column naming issues, includes resource-intensive queries,
    and captures execution time for benchmarking.
    
    Args:
        configuration_name (str): Name of the configuration being tested
    
    Returns:
        Path to the CSV file with timing results
    """
    # Dictionary to store execution times
    execution_times = []
    
    # Dictionary of TPC-DS queries with fixed column names
    tpcds_queries = {
        # Query 1
        'q1': """
        WITH customer_total_return AS(
            SELECT
                sr_customer_sk AS ctr_customer_sk,
                sr_store_sk AS ctr_store_sk,
                sum(sr_return_amt) AS ctr_total_return
            FROM 
                store_returns, date_dim
            WHERE 
                sr_returned_date_sk = d_date_sk AND d_year = 2000
            GROUP BY 
                sr_customer_sk, sr_store_sk
        )
        SELECT c_customer_id
        FROM 
            customer_total_return ctr1, store, customer
        WHERE 
            ctr1.ctr_total_return > 
            (SELECT avg(ctr_total_return) * 1.2
             FROM customer_total_return ctr2
             WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk)
            AND s_store_sk = ctr1.ctr_store_sk
            AND s_state = 'TN'
            AND ctr1.ctr_customer_sk = c_customer_sk
        ORDER BY c_customer_id
        LIMIT 100
        """,
        
        # Query 2 - Fixed column names
        'q2': """
        WITH wscs AS (
            SELECT 
                sold_date_sk, sales_price
            FROM 
                (SELECT 
                    ws_sold_date_sk AS sold_date_sk, 
                    ws_ext_sales_price AS sales_price
                 FROM web_sales) x
            UNION ALL
            (SELECT 
                cs_sold_date_sk AS sold_date_sk, 
                cs_ext_sales_price AS sales_price
             FROM catalog_sales)
        ),
        wswscs AS (
            SELECT 
                d_week_seq,
                sum(CASE WHEN (d_day_name = 'Sunday') THEN sales_price ELSE 0 END) sun_sales,
                sum(CASE WHEN (d_day_name = 'Monday') THEN sales_price ELSE 0 END) mon_sales,
                sum(CASE WHEN (d_day_name = 'Tuesday') THEN sales_price ELSE 0 END) tue_sales,
                sum(CASE WHEN (d_day_name = 'Wednesday') THEN sales_price ELSE 0 END) wed_sales,
                sum(CASE WHEN (d_day_name = 'Thursday') THEN sales_price ELSE 0 END) thu_sales,
                sum(CASE WHEN (d_day_name = 'Friday') THEN sales_price ELSE 0 END) fri_sales,
                sum(CASE WHEN (d_day_name = 'Saturday') THEN sales_price ELSE 0 END) sat_sales
            FROM wscs, date_dim
            WHERE d_date_sk = sold_date_sk
            GROUP BY d_week_seq
        )
        SELECT 
            d_week_seq1, 
            (sun_sales1/sun_sales2) AS sun_ratio,
            (mon_sales1/mon_sales2) AS mon_ratio,
            (tue_sales1/tue_sales2) AS tue_ratio,
            (wed_sales1/wed_sales2) AS wed_ratio,
            (thu_sales1/thu_sales2) AS thu_ratio,
            (fri_sales1/fri_sales2) AS fri_ratio,
            (sat_sales1/sat_sales2) AS sat_ratio
        FROM
            (SELECT 
                wswscs.d_week_seq AS d_week_seq1,
                sun_sales AS sun_sales1,
                mon_sales AS mon_sales1,
                tue_sales AS tue_sales1,
                wed_sales AS wed_sales1,
                thu_sales AS thu_sales1,
                fri_sales AS fri_sales1,
                sat_sales AS sat_sales1
             FROM wswscs, date_dim
             WHERE date_dim.d_week_seq = wswscs.d_week_seq AND d_year = 2001) y,
            (SELECT 
                wswscs.d_week_seq AS d_week_seq2,
                sun_sales AS sun_sales2,
                mon_sales AS mon_sales2,
                tue_sales AS tue_sales2,
                wed_sales AS wed_sales2,
                thu_sales AS thu_sales2,
                fri_sales AS fri_sales2,
                sat_sales AS sat_sales2
             FROM wswscs, date_dim
             WHERE date_dim.d_week_seq = wswscs.d_week_seq AND d_year = 2001+1) z
        WHERE d_week_seq1 = d_week_seq2-53
        ORDER BY d_week_seq1
        """,
        
        # Query 3
        'q3': """
        SELECT 
            dt.d_year, 
            item.i_brand_id brand_id, 
            item.i_brand brand,
            SUM(ss_ext_sales_price) sum_agg
        FROM 
            date_dim dt, 
            store_sales, 
            item
        WHERE 
            dt.d_date_sk = store_sales.ss_sold_date_sk
            AND store_sales.ss_item_sk = item.i_item_sk
            AND item.i_manufact_id = 128
            AND dt.d_moy = 11
        GROUP BY 
            dt.d_year, 
            item.i_brand, 
            item.i_brand_id
        ORDER BY 
            dt.d_year, 
            sum_agg DESC, 
            brand_id
        LIMIT 100
        """,
        
        # Query 4
        'q4': """
        WITH year_total AS (
            SELECT 
                c_customer_id customer_id,
                c_first_name customer_first_name,
                c_last_name customer_last_name,
                c_preferred_cust_flag customer_preferred_cust_flag,
                c_birth_country customer_birth_country,
                c_login customer_login,
                c_email_address customer_email_address,
                d_year dyear,
                sum(((ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2) year_total,
                s_state
            FROM 
                customer, 
                store_sales, 
                date_dim, 
                store
            WHERE 
                c_customer_sk = ss_customer_sk
                AND ss_sold_date_sk = d_date_sk
                AND ss_store_sk = s_store_sk
            GROUP BY 
                c_customer_id,
                c_first_name,
                c_last_name,
                c_preferred_cust_flag,
                c_birth_country,
                c_login,
                c_email_address,
                d_year,
                s_state
        )
        SELECT 
            t_s_secyear.customer_id,
            t_s_secyear.customer_first_name,
            t_s_secyear.customer_last_name
        FROM 
            year_total t_s_firstyear,
            year_total t_s_secyear,
            year_total t_c_firstyear,
            year_total t_c_secyear,
            year_total t_w_firstyear,
            year_total t_w_secyear
        WHERE 
            t_s_secyear.customer_id = t_s_firstyear.customer_id
            AND t_s_firstyear.customer_id = t_c_secyear.customer_id
            AND t_s_firstyear.customer_id = t_c_firstyear.customer_id
            AND t_s_firstyear.customer_id = t_w_firstyear.customer_id
            AND t_s_firstyear.customer_id = t_w_secyear.customer_id
            AND t_s_firstyear.dyear = 2001
            AND t_s_secyear.dyear = 2001+1
            AND t_c_firstyear.dyear = 2001
            AND t_c_secyear.dyear = 2001+1
            AND t_w_firstyear.dyear = 2001
            AND t_w_secyear.dyear = 2001+1
            AND t_s_firstyear.year_total > 0
            AND t_c_firstyear.year_total > 0
            AND t_w_firstyear.year_total > 0
            AND CASE WHEN t_c_firstyear.year_total > 0 THEN t_c_secyear.year_total / t_c_firstyear.year_total ELSE 0.0 END
                > CASE WHEN t_s_firstyear.year_total > 0 THEN t_s_secyear.year_total / t_s_firstyear.year_total ELSE 0.0 END
            AND CASE WHEN t_c_firstyear.year_total > 0 THEN t_c_secyear.year_total / t_c_firstyear.year_total ELSE 0.0 END
                > CASE WHEN t_w_firstyear.year_total > 0 THEN t_w_secyear.year_total / t_w_firstyear.year_total ELSE 0.0 END
        ORDER BY 
            t_s_secyear.customer_id,
            t_s_secyear.customer_first_name,
            t_s_secyear.customer_last_name
        LIMIT 100
        """,
        
        # Query 5 - Fixed column names
        'q5': """
        WITH ssr AS (
            SELECT 
                s_store_id,
                sum(sales_price) AS sales,
                sum(profit) AS profit,
                sum(return_amt) AS returns,
                sum(net_loss) AS profit_loss
            FROM (
                SELECT 
                    ss_store_sk AS store_sk,
                    ss_sold_date_sk AS date_sk,
                    ss_ext_sales_price AS sales_price,
                    ss_net_profit AS profit,
                    cast(0 AS decimal(7,2)) AS return_amt,
                    cast(0 AS decimal(7,2)) AS net_loss
                FROM store_sales
                UNION ALL
                SELECT 
                    sr_store_sk AS store_sk,
                    sr_returned_date_sk AS date_sk,
                    cast(0 AS decimal(7,2)) AS sales_price,
                    cast(0 AS decimal(7,2)) AS profit,
                    sr_return_amt AS return_amt,
                    sr_net_loss AS net_loss
                FROM store_returns
            ) salesreturns, date_dim, store
            WHERE 
                date_sk = d_date_sk
                AND d_date BETWEEN cast('2000-08-23' AS date) AND (cast('2000-08-23' AS date) + INTERVAL 14 days)
                AND store_sk = s_store_sk
            GROUP BY s_store_id
        ),
        csr AS (
            SELECT 
                cp_catalog_page_id,
                sum(sales_price) AS sales,
                sum(profit) AS profit,
                sum(return_amt) AS returns,
                sum(net_loss) AS profit_loss
            FROM (
                SELECT 
                    cs_catalog_page_sk AS page_sk,
                    cs_sold_date_sk AS date_sk,
                    cs_ext_sales_price AS sales_price,
                    cs_net_profit AS profit,
                    cast(0 AS decimal(7,2)) AS return_amt,
                    cast(0 AS decimal(7,2)) AS net_loss
                FROM catalog_sales
                UNION ALL
                SELECT 
                    cr_catalog_page_sk AS page_sk,
                    cr_returned_date_sk AS date_sk,
                    cast(0 AS decimal(7,2)) AS sales_price,
                    cast(0 AS decimal(7,2)) AS profit,
                    cr_return_amount AS return_amt,
                    cr_net_loss AS net_loss
                FROM catalog_returns
            ) salesreturns, date_dim, catalog_page
            WHERE 
                date_sk = d_date_sk
                AND d_date BETWEEN cast('2000-08-23' AS date) AND (cast('2000-08-23' AS date) + INTERVAL 14 days)
                AND page_sk = cp_catalog_page_sk
            GROUP BY cp_catalog_page_id
        ),
        wsr AS (
            SELECT 
                web_site_id,
                sum(sales_price) AS sales,
                sum(profit) AS profit,
                sum(return_amt) AS returns,
                sum(net_loss) AS profit_loss
            FROM (
                SELECT 
                    ws_web_site_sk AS wsr_web_site_sk,
                    ws_sold_date_sk AS date_sk,
                    ws_ext_sales_price AS sales_price,
                    ws_net_profit AS profit,
                    cast(0 AS decimal(7,2)) AS return_amt,
                    cast(0 AS decimal(7,2)) AS net_loss
                FROM web_sales
                UNION ALL
                SELECT 
                    ws_web_site_sk AS wsr_web_site_sk,
                    wr_returned_date_sk AS date_sk,
                    cast(0 AS decimal(7,2)) AS sales_price,
                    cast(0 AS decimal(7,2)) AS profit,
                    wr_return_amt AS return_amt,
                    wr_net_loss AS net_loss
                FROM web_returns LEFT OUTER JOIN web_sales ON
                    (wr_item_sk = ws_item_sk AND wr_order_number = ws_order_number)
            ) salesreturns, date_dim, web_site
            WHERE 
                date_sk = d_date_sk
                AND d_date BETWEEN cast('2000-08-23' AS date) AND (cast('2000-08-23' AS date) + INTERVAL 14 days)
                AND wsr_web_site_sk = web_site_sk
            GROUP BY web_site_id
        )
        SELECT 
            channel,
            id,
            sum(sales) AS total_sales,
            sum(returns) AS total_returns,
            sum(profit) AS total_profit
        FROM (
            SELECT 
                'store channel' AS channel,
                concat('store', s_store_id) AS id,
                sales,
                returns,
                (profit - profit_loss) AS profit
            FROM ssr
            UNION ALL
            SELECT 
                'catalog channel' AS channel,
                concat('catalog_page', cp_catalog_page_id) AS id,
                sales,
                returns,
                (profit - profit_loss) AS profit
            FROM csr
            UNION ALL
            SELECT 
                'web channel' AS channel,
                concat('web_site', web_site_id) AS id,
                sales,
                returns,
                (profit - profit_loss) AS profit
            FROM wsr
        ) x
        GROUP BY ROLLUP (channel, id)
        ORDER BY channel, id
        LIMIT 100
        """,
        
        # Query 19 - Resource intensive join query
        'q19': """
        SELECT 
            i_brand_id brand_id, 
            i_brand brand, 
            i_manufact_id, 
            i_manufact,
            sum(ss_ext_sales_price) ext_price
        FROM 
            date_dim, 
            store_sales, 
            item,
            customer,
            customer_address,
            store
        WHERE 
            d_date_sk = ss_sold_date_sk
            AND ss_item_sk = i_item_sk
            AND i_manager_id = 8
            AND d_moy = 11
            AND d_year = 1998
            AND ss_customer_sk = c_customer_sk 
            AND c_current_addr_sk = ca_address_sk
            AND SUBSTRING(ca_zip, 1, 5) <> SUBSTRING(s_zip, 1, 5)
            AND ss_store_sk = s_store_sk
        GROUP BY 
            i_brand,
            i_brand_id,
            i_manufact_id,
            i_manufact
        ORDER BY 
            ext_price DESC,
            i_brand,
            i_brand_id,
            i_manufact_id,
            i_manufact
        LIMIT 100
        """,
        
        # Query 27 - Resource intensive with multiple joins and aggregations
        'q27': """
        SELECT 
            i_item_id,
            s_state,
            grouping(s_state) g_state,
            avg(ss_quantity) agg1,
            avg(ss_list_price) agg2,
            avg(ss_coupon_amt) agg3,
            avg(ss_sales_price) agg4
        FROM 
            store_sales, 
            customer_demographics, 
            date_dim, 
            store, 
            item
        WHERE 
            ss_sold_date_sk = d_date_sk 
            AND ss_item_sk = i_item_sk 
            AND ss_store_sk = s_store_sk 
            AND ss_cdemo_sk = cd_demo_sk 
            AND cd_gender = 'M' 
            AND cd_marital_status = 'S' 
            AND cd_education_status = 'College' 
            AND d_year = 2002 
            AND s_state IN ('TN', 'TN', 'TN', 'TN', 'TN', 'TN')
        GROUP BY 
            ROLLUP (i_item_id, s_state)
        ORDER BY 
            i_item_id, s_state
        LIMIT 100
        """,
        
        # Query 34 - Complex with window functions
        'q34': """
        SELECT 
            c_last_name,
            c_first_name,
            c_salutation,
            c_preferred_cust_flag,
            ss_ticket_number,
            cnt
        FROM 
            (SELECT 
                ss_ticket_number,
                ss_customer_sk,
                count(*) cnt
             FROM 
                store_sales, date_dim, store, household_demographics
             WHERE 
                store_sales.ss_sold_date_sk = date_dim.d_date_sk
                AND store_sales.ss_store_sk = store.s_store_sk  
                AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
                AND (date_dim.d_dom BETWEEN 1 AND 3 OR date_dim.d_dom BETWEEN 25 AND 28)
                AND (household_demographics.hd_buy_potential = '>10000' OR
                    household_demographics.hd_buy_potential = 'Unknown')
                AND household_demographics.hd_vehicle_count > 0
                AND (CASE WHEN household_demographics.hd_vehicle_count > 0 
                      THEN household_demographics.hd_dep_count/ household_demographics.hd_vehicle_count 
                      ELSE NULL 
                      END) > 1.2
                AND date_dim.d_year IN (1999, 1999+1, 1999+2)
                AND store.s_county IN ('Williamson County', 'Williamson County', 'Williamson County', 'Williamson County',
                                     'Williamson County', 'Williamson County', 'Williamson County', 'Williamson County')
             GROUP BY ss_ticket_number, ss_customer_sk) dn,
             customer
        WHERE 
            ss_customer_sk = c_customer_sk
            AND cnt BETWEEN 15 AND 20
        ORDER BY 
            c_last_name, c_first_name, c_salutation, c_preferred_cust_flag DESC, ss_ticket_number
        """,
        
        # Query 46 - Complex correlated subquery
        'q46': """
        SELECT 
            c_last_name, 
            c_first_name, 
            ca_city, 
            bought_city, 
            ss_ticket_number, 
            amt, 
            profit
        FROM 
            (SELECT 
                ss_ticket_number, 
                ss_customer_sk, 
                ca_city bought_city, 
                sum(ss_coupon_amt) amt, 
                sum(ss_net_profit) profit
             FROM 
                store_sales, 
                date_dim, 
                store, 
                household_demographics, 
                customer_address
             WHERE 
                store_sales.ss_sold_date_sk = date_dim.d_date_sk
                AND store_sales.ss_store_sk = store.s_store_sk  
                AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
                AND store_sales.ss_addr_sk = customer_address.ca_address_sk
                AND (household_demographics.hd_dep_count = 4 OR
                     household_demographics.hd_vehicle_count = 3)
                AND date_dim.d_dow IN (6, 0)
                AND date_dim.d_year IN (1999, 1999+1, 1999+2) 
                AND store.s_city IN ('Fairview', 'Midway')
             GROUP BY 
                ss_ticket_number, 
                ss_customer_sk, 
                ss_addr_sk, 
                ca_city) dn,
             customer,
             customer_address current_addr
        WHERE 
            ss_customer_sk = c_customer_sk
            AND customer.c_current_addr_sk = current_addr.ca_address_sk
            AND current_addr.ca_city <> bought_city
        ORDER BY 
            c_last_name, 
            c_first_name, 
            ca_city, 
            bought_city, 
            ss_ticket_number
        LIMIT 100
        """,
        
        # Query 67 - With multiple large aggregations
        'q67': """
        SELECT *
        FROM 
            (SELECT 
                i_category, 
                i_class, 
                i_brand, 
                i_product_name, 
                d_year, 
                d_qoy, 
                d_moy, 
                s_store_id,
                sumsales, 
                rank() OVER (PARTITION BY i_category ORDER BY sumsales DESC) rk
             FROM 
                (SELECT 
                    i_category, 
                    i_class, 
                    i_brand, 
                    i_product_name, 
                    d_year, 
                    d_qoy, 
                    d_moy, 
                    s_store_id, 
                    sum(coalesce(ss_sales_price*ss_quantity, 0)) sumsales
                 FROM 
                    store_sales, 
                    date_dim, 
                    store, 
                    item
                 WHERE 
                    ss_sold_date_sk = d_date_sk
                    AND ss_item_sk = i_item_sk
                    AND ss_store_sk = s_store_sk
                    AND d_month_seq BETWEEN 1200 AND 1200+11
                 GROUP BY 
                    ROLLUP(i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy, s_store_id)
                ) dw1
            ) dw2
        WHERE 
            rk <= 100
        ORDER BY 
            i_category, 
            i_class, 
            i_brand, 
            i_product_name, 
            d_year, 
            d_qoy, 
            d_moy, 
            s_store_id, 
            sumsales, 
            rk
        LIMIT 100
        """,
        
        # Query 72 - Fixed to use d_dom instead of d_day
        'q72': """
        SELECT 
            i_item_desc,
            w_warehouse_name,
            d1.d_year,
            d1.d_moy,
            d1.d_dom as d1_dom,
            inv1.inv_quantity_on_hand inv_quantity_on_hand_1,
            inv2.inv_quantity_on_hand inv_quantity_on_hand_2,
            d2.d_year,
            d2.d_moy,
            d2.d_dom as d2_dom,
            d3.d_year,
            d3.d_moy,
            d3.d_dom as d3_dom
        FROM 
            inventory inv1,
            inventory inv2,
            inventory inv3,
            warehouse,
            item,
            date_dim d1,
            date_dim d2,
            date_dim d3
        WHERE 
            inv1.inv_item_sk = inv2.inv_item_sk
            AND inv2.inv_item_sk = inv3.inv_item_sk
            AND inv1.inv_warehouse_sk = inv2.inv_warehouse_sk
            AND inv2.inv_warehouse_sk = inv3.inv_warehouse_sk
            AND inv1.inv_date_sk = d1.d_date_sk
            AND inv2.inv_date_sk = d2.d_date_sk
            AND inv3.inv_date_sk = d3.d_date_sk
            AND inv1.inv_quantity_on_hand < inv2.inv_quantity_on_hand
            AND inv2.inv_quantity_on_hand < inv3.inv_quantity_on_hand
            AND d1.d_year = 2001
            AND d2.d_year = 2001 + 1
            AND d3.d_year = 2001 + 2
            AND d1.d_moy = 1
            AND d2.d_moy = 1
            AND d3.d_moy = 1
            AND inv1.inv_item_sk = item.i_item_sk
            AND inv1.inv_warehouse_sk = warehouse.w_warehouse_sk
        ORDER BY 
            i_item_desc,
            w_warehouse_name,
            d1.d_year,
            d1.d_moy,
            d1.d_dom
        LIMIT 100
        """,
        
        # Query 95 - With COUNT DISTINCT aggregation (resource intensive)
        'q95': """
        WITH ws_wh AS (
            SELECT 
                ws1.ws_order_number,
                ws1.ws_warehouse_sk wh1,
                ws2.ws_warehouse_sk wh2
            FROM 
                web_sales ws1,
                web_sales ws2
            WHERE 
                ws1.ws_order_number = ws2.ws_order_number
                AND ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk
        )
        SELECT  
            count(DISTINCT ws_order_number) AS order_count,
            sum(ws_ext_ship_cost) AS total_shipping_cost,
            sum(ws_net_profit) AS total_net_profit
        FROM 
            web_sales ws1,
            date_dim,
            customer_address,
            web_site
        WHERE 
            CAST(d_date AS DATE) BETWEEN CAST('1999-2-01' AS DATE) AND
            (CAST('1999-2-01' AS DATE) + INTERVAL 60 days)
            AND ws1.ws_ship_date_sk = d_date_sk
            AND ws1.ws_ship_addr_sk = ca_address_sk
            AND ca_state = 'IL'
            AND ws1.ws_web_site_sk = web_site_sk
            AND web_company_name = 'pri'
            AND ws1.ws_order_number IN (SELECT ws_order_number FROM ws_wh)
            AND ws1.ws_order_number IN (
                SELECT wr_order_number
                FROM web_returns,ws_wh
                WHERE wr_order_number = ws_wh.ws_order_number
            )
        ORDER BY 
            count(DISTINCT ws_order_number)
        LIMIT 100
        """
    }
    
    # Execute each query and save as Delta table
    for query_id, query_sql in tpcds_queries.items():
        print(f"Executing {query_id}...")
        try:
            # Measure execution time
            start_time = time.time()
            
            # Execute the query
            result = spark.sql(query_sql)
            
            # Save as Delta table
            result.write.format("delta").mode("overwrite").saveAsTable(f"{query_id}_1")
            
            # Calculate execution time
            end_time = time.time()
            duration = end_time - start_time
            
            # Store execution time
            execution_times.append({
                'query_name': query_id,
                'duration': duration,
                'configuration': configuration_name
            })
            
            print(f"Successfully saved {query_id} as Delta table. Execution time: {duration:.2f} seconds")
        except Exception as e:
            print(f"Error executing {query_id}: {str(e)}")
            # Store failed execution
            execution_times.append({
                'query_name': query_id,
                'duration': -1,  # -1 indicates failure
                'configuration': configuration_name,
                'error': str(e)
            })
    
    # Create a DataFrame from the execution times and save as CSV
    result_df = pd.DataFrame(execution_times)
    
    # Ensure Files directory exists
    os.makedirs('Files', exist_ok=True)
    
    # Save the results to a CSV file
    csv_path = f"Files/tpcds_benchmark_{configuration_name}.csv"
    result_df.to_csv(csv_path, index=False)
    
    print(f"All TPC-DS queries executed. Results saved to {csv_path}")
    return csv_path

execute_tpcds_queries("default_spark")



