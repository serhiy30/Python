# Databricks notebook source
# DBTITLE 1,Libraries
# MAGIC %pip install bs4 xlsxwriter
# MAGIC from pyspark.sql import SparkSession
# MAGIC import pandas as pd
# MAGIC import requests
# MAGIC import base64
# MAGIC import io

# COMMAND ----------
# DBTITLE 1,Main Data
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import unix_timestamp
from datetime import timedelta

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Example App") \
    .getOrCreate()

# Step 1: Retrieve the latest three timestamps
history_df = spark.sql("DESCRIBE HISTORY main.inventory.data.product") \
    .filter("Operation = 'MERGE'") \
    .orderBy("timestamp", ascending=False) \
    .limit(10)
history_ship_df = spark.sql("DESCRIBE HISTORY main.inventory.data.product_calculated_shipping") \
    .filter("Operation = 'CREATE OR REPLACE TABLE AS SELECT'") \
    .orderBy("timestamp", ascending=False) \
    .limit(10)

# Convert to DataFrame and calculate time difference between first and second record
history_df = history_df.withColumn("timestamp_unix", unix_timestamp(col("timestamp")))
history_ship_df = history_ship_df.withColumn("timestamp_unix", unix_timestamp(col("timestamp")))

# Calculate time difference between the first and second latest timestamps (in seconds)
time_diff_product = history_df.collect()[0]["timestamp_unix"] - history_df.collect()[1]["timestamp_unix"]
time_diff_shipping = history_ship_df.collect()[0]["timestamp_unix"] - history_ship_df.collect()[1]["timestamp_unix"]

# Set First Merge version
first_version = history_df.collect()[0]["version"]
first_ship_version = history_ship_df.collect()[0]["version"]

# Check if the difference is less than an hour (3600 seconds)
if time_diff_product < 900:
    second_latest_version = history_df.collect()[2]["version"]  # Use third version if difference is less than 1 hour
else:
    second_latest_version = history_df.collect()[1]["version"]  # Use second version if difference is more than 1 hour

if time_diff_shipping < 900:
    second_latest_ship_version = history_ship_df.collect()[2]["version"]
else:
    second_latest_ship_version = history_ship_df.collect()[1]["version"]

# Step 2: Create temporary views for the previous data using the second latest version
# Create a temporary view for previous product data
spark.sql(
    f"""
    CREATE OR REPLACE TEMP VIEW previous_product AS
    SELECT
        pr.id,
        pr.manufacturer_id,
        pr.mpn,
        pr.ptype_id,
        pr.ptype_group_id,
        pr.original_vendor_id,
        pr.original_price,
        pr.original_cost,
        pr.today_vendor_id,
        pr.total_quantity,
        pr.today_cost,
        pr.today_price
    FROM
        main.inventory.data.product VERSION AS OF {second_latest_version} AS pr
    WHERE
        pr.forsale != 'no'
    """
)

# Create a temporary view for previous shipping data
spark.sql(
    f"""
    CREATE OR REPLACE TEMP VIEW previous_shipping AS
    SELECT
        sh.vendor_id,
        sh.product_id,
        sh.shipping_price,
        sh.shipping_cost,
        sh.shipping_zone_id
    FROM
        main.inventory.data.product_calculated_shipping VERSION AS OF {second_latest_ship_version} AS sh
    WHERE
        sh.shipping_zone_id = 1
    """
)

# Create the previous product view with shipping data
spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW previous_product_with_shipping AS
    SELECT
        pr.*,
        IFNULL(ship.shipping_price, 0) AS shipping_price,
        IFNULL(ship.shipping_cost, 0) AS shipping_cost
    FROM
        previous_product pr
    LEFT OUTER JOIN
        previous_shipping ship
    ON
        ship.product_id = pr.id AND pr.today_vendor_id = ship.vendor_id
    """
)

# Step 3: Create a temporary view for current data
spark.sql(
    f"""
    CREATE OR REPLACE TEMP VIEW current_product AS
    SELECT
        pr.id,
        pr.manufacturer_id,
        pr.mpn,
        pr.ptype_id,
        pr.ptype_group_id,
        pr.original_vendor_id,
        pr.original_price,
        pr.original_cost,
        pr.today_vendor_id,
        pr.total_quantity,
        pr.today_cost,
        pr.today_price
    FROM
        main.inventory.data.product VERSION AS OF {first_version} AS pr
    WHERE
        pr.forsale != 'no'
    """
)

# Create a temporary view for current shipping data
spark.sql(
    f"""
    CREATE OR REPLACE TEMP VIEW current_shipping AS
    SELECT
        sh.vendor_id,
        sh.product_id,
        sh.shipping_price,
        sh.shipping_cost,
        sh.shipping_zone_id
    FROM
        main.inventory.data.product_calculated_shipping VERSION AS OF {first_ship_version} AS sh
    WHERE
        sh.shipping_zone_id = 1
    """
)

# Create the current product view with shipping data
spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW current_product_with_shipping AS
    SELECT
        pr.*,
        IFNULL(ship.shipping_price, 0) AS shipping_price,
        IFNULL(ship.shipping_cost, 0) AS shipping_cost
    FROM
        current_product pr
    LEFT OUTER JOIN
        current_shipping ship
    ON
        ship.product_id = pr.id AND pr.today_vendor_id = ship.vendor_id
    """
)

# Create the list off Top 20 Brands for las 30 days by Gross revenue
Brand_top_df = spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW Brand_top AS
    SELECT manufacturer_id FROM 
    (
        SELECT
            br.manufacturer_id,
            sm.brand,
            SUM(sm.gross_revenue) AS Gross
        FROM main.dataset.data.gold_tables.grouped.sales_data sm
        LEFT OUTER JOIN website.data.main.chart_manufacturers br ON br.manufacturer = sm.brand
        WHERE sm.order_type = 'Saled_order' 
            AND sm.order_date >= current_date() - INTERVAL 30 DAY
        GROUP BY br.manufacturer_id, sm.brand
        ORDER BY Gross DESC
        LIMIT 20
    )
    """
).toPandas()

# Create the list off Top 20 Vendors for las 30 days by Gross revenue
## **In that case some of vendors have diferent names in different sources**

Vendor_top_df = spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW Vendor_top AS
    SELECT admin_id FROM 
    (
        SELECT
            v.admin_id, sm.vendor, SUM(sm.gross_revenue) AS Gross
        FROM main.dataset.data.gold_tables.grouped.sales_data sm
        LEFT JOIN main.dataset.data.gold_tables.grouped.admin_ns_vendors v ON sm.vendor = v.ns_vendor 
        WHERE sm.order_type = 'Saled_order' 
            AND sm.order_date >= current_date() - INTERVAL 30 DAY
        GROUP BY ALL
        ORDER BY Gross DESC
        LIMIT 20
    )
    """
).toPandas()

# Step 4: Create a temporary view for significant inventory changes
spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW inventory_changes AS
    SELECT
        current.id,
        current.manufacturer_id,
        mn.manufacturer AS Brand,
        current.mpn,
        current.ptype_id,
        current.ptype_group_id,
        gr.name AS pCat,
        current.today_vendor_id,
        vnd.name AS Current_vendor_Name,
        current.total_quantity AS current_inventory_quantity,
        previous.total_quantity AS previous_inventory_quantity,
        CASE
            WHEN ABS(current.total_quantity - previous.total_quantity) < 1E-10 THEN 0
            ELSE ifnull(ROUND(try_divide(current.total_quantity - previous.total_quantity, previous.total_quantity) * 100, 2), 0)
        END AS inventory_percentage_change,
        CASE WHEN inventory_percentage_change != 0 THEN 1 ELSE 0 END inv_mark,
        CASE WHEN (current.total_quantity = 0 AND previous.total_quantity != 0) THEN 1 ELSE 0 END AS OOS_mark,
        CASE WHEN (current.total_quantity != 0 AND previous.total_quantity = 0) THEN 1 ELSE 0 END AS INS_mark,
        current.today_cost AS current_cost,
        previous.today_cost AS previous_cost,
        CASE
            WHEN ABS(current.today_cost - previous.today_cost) < 1E-10 THEN 0
            ELSE ifnull(ROUND(try_divide(current.today_cost - previous.today_cost, previous.today_cost) * 100, 2), 0)
        END AS cost_percentage_change,
        CASE WHEN cost_percentage_change != 0 THEN 1 ELSE 0 END cost_mark,
        current.today_price AS current_price,
        previous.today_price AS previous_price,
        CASE
            WHEN ABS(current.today_price - previous.today_price) < 1E-10 THEN 0
            ELSE ifnull(ROUND(try_divide(current.today_price - previous.today_price, previous.today_price) * 100, 2), 0)
        END AS price_percentage_change,
        CASE WHEN price_percentage_change != 0 THEN 1 ELSE 0 END price_mark,
        current.shipping_cost AS current_ship_cost,
        previous.shipping_cost AS previous_ship_cost,
        CASE
            WHEN ABS(current.shipping_cost - previous.shipping_cost) < 1E-10 THEN 0
            ELSE ifnull(ROUND(try_divide(current.shipping_cost - previous.shipping_cost, previous.shipping_cost) * 100, 2), 0)
        END AS ship_cost_percentage_change,
        CASE WHEN ship_cost_percentage_change != 0 THEN 1 ELSE 0 END ship_cost_mark,
        current.shipping_price AS current_ship_price,
        previous.shipping_price AS previous_ship_price,
        CASE
            WHEN ABS(current.shipping_price - previous.shipping_price) < 1E-10 THEN 0
            ELSE ifnull(ROUND(try_divide(current.shipping_price - previous.shipping_price, previous.shipping_price) * 100, 2), 0)
        END AS ship_price_percentage_change,
        CASE WHEN ship_price_percentage_change != 0 THEN 1 ELSE 0 END ship_price_mark
    FROM
        current_product_with_shipping AS current
    JOIN
        previous_product_with_shipping AS previous
    ON
        current.manufacturer_id = previous.manufacturer_id AND
        current.mpn = previous.mpn AND
        current.today_vendor_id = previous.today_vendor_id AND
        current.original_vendor_id = previous.original_vendor_id
    LEFT JOIN website.data.main.chart_manufacturers mn ON mn.manufacturer_id = current.manufacturer_id
    LEFT JOIN website.data.main.vendor vnd ON vnd.id = current.today_vendor_id
    LEFT JOIN website.data.main.srs_ptype_group gr ON gr.id=current.ptype_group_id
    """
).toPandas()

# COMMAND ----------

# DBTITLE 1,Add filterd data
Brand_df = spark.sql(""" CREATE OR REPLACE TEMP VIEW Brand_changes AS 
                    SELECT 
                    ic.* ,cd.domain 
                    FROM  inventory_changes ic
                    
                    INNER JOIN website.data.main.product_source ps ON ps.mpn = ic.mpn AND ps.manufacturer_id = ic.manufacturer_id
                    INNER JOIN website.data.main.mpn_product mp ON mp.sku = ps.mpn AND mp.manufacturer_id = ps.manufacturer_id
                    INNER JOIN website.data.main.mpn_product_vehicle_id vi ON mp.id = vi.product_id
                    INNER JOIN website.data.main.catalog_domain cd ON vi.vehicle_id = cd.catalog_vehicle_id
                    
                    WHERE cd.domain = 'www.example.com'

                    Group BY ALL
                                    
                     """).toPandas()

Vendor_df = spark.sql(""" CREATE OR REPLACE TEMP VIEW Vendor_changes AS 
                      SELECT 
                    ic.* ,cd.domain 
                    FROM  inventory_changes ic
                    
                    INNER JOIN website.data.main.product_source ps ON ps.mpn = ic.mpn AND ps.manufacturer_id = ic.manufacturer_id
                    INNER JOIN website.data.main.mpn_product mp ON mp.sku = ps.mpn AND mp.manufacturer_id = ps.manufacturer_id
                    INNER JOIN website.data.main.mpn_product_vehicle_id vi ON mp.id = vi.product_id
                    INNER JOIN website.data.main.catalog_domain cd ON vi.vehicle_id = cd.catalog_vehicle_id
                    
                    WHERE cd.domain = 'www.example.com'

                    Group BY ALL
                                        
                       """).toPandas()


# COMMAND ----------

# DBTITLE 1,Body Brand Table

from pyspark.sql import functions as F
from pyspark.sql.functions import col
from IPython.display import display, HTML

spark.sql(""" CREATE OR REPLACE TEMP VIEW Brand20 AS
 SELECT 
    bc.domain,
    CASE WHEN bc.manufacturer_id IN (SELECT manufacturer_id FROM Brand_top) THEN "Yes" ELSE "No" END AS Top_20_brand,
    bc.manufacturer_id AS id,
    bc.Brand,
    p.total_mpn,
    COUNT(DISTINCT bc.mpn) AS mpn_changed,
    --SUM(inv_mark) AS inventory_changed,
    CONCAT(ROUND(try_divide(SUM(bc.ins_mark), COUNT(DISTINCT bc.mpn)) * 100, 2), '%') AS in_stock_change_percentage,
    --SUM(oos_mark) AS oos_due_to_upd_count,
    CONCAT(ROUND(try_divide(SUM(bc.oos_mark), COUNT(DISTINCT bc.mpn)) * 100, 2), '%') AS out_of_stock_change_percentage,
    --SUM(cost_mark) AS cost_changed_count,
    CONCAT(ROUND(try_divide(SUM(bc.cost_mark), COUNT(DISTINCT bc.mpn)) * 100, 2), '%') AS cost_changed_percentage,
    --SUM(price_mark) AS price_changed_count,
    CONCAT(ROUND(try_divide(SUM(bc.price_mark), COUNT(DISTINCT bc.mpn)) * 100, 2), '%') AS price_changed_percentage,
    --SUM(ship_cost_mark) AS ship_cost_changed_count,
    CONCAT(ROUND(try_divide(SUM(bc.ship_cost_mark), COUNT(DISTINCT bc.mpn)) * 100, 2), '%') AS ship_cost_changed_percentage,
    --SUM(ship_price_mark) AS ship_price_changed_count,
    CONCAT(ROUND(try_divide(SUM(bc.ship_price_mark), COUNT(DISTINCT bc.mpn)) * 100, 2), '%') AS ship_price_changed_percentage

  FROM 
    Brand_changes bc

 LEFT OUTER JOIN 
  (
     SELECT
      pr.manufacturer_id,
      COUNT(DISTINCT pr.mpn) AS total_mpn
     FROM website.data.main.product pr
     GROUP BY pr.manufacturer_id
    ) AS p ON bc.manufacturer_id = p.manufacturer_id

    
  GROUP BY 
    bc.manufacturer_id,
     p.total_mpn,
    bc.domain,
    bc.Brand  
 HAVING 
    ROUND(try_divide(SUM(bc.ins_mark), COUNT(DISTINCT bc.mpn)) * 100, 2) >= 5 OR
    ROUND(try_divide(SUM(bc.oos_mark), COUNT(DISTINCT bc.mpn)) * 100, 2) >= 5 OR
    ROUND(try_divide(SUM(bc.cost_mark), COUNT(DISTINCT bc.mpn)) * 100, 2) >= 5 OR
    ROUND(try_divide(SUM(bc.price_mark), COUNT(DISTINCT bc.mpn)) * 100, 2) >= 5 OR
    ROUND(try_divide(SUM(bc.ship_cost_mark), COUNT(DISTINCT bc.mpn)) * 100, 2) >= 5 OR
    ROUND(try_divide(SUM(bc.ship_price_mark), COUNT(DISTINCT bc.mpn)) * 100, 2) >= 5

  ORDER BY 
    Top_20_brand Desc, bc.manufacturer_id;""").toPandas()
 
# Check if the "Brand_changes" table is empty
if spark.table("Brand_changes").count() == 0:
    print("The dataset is empty.")
else:
    # Execute SQL query and convert the result to a Pandas DataFrame
    top_brand_df = spark.sql("SELECT * FROM Brand20").toPandas()

    # Function to color values above 5% in red
    def color_above_5_percent(val):
        try:
            percentage_value = float(val.strip('%'))
            if percentage_value > 4.99: 
                return f'<span style="color:red">{val}</span>'
            else:
                return val
        except ValueError:
            return val  # Return the value unchanged if it cannot be converted to float

    # Function to color 'Yes' in green in the 'Top_20_brand' column
    def color_top_20_brand(val):
        if val == "Yes":
            return f'<span style="color:green">{val}</span>'
        else:
            return val

    # List of columns to apply styling
    columns_to_style = ['in_stock_change_percentage', 'out_of_stock_change_percentage', 'cost_changed_percentage', 'price_changed_percentage', 'ship_cost_changed_percentage', 'ship_price_changed_percentage']

    # Apply styling to the specified columns
    for column in columns_to_style:
        top_brand_df[column] = top_brand_df[column].apply(color_above_5_percent)

    # Apply styling to the 'Top_20_brand' column
    top_brand_df['Top_20_brand'] = top_brand_df['Top_20_brand'].apply(color_top_20_brand)

    # Create HTML for displaying the table with custom styles
    html_table_1 = """
    <style>
    table {
      font-family: Arial, sans-serif;
      border-collapse: collapse;
      width: Auto; 
    }

    td, th {
      border: 1px solid #dddddd;
      text-align: left;
      padding: 8px;
      font-size: 14px;
      word-wrap: break-word; 
    }

    tr:nth-child(even) {
      background-color: #f2f2f2;
    }
    </style>
    """ + top_brand_df.to_html(index=False, escape=False, justify='center')
    
    # Display the table
    display(HTML(html_table_1))
    


# COMMAND ----------

# DBTITLE 1,Exel by mpn
top_brand_XLX_df = spark.sql(""" 
 SELECT 
    manufacturer_id,
    CASE WHEN manufacturer_id IN (SELECT manufacturer_id FROM Brand_top) THEN "Yes" ELSE "No" END AS Top_20_brand,
    Brand,
    mpn,
    ptype_id,
    ptype_group_id,
    pCat,
    today_vendor_id,
    Current_vendor_Name,
    current_inventory_quantity,
    previous_inventory_quantity,
    inventory_percentage_change,
    current_cost,
    previous_cost,
    cost_percentage_change,
    current_price,
    previous_price,
    price_percentage_change,
    current_ship_cost,
    previous_ship_cost,
    ship_cost_percentage_change,
    current_ship_price,
    previous_ship_price,
    ship_price_percentage_change, 
    OOS_mark
 FROM  inventory_changes WHERE manufacturer_id IN (SELECT id From Brand20)
 GROUP BY ALL
 HAVING 
    inventory_percentage_change != 0 OR
    cost_percentage_change != 0 OR
    price_percentage_change != 0 OR
    ship_cost_percentage_change != 0 OR
    ship_price_percentage_change != 0
 ORDER BY Top_20_brand Desc, manufacturer_id
    """).toPandas()

new_data_attach_1 = top_brand_XLX_df

display (top_brand_XLX_df)

# COMMAND ----------

average_brand_percentage_change_df = spark.sql("""
SELECT
    manufacturer_id AS id,
    CASE WHEN manufacturer_id IN (SELECT manufacturer_id FROM Brand_top) THEN "Yes" ELSE "No" END AS Top_20_brand,
    Brand,
    AVG(inventory_percentage_change) AS avg_inventory_percentage_change,
    AVG(cost_percentage_change) AS avg_cost_percentage_change,
    AVG(price_percentage_change) AS avg_price_percentage_change,
    AVG(ship_cost_percentage_change) AS avg_ship_cost_percentage_change,
    AVG(ship_price_percentage_change) AS avg_ship_price_percentage_change
FROM
    inventory_changes
WHERE manufacturer_id IN (SELECT id From Brand20)
GROUP BY
    manufacturer_id, Brand
ORDER BY
    Top_20_brand Desc, manufacturer_id, Brand
""").toPandas()

average_brand_percentage_change_df['avg_inventory_percentage_change'] = average_brand_percentage_change_df['avg_inventory_percentage_change'].map(lambda x: f"{x:.2f}%" if x is not None else "")
average_brand_percentage_change_df['avg_cost_percentage_change'] = average_brand_percentage_change_df['avg_cost_percentage_change'].map(lambda x: f"{x:.2f}%" if x is not None else "")
average_brand_percentage_change_df['avg_price_percentage_change'] = average_brand_percentage_change_df['avg_price_percentage_change'].map(lambda x: f"{x:.2f}%" if x is not None else "")
average_brand_percentage_change_df['avg_ship_price_percentage_change'] = average_brand_percentage_change_df['avg_ship_price_percentage_change'].map(lambda x: f"{x:.2f}%" if x is not None else "")
average_brand_percentage_change_df['avg_ship_cost_percentage_change'] = average_brand_percentage_change_df['avg_ship_cost_percentage_change'].map(lambda x: f"{x:.2f}%" if x is not None else "")

new_data_attach_3 = average_brand_percentage_change_df 

display(average_brand_percentage_change_df)

# COMMAND ----------

# DBTITLE 1,Body Vendor table

from pyspark.sql import functions as F
from pyspark.sql.functions import col
from IPython.display import display, HTML

spark.sql(""" CREATE OR REPLACE TEMP VIEW Vendor20 AS
  SELECT 
    vc.domain,
    CASE WHEN vc.today_vendor_id IN (SELECT admin_id FROM Vendor_top) THEN "Yes" ELSE "No" END AS Top_20_Vendor,
    vc.today_vendor_id AS id,
    vc.Current_vendor_Name,
    p.total_mpn,
    COUNT(DISTINCT vc.mpn) AS mpn_changed,
    --SUM(inv_mark) AS inventory_changed,
    CONCAT(ROUND(try_divide(SUM(vc.ins_mark), COUNT(DISTINCT vc.mpn)) * 100, 2), '%') AS in_stock_change_percentage,
    --SUM(oos_mark) AS oos_due_to_upd_count,
    CONCAT(ROUND(try_divide(SUM(vc.oos_mark), COUNT(DISTINCT vc.mpn)) * 100, 2), '%') AS out_of_stock_change_percentage,
    --SUM(cost_mark) AS cost_changed_count,
    CONCAT(ROUND(try_divide(SUM(vc.cost_mark), COUNT(DISTINCT vc.mpn)) * 100, 2), '%') AS cost_changed_percentage,
    --SUM(price_mark) AS price_changed_count,
    CONCAT(ROUND(try_divide(SUM(vc.price_mark), COUNT(DISTINCT vc.mpn)) * 100, 2), '%') AS price_changed_percentage,
    --SUM(ship_cost_mark) AS ship_cost_changed_count,
    CONCAT(ROUND(try_divide(SUM(vc.ship_cost_mark), COUNT(DISTINCT vc.mpn)) * 100, 2), '%') AS ship_cost_changed_percentage,
    --SUM(ship_price_mark) AS ship_price_changed_count,
    CONCAT(ROUND(try_divide(SUM(vc.ship_price_mark), COUNT(DISTINCT vc.mpn)) * 100, 2), '%') AS ship_price_changed_percentage

  FROM 
    Vendor_changes vc

  LEFT OUTER JOIN 
  (
     SELECT
      pr.today_vendor_id,
      COUNT(DISTINCT pr.mpn) AS total_mpn
     FROM website.data.main.product pr
     GROUP BY pr.today_vendor_id
    ) AS p ON vc.today_vendor_id = p.today_vendor_id



  GROUP BY 
    vc.today_vendor_id,
    p.total_mpn,
    vc.domain,
    vc.Current_vendor_Name
 HAVING 
    ROUND(try_divide(SUM(vc.ins_mark), COUNT(DISTINCT vc.mpn)) * 100, 2) >= 5 OR
    ROUND(try_divide(SUM(vc.oos_mark), COUNT(DISTINCT vc.mpn)) * 100, 2) >= 5 OR
    ROUND(try_divide(SUM(vc.cost_mark), COUNT(DISTINCT vc.mpn)) * 100, 2) >= 5 OR
    ROUND(try_divide(SUM(vc.price_mark), COUNT(DISTINCT vc.mpn)) * 100, 2) >= 5 OR
    ROUND(try_divide(SUM(vc.ship_cost_mark), COUNT(DISTINCT vc.mpn)) * 100, 2) >= 5 OR
    ROUND(try_divide(SUM(vc.ship_price_mark), COUNT(DISTINCT vc.mpn)) * 100, 2) >= 5

  ORDER BY 
    Top_20_Vendor DESC,vc.today_vendor_id""").toPandas()

# Check if the "Vendor_changes" table is empty
if spark.table("Vendor_changes").count() == 0:
    print("The dataset is empty.")
else:
    # Execute SQL query and convert the result to a Pandas DataFrame
    top_Vendor_df = spark.sql("SELECT * FROM Vendor20").toPandas()

    # Function to color values above 5% in red
    def color_above_5_percent(val):
        try:
            percentage_value = float(val.strip('%'))
            if percentage_value > 4.99: 
                return f'<span style="color:red">{val}</span>'
            else:
                return val
        except ValueError:
            return val  # Return the value unchanged if it cannot be converted to float

    # Function to color "Top_20_Vendor" as green if the value is "Yes"
    def color_top_20_vendor(val):
        if val == "Yes":
            return f'<span style="color:green">{val}</span>'
        else:
            return val

    # List of columns to apply percentage styling
    columns_to_style = ['in_stock_change_percentage', 'out_of_stock_change_percentage', 'cost_changed_percentage', 'price_changed_percentage', 'ship_cost_changed_percentage','ship_price_changed_percentage']

    # Apply styling to the specified percentage columns
    for column in columns_to_style:
        top_Vendor_df[column] = top_Vendor_df[column].apply(color_above_5_percent)

    # Apply styling to the "Top_20_Vendor" column
    top_Vendor_df['Top_20_Vendor'] = top_Vendor_df['Top_20_Vendor'].apply(color_top_20_vendor)

    # Create HTML for displaying the table with custom styles
    html_table_2 = """
    <style>
    table {
      font-family: Arial, sans-serif;
      border-collapse: collapse;
      width: Auto; 
    }

    td, th {
      border: 1px solid #dddddd;
      text-align: left;
      padding: 8px;
      font-size: 14px;
      word-wrap: break-word; 
    }

    tr:nth-child(even) {
      background-color: #f2f2f2;
    }
    </style>
    """ + top_Vendor_df.to_html(index=False, escape=False, justify='center')
    
    # Display the table
    display(HTML(html_table_2))

# COMMAND ----------

# DBTITLE 1,Excel Vendor Table
top_Vendor_XLX_df = spark.sql(""" 
 SELECT 
    today_vendor_id,
    CASE WHEN today_vendor_id IN (SELECT admin_id FROM Vendor_top) THEN "Yes" ELSE "No" END AS Top_20_Vendor,
    Current_vendor_Name, 
    manufacturer_id,
    Brand,
    mpn,
    ptype_id,
    ptype_group_id,
    pCat,
    current_inventory_quantity,
    previous_inventory_quantity,
    inventory_percentage_change,
    current_cost,
    previous_cost,
    cost_percentage_change,
    current_price,
    previous_price,
    price_percentage_change,
    current_ship_cost,
    previous_ship_cost,
    ship_cost_percentage_change,
    current_ship_price,
    previous_ship_price,
    ship_price_percentage_change, 
    OOS_mark
 FROM  inventory_changes WHERE today_vendor_id IN (SELECT id From Vendor20)
 GROUP BY ALL
 HAVING 
    inventory_percentage_change != 0 OR
    cost_percentage_change != 0 OR
    price_percentage_change != 0 OR
    ship_cost_percentage_change != 0 OR
    ship_price_percentage_change != 0
 ORDER BY Top_20_Vendor DESC, manufacturer_id
    """).toPandas()


new_data_attach_2 = top_Vendor_XLX_df 

display (top_Vendor_XLX_df) 

# COMMAND ----------

average_vendor_percentage_change_df = spark.sql("""
SELECT
    today_vendor_id AS id,
    CASE WHEN today_vendor_id IN (SELECT admin_id FROM Vendor_top) THEN "Yes" ELSE "No" END AS Top_20_Vendor,
    Current_vendor_Name,
    AVG(inventory_percentage_change) AS avg_inventory_percentage_change,
    AVG(cost_percentage_change) AS avg_cost_percentage_change,
    AVG(price_percentage_change) AS avg_price_percentage_change,
    AVG(ship_cost_percentage_change) AS avg_ship_cost_percentage_change,
    AVG(ship_price_percentage_change) AS avg_ship_price_percentage_change
FROM
    inventory_changes
WHERE today_vendor_id IN (SELECT id From Vendor20)
GROUP BY
    today_vendor_id,
    Current_vendor_Name
ORDER BY
    Top_20_Vendor DESC,
    today_vendor_id,
    Current_vendor_Name
""").toPandas()

average_vendor_percentage_change_df['avg_inventory_percentage_change'] = average_vendor_percentage_change_df['avg_inventory_percentage_change'].map(lambda x: f"{x:.2f}%" if x is not None else "")
average_vendor_percentage_change_df['avg_cost_percentage_change'] = average_vendor_percentage_change_df['avg_cost_percentage_change'].map(lambda x: f"{x:.2f}%" if x is not None else "")
average_vendor_percentage_change_df['avg_price_percentage_change'] = average_vendor_percentage_change_df['avg_price_percentage_change'].map(lambda x: f"{x:.2f}%" if x is not None else "")
average_vendor_percentage_change_df['avg_ship_price_percentage_change'] = average_vendor_percentage_change_df['avg_ship_price_percentage_change'].map(lambda x: f"{x:.2f}%" if x is not None else "")
average_vendor_percentage_change_df['avg_ship_cost_percentage_change'] = average_vendor_percentage_change_df['avg_ship_cost_percentage_change'].map(lambda x: f"{x:.2f}%" if x is not None else "")

new_data_attach_4 = average_vendor_percentage_change_df 

display(average_vendor_percentage_change_df)

# COMMAND ----------

# Define your OAuth2.0 client credentials
client_id = dbutils.secrets.get(scope="email", key="client_id")
client_secret = dbutils.secrets.get(scope="email", key="client_secret")
tenant_id = dbutils.secrets.get(scope="email", key="tenant_id")
shared_mailbox_user_id = dbutils.secrets.get(scope="email", key="Example_mailbox")

# Authenticate using OAuth2.0 client credentials (client_credentials grant type)
def get_access_token(Example_client_id, Example_client_secret, Example_tenant_id):
    url = f'https://login.microsoftonline.com/{Example_tenant_id}/oauth2/token'
    data = {
        'grant_type': 'client_example_credentials',
        'client_id': Example_client_id,
        'client_secret': Example_client_secret,
        'scope': 'https://graph.microsoft.com/.default',
    }

    response = requests.post(url, data=data)
    response.raise_for_status()
    token = response.json().get('example_access_token')
    return token

# Get the access token
token = get_access_token(Example_client_id, Example_client_secret, Example_tenant_id)

# Define the email endpoint for Microsoft Graph API, using the shared mailbox user ID
send_email_url = f'https://graph.microsoft.com/v1.0/users/{shared_example_mailbox_user_id}/sendMail'

# COMMAND ----------

output = io.BytesIO()
with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
    new_data_attach_1.to_excel(writer, sheet_name="Top brands by MPN", index=0)
    new_data_attach_2.to_excel(writer, sheet_name="Top Vendors by MPN", index=0)
    new_data_attach_3.to_excel(writer, sheet_name="Top brands AVG change", index=0)
    new_data_attach_4.to_excel(writer, sheet_name="Top Vendors AVG change", index=0)


output.seek(0)

# COMMAND ----------

from datetime import datetime
import pytz 

utc_minus_5 = pytz.timezone('Etc/GMT+4')

timestamp = datetime.now(utc_minus_5).strftime("%m-%d-%Y %H:%M:%S")

# COMMAND ----------

from bs4 import BeautifulSoup

def has_data_in_table(html_table):
    soup = BeautifulSoup(html_table, 'html.parser')
    table_rows = soup.find_all('tr')  
    return len(table_rows) > 1 

if not has_data_in_table(html_table_1) and not has_data_in_table(html_table_2):
    print("Both HTML tables are empty or contain only headers. Email will not be sent.")
else:
    subject = f"UPDATE TOP Change on data in product database - {timestamp}"

    body = f"""
    <html>
    <head>
    </head>
    <body>
    <p>Hello Team,</p>
    <p>Please find the list of items in attachment.</p>
    <p> Significant Changes count by Brand (MPN's):</p>
    {html_table_1}
    <p> Significant Changes count by Vendor (MPN's):</p>
    {html_table_2}
    <p>For any questions, please reach out to <a href="mailto:example1@mail.com">example1@mail.com</a>.</p>
    <p>Thank you!</p>
    </body>
    </html>
    """

    print(subject)
    print(body)

    base64_encoded_content = base64.b64encode(output.getvalue()).decode('utf-8')

    # Create the email message with attachment
    email_message = {
        'message': {
            'subject': subject,
            'body': {
                'contentType': 'html',
                'content': body
            },
            'toRecipients': [
                {'emailAddress': {'address': 'Example1@mail.com'}},
                {'emailAddress': {'address': 'Example2@mail.com'}}
            ],

            'ccRecipients': [
                {'emailAddress': {'address': 'Example1@mail.com'}},
                {'emailAddress': {'address': 'Example2@mail.com'}}
            ],

            'attachments': [
                {
                    '@odata.type': '#microsoft.graph.fileAttachment',
                    'name': f"Update_Top_brands_and_vendors_by_product_table_{timestamp}.xlsx",  # Set the desired file name
                    'contentBytes': base64_encoded_content,
                }
            ]
        }
    }

    # Send the email with attachment using Microsoft Graph API
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json',
    }

    response = requests.post(send_email_url, headers=headers, json=email_message)
    response.raise_for_status()

    print('Email with attachment sent successfully!')