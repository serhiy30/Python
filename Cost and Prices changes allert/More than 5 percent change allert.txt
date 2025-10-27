"""
Example: Automated Daily Delta Table Comparison Report
-----------------------------------------------------
This notebook compares two versions of a Delta table, calculates percentage
changes in price and cost, generates a summary by brand and vendor, exports
the results to Excel, and sends an HTML report via Microsoft Graph API.

All data sources, email addresses, and database names are anonymized.
"""

# Databricks notebook source
# DBTITLE 1,Libraries
# MAGIC %pip install bs4 xlsxwriter
# MAGIC from pyspark.sql import SparkSession
# MAGIC import pandas as pd
# MAGIC import requests
# MAGIC import base64
# MAGIC import io


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, lit, abs as abs_diff
from datetime import datetime, timedelta

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Daily Version Compare") \
    .getOrCreate()

# Step 1: Retrieve the latest timestamps
history_df = spark.sql("DESCRIBE HISTORY prod_db.products") \
    .filter("operation = 'MERGE'") \
    .select("version", "timestamp") \
    .orderBy(col("timestamp").desc()) \
    .limit(20)  # Fetch more rows to ensure we can find the right comparison point

# Add unix timestamp column
history_df = history_df.withColumn("timestamp_unix", unix_timestamp(col("timestamp")))

# Collect rows to driver
rows = history_df.collect()
if len(rows) < 2:
    raise Exception("Not enough records to compare.")

# Get the latest entry
latest_row = rows[0]
latest_unix = latest_row["timestamp_unix"]
first_version = latest_row["version"]

#  Define the target timestamp (24 hours ago from latest)
target_unix = latest_unix - 86400  # 86400 seconds in 24 hours

#  Find the row closest to 24 hours ago
closest_index = min(range(1, len(rows)), key=lambda i: abs(rows[i]["timestamp_unix"] - target_unix))
closest_row = rows[closest_index]

#  Check neighboring rows (before and after)
before_row = rows[closest_index - 1] if closest_index - 1 >= 0 else None
after_row = rows[closest_index + 1] if closest_index + 1 < len(rows) else None

# Check for entries within 15 minutes (900 seconds) around the closest row
candidates = [closest_row]

if before_row:
    diff = abs(closest_row["timestamp_unix"] - before_row["timestamp_unix"])
    if diff < 900:
        candidates.append(before_row)

if after_row:
    diff = abs(closest_row["timestamp_unix"] - after_row["timestamp_unix"])
    if diff < 900:
        candidates.append(after_row)

# Pick the most recent among the close candidates
second_latest_row = min(candidates, key=lambda r: -r["timestamp_unix"])  # higher timestamp = more recent
second_latest_version = second_latest_row["version"]

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
        prod_db.products VERSION AS OF {second_latest_version} AS pr
    WHERE
        pr.forsale != 'no'
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
        prod_db.products VERSION AS OF {first_version} AS pr
    WHERE
        pr.forsale != 'no'
    """
)

Brand_top_df = spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW Brand_top AS
    SELECT manufacturerid FROM 
    (
        SELECT
            br.manufacturerid,
            sm.brand,
            SUM(sm.gross_revenue) AS Gross
        FROM db.data.gold_tables.sales_data sm
        LEFT OUTER JOIN ref.manufacturers br ON br.manufacturer = sm.brand
        WHERE sm.order_type = 'Sale' 
            AND sm.order_date >= current_date() - INTERVAL 30 DAY
        GROUP BY br.manufacturerid, sm.brand
        ORDER BY Gross DESC
        LIMIT 20
    )
    """
).toPandas()

Vendor_top_df = spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW Vendor_top AS
    SELECT admin_id FROM 
    (
        SELECT
            v.admin_id, sm.vendor, SUM(sm.gross_revenue) AS Gross
        FROM db.data.gold_tables.sales_data sm
        LEFT JOIN db.data.gold_tables.admin_name_vendors v ON sm.vendor = v.netsuite_vendor
        WHERE sm.order_type ='Sale' 
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
    CREATE OR REPLACE TEMP VIEW price_changes AS
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
        CASE WHEN price_percentage_change != 0 THEN 1 ELSE 0 END price_mark
        
    FROM
        current_product AS current
    JOIN
        previous_product AS previous
    ON
        current.manufacturer_id = previous.manufacturer_id AND
        current.mpn = previous.mpn AND
        current.today_vendor_id = previous.today_vendor_id AND
        current.original_vendor_id = previous.original_vendor_id
    LEFT JOIN ref.manufacturers mn ON mn.manufacturerid = current.manufacturer_id
    LEFT JOIN ref.vendors vnd ON vnd.id = current.today_vendor_id
    LEFT JOIN ref.ptype_group gr ON gr.id=current.ptype_group_id
    """
).toPandas()


# COMMAND ----------

# DBTITLE 1,Add filterd data
Brand_df = spark.sql(""" CREATE OR REPLACE TEMP VIEW Brand_changes AS 
                    SELECT 
                    ic.* ,cd.domain 
                    FROM  price_changes ic
                    
                    INNER JOIN prod_db.products_source ps ON ps.mpn = ic.mpn AND ps.manufacturer_id = ic.manufacturer_id
                    INNER JOIN db.data.catalog.data.info.product.mpn_product mp ON mp.sku = ps.mpn AND mp.manufacturer_id = ps.manufacturer_id
                    INNER JOIN db.data.catalog.data.info.product.mpn_product_vehicle_id vi ON mp.id = vi.product_id
                    INNER JOIN db.data.catalog.data.info.product.catalog_domain cd ON vi.vehicle_id = cd.catalog_vehicle_id
                    
                    WHERE cd.domain = 'www.websiteofacompany.com'

                    Group BY ALL
                                    
                     """).toPandas()

Vendor_df = spark.sql(""" CREATE OR REPLACE TEMP VIEW Vendor_changes AS 
                      SELECT 
                    ic.* ,cd.domain 
                    FROM  price_changes ic
                    
                    INNER JOIN prod_db.products_source ps ON ps.mpn = ic.mpn AND ps.manufacturer_id = ic.manufacturer_id
                    INNER JOIN db.data.catalog.data.info.product.mpn_product mp ON mp.sku = ps.mpn AND mp.manufacturer_id = ps.manufacturer_id
                    INNER JOIN db.data.catalog.data.info.product.mpn_product_vehicle_id vi ON mp.id = vi.product_id
                    INNER JOIN db.data.catalog.data.info.product.catalog_domain cd ON vi.vehicle_id = cd.catalog_vehicle_id
                    
                    WHERE cd.domain = 'www.websiteofacompany.com'

                    Group BY ALL
                                        
                       """).toPandas()


# COMMAND ----------

from pyspark.sql import functions as F
from IPython.display import display, HTML

spark.sql("""
CREATE OR REPLACE TEMP VIEW Brand20 AS
SELECT 
    bc.domain,
    CASE WHEN bc.manufacturer_id IN (SELECT manufacturerid FROM Brand_top) THEN "Yes" ELSE "No" END AS Top_20_brand,
    bc.manufacturer_id AS id,
    bc.Brand,
    COUNT(DISTINCT bc.mpn) AS total_mpn,

    SUM(CASE WHEN bc.cost_percentage_change > 5 THEN 1 ELSE 0 END) AS cost_increased_more_than_5_count,
    ROUND(TRY_DIVIDE(SUM(CASE WHEN bc.cost_percentage_change > 5 THEN 1 ELSE 0 END), COUNT(DISTINCT bc.mpn)) * 100, 2) AS cost_increased_more_than_5_percentage,

    SUM(CASE WHEN bc.price_percentage_change > 5 THEN 1 ELSE 0 END) AS price_increased_more_than_5_count,
    ROUND(TRY_DIVIDE(SUM(CASE WHEN bc.price_percentage_change > 5 THEN 1 ELSE 0 END), COUNT(DISTINCT bc.mpn)) * 100, 2) AS price_increased_more_than_5_percentage,
    promotion.Promo_end_date

FROM Brand_changes bc

LEFT OUTER JOIN   
 (SELECT manufacturer_id, Brand, MAX(end_date) AS Promo_end_date
  FROM 
      (Select sp.product_id, pr.manufacturer_id, mn.manufacturer AS Brand , sp.start_date, sp.end_date
        FROM  
      db.data.catalog.data.info.product.product_sale_price sp
      LEFT OUTER JOIN prod_db.products pr ON pr.id = sp.product_id
      LEFT OUTER JOIN ref.manufacturers mn ON mn.manufacturerid = pr.manufacturer_id)
  GROUP BY manufacturer_id, Brand ) AS promo
ON bc.manufacturer_id = promotion.manufacturer_id

GROUP BY 
    bc.domain,
    bc.manufacturer_id,
    bc.Brand,
    promotion.Promo_end_date
""")

top_brand_df = spark.sql("SELECT * FROM Brand20").toPandas()

# Rename columns to readable English names
top_brand_df.rename(columns={
    'domain': 'Domain',
    'Top_20_brand': 'Top 20 Brand',
    'id': 'Brand ID',
    'Brand': 'Brand Name',
    'total_mpn': 'Total MPNs',
    'cost_increased_more_than_5_count': 'Cost ↑ >5%',
    'cost_increased_more_than_5_percentage': 'Cost ↑ >5% (%)',
    'price_increased_more_than_5_count': 'Price ↑ >5%',
    'price_increased_more_than_5_percentage': 'Price ↑ >5% (%)',
    'Promo_end_date' : 'End Last Promo'
}, inplace=True)

top_brand_df['End Last Promo'] = top_brand_df['End Last Promo'].apply(
    lambda x: "-" if pd.isna(x) or str(x) == "1970-01-01 00:00:02" else x.strftime("%Y-%m-%d"))

# Save numeric columns before formatting
top_brand_df['cost_numeric'] = top_brand_df['Cost ↑ >5% (%)']
top_brand_df['price_numeric'] = top_brand_df['Price ↑ >5% (%)']

# Filter for values above 4.99%
top_brand_df = top_brand_df[
    (top_brand_df['cost_numeric'] > 4.99) &
    (top_brand_df['price_numeric'] > 4.99)
]

# Format percentage columns
top_brand_df['Cost ↑ >5% (%)'] = top_brand_df['cost_numeric'].map(lambda x: f"{x:.2f}%")
top_brand_df['Price ↑ >5% (%)'] = top_brand_df['price_numeric'].map(lambda x: f"{x:.2f}%")

# Drop helper columns
top_brand_df.drop(columns=['cost_numeric', 'price_numeric'], inplace=True)

def color_above_5_percent(val):
    try:
        percentage_value = float(val.strip('%'))
        if percentage_value > 4.99: 
            return f'<span style="color:red">{val}</span>'
        else:
            return val
    except:
        return val

def color_top_20(val):
    return f'<span style="color:green">{val}</span>' if val == "Yes" else val

top_brand_df['Cost ↑ >5% (%)'] = top_brand_df['Cost ↑ >5% (%)'].apply(color_above_5_percent)
top_brand_df['Price ↑ >5% (%)'] = top_brand_df['Price ↑ >5% (%)'].apply(color_above_5_percent)
top_brand_df['Top 20 Brand'] = top_brand_df['Top 20 Brand'].apply(color_top_20)

html_table_1 = """
<style>
table {
  font-family: Arial, sans-serif;
  border-collapse: collapse;
  width: auto; 
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
""" + top_brand_df.to_html(index=False, escape=False)

display(HTML(html_table_1))


# COMMAND ----------

# DBTITLE 1,Exel by mpn
top_brand_XLX_df = spark.sql(""" 
 SELECT 
    manufacturer_id,
    CASE WHEN manufacturer_id IN (SELECT manufacturerid FROM Brand_top) THEN "Yes" ELSE "No" END AS Top_20_brand,
    Brand,
    mpn,
    ptype_id,
    ptype_group_id,
    pCat,
    today_vendor_id,
    Current_vendor_Name,
    current_inventory_quantity,
    current_cost,
    previous_cost,
    cost_percentage_change,
    current_price,
    previous_price,
    price_percentage_change
    
 FROM  price_changes WHERE manufacturer_id IN (SELECT id From Brand20)
 GROUP BY ALL
 HAVING 
    ((cost_percentage_change > 4.99) OR
    (price_percentage_change > 4.99))
    AND current_inventory_quantity !=0


 ORDER BY Top_20_brand Desc, manufacturer_id
    """).toPandas()

new_data_attach_1 = top_brand_XLX_df

display (top_brand_XLX_df)

# COMMAND ----------

average_brand_percentage_change_df = spark.sql("""
SELECT
    manufacturer_id AS id,
    CASE WHEN manufacturer_id IN (SELECT manufacturerid FROM Brand_top) THEN "Yes" ELSE "No" END AS Top_20_brand,
    Brand,
    AVG(cost_percentage_change) AS avg_cost_percentage_change,
    AVG(price_percentage_change) AS avg_price_percentage_change

FROM
    price_changes
WHERE manufacturer_id IN (SELECT id From Brand20)
GROUP BY
    manufacturer_id, Brand
HAVING 
    AVG(cost_percentage_change) > 4.99 OR
    AVG(price_percentage_change) > 4.99
ORDER BY
    Top_20_brand Desc, manufacturer_id, Brand
""").toPandas()

average_brand_percentage_change_df['avg_cost_percentage_change'] = average_brand_percentage_change_df['avg_cost_percentage_change'].map(lambda x: f"{x:.2f}%" if x is not None else "")
average_brand_percentage_change_df['avg_price_percentage_change'] = average_brand_percentage_change_df['avg_price_percentage_change'].map(lambda x: f"{x:.2f}%" if x is not None else "")

new_data_attach_3 = average_brand_percentage_change_df 

display(average_brand_percentage_change_df)

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TEMP VIEW Vendor20 AS
  SELECT 
    vc.domain,
    CASE WHEN vc.today_vendor_id IN (SELECT admin_id FROM Vendor_top) THEN "Yes" ELSE "No" END AS Top_20_Vendor,
    vc.today_vendor_id AS id,
    vc.Current_vendor_Name,
    p.total_mpn,

    SUM(CASE WHEN vc.cost_percentage_change > 5 THEN 1 ELSE 0 END) AS cost_increased_more_than_5_count,
    ROUND(TRY_DIVIDE(SUM(CASE WHEN vc.cost_percentage_change > 5 THEN 1 ELSE 0 END),  p.total_mpn) * 100, 2) AS cost_increased_more_than_5_percentage,

    SUM(CASE WHEN vc.price_percentage_change > 5 THEN 1 ELSE 0 END) AS price_increased_more_than_5_count,
    ROUND(TRY_DIVIDE(SUM(CASE WHEN vc.price_percentage_change > 5 THEN 1 ELSE 0 END),  p.total_mpn) * 100, 2) AS price_increased_more_than_5_percentage,
    promotion.Promo_end_date

  FROM Vendor_changes vc

  LEFT OUTER JOIN 
      (
        SELECT
          pr.today_vendor_id,
          COUNT(DISTINCT pr.mpn) AS total_mpn
        FROM prod_db.products pr
        GROUP BY pr.today_vendor_id
      ) AS p 
  ON vc.today_vendor_id = p.today_vendor_id

  LEFT OUTER JOIN 
      ( SELECT today_vendor_id, Current_vendor_Name, MAX(end_date) AS Promo_end_date
        FROM
            (Select sp.product_id, pr.manufacturer_id, mn.manufacturer AS Brand , 
                    pr.today_vendor_id,
                    vnd.name AS Current_vendor_Name,
            sp.start_date, sp.end_date
              FROM  
            db.data.catalog.data.info.product.product_sale_price sp
            Left outer JOIN prod_db.products pr ON pr.id = sp.product_id
            Left outer JOIN ref.manufacturers mn ON mn.manufacturerid = pr.manufacturer_id
            Left outer JOIN  ref.vendors vnd ON vnd.id = pr.today_vendor_id
            )
        GROUP BY today_vendor_id, Current_vendor_Name
      ) AS promo
      ON vc.today_vendor_id = promotion.today_vendor_id


  GROUP BY 
    vc.today_vendor_id,
    p.total_mpn,
    vc.domain,
    vc.Current_vendor_Name,
    promotion.Promo_end_date


  HAVING 
    ROUND(TRY_DIVIDE(SUM(CASE WHEN vc.cost_percentage_change > 5 THEN 1 ELSE 0 END),  p.total_mpn) * 100, 2) > 4.99 AND
    ROUND(TRY_DIVIDE(SUM(CASE WHEN vc.price_percentage_change > 5 THEN 1 ELSE 0 END), p.total_mpn) * 100, 2) > 4.99

  ORDER BY 
    Top_20_Vendor DESC,
    vc.today_vendor_id
""")

if spark.table("Vendor_changes").count() == 0:
    print("The dataset is empty.")
else:
    top_vendor_df = spark.sql("SELECT * FROM Vendor20").toPandas()

    # Rename columns to simpler English names
    top_vendor_df.rename(columns={
        'domain': 'Domain',
        'Top_20_Vendor': 'Top 20 Vendor',
        'id': 'Vendor ID',
        'Current_vendor_Name': 'Vendor Name',
        'total_mpn': 'Total MPNs',
        'cost_increased_more_than_5_count': 'Cost ↑ >5%',
        'cost_increased_more_than_5_percentage': 'Cost ↑ >5% (%)',
        'price_increased_more_than_5_count': 'Price ↑ >5%',
        'price_increased_more_than_5_percentage': 'Price ↑ >5% (%)',
        'Promo_end_date' : 'End Last Promo'
    }, inplace=True)

    top_vendor_df['End Last Promo'] = top_vendor_df['End Last Promo'].apply(
    lambda x: "-" if pd.isna(x) or str(x) == "1970-01-01 00:00:02" else x.strftime("%Y-%m-%d"))

    # Keep numeric versions for formatting
    top_vendor_df['cost_numeric'] = top_vendor_df['Cost ↑ >5% (%)']
    top_vendor_df['price_numeric'] = top_vendor_df['Price ↑ >5% (%)']

    # Format percentage columns with % symbol
    top_vendor_df['Cost ↑ >5% (%)'] = top_vendor_df['cost_numeric'].map(lambda x: f"{x:.2f}%")
    top_vendor_df['Price ↑ >5% (%)'] = top_vendor_df['price_numeric'].map(lambda x: f"{x:.2f}%")

    # Drop numeric helper columns
    top_vendor_df.drop(columns=['cost_numeric', 'price_numeric'], inplace=True)

    def color_above_5_percent(val):
        try:
            percentage_value = float(val.strip('%'))
            if percentage_value > 4.99:
                return f'<span style="color:red">{val}</span>'
            else:
                return val
        except:
            return val

    def color_top_20_vendor(val):
        return f'<span style="color:green">{val}</span>' if val == "Yes" else val

    top_vendor_df['Cost ↑ >5% (%)'] = top_vendor_df['Cost ↑ >5% (%)'].apply(color_above_5_percent)
    top_vendor_df['Price ↑ >5% (%)'] = top_vendor_df['Price ↑ >5% (%)'].apply(color_above_5_percent)
    top_vendor_df['Top 20 Vendor'] = top_vendor_df['Top 20 Vendor'].apply(color_top_20_vendor)

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
    """ + top_vendor_df.to_html(index=False, escape=False, justify='center')

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
    current_cost,
    previous_cost,
    cost_percentage_change,
    current_price,
    previous_price,
    price_percentage_change
   
 FROM  price_changes WHERE today_vendor_id IN (SELECT id From Vendor20)
 GROUP BY ALL
 HAVING 
     ((cost_percentage_change > 4.99) OR
    (price_percentage_change > 4.99))
    AND current_inventory_quantity !=0
    
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
    AVG(cost_percentage_change) AS avg_cost_percentage_change,
    AVG(price_percentage_change) AS avg_price_percentage_change
FROM
    price_changes
WHERE today_vendor_id IN (SELECT id From Vendor20)
GROUP BY
    today_vendor_id,
    Current_vendor_Name
HAVING 
    ((avg_cost_percentage_change > 4.99) OR
    (avg_price_percentage_change > 4.99))
ORDER BY
    Top_20_Vendor DESC,
    today_vendor_id,
    Current_vendor_Name
""").toPandas()

average_vendor_percentage_change_df['avg_cost_percentage_change'] = average_vendor_percentage_change_df['avg_cost_percentage_change'].map(lambda x: f"{x:.2f}%" if x is not None else "")
average_vendor_percentage_change_df['avg_price_percentage_change'] = average_vendor_percentage_change_df['avg_price_percentage_change'].map(lambda x: f"{x:.2f}%" if x is not None else "")
new_data_attach_4 = average_vendor_percentage_change_df 

display(average_vendor_percentage_change_df)

# COMMAND ----------

# Define your OAuth2.0 client credentials
client_id = dbutils.secrets.get(scope="portfolio_scope", key="client_id")
client_secret = dbutils.secrets.get(scope="email", key="client_secret")
tenant_id = dbutils.secrets.get(scope="email", key="tenant_id")
shared_mailbox_user_id = dbutils.secrets.get(scope="email", key="mailbox")

# Authenticate using OAuth2.0 client credentials (client_credentials grant type)
def get_access_token(client_id, client_secret, tenant_id):
    url = f'https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token'
    data = {
        client_id = "<your_client_id_here>"
	client_secret = "<your_client_secret_here>"
	tenant_id = "<your_tenant_id_here>"
	shared_mailbox_user_id = "<demo_mailbox_id>"
    }
# In demo mode, secrets are not retrieved from Databricks scope.
# Replace these with your own credentials if testing email sending.

    response = requests.post(url, data=data)
    response.raise_for_status()
    token = response.json().get('access_token')
    return token

# Get the access token
token = get_access_token(client_id, client_secret, tenant_id)

# Define the email endpoint for Microsoft Graph API, using the shared mailbox user ID
send_email_url = f'https://graph.microsoft.com/v1.0/users/{shared_mailbox_user_id}/sendMail'

# COMMAND ----------

output = io.BytesIO()
with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
    new_data_attach_1.to_excel(writer, sheet_name="Brands by MPN", index=0)
    new_data_attach_2.to_excel(writer, sheet_name="Vendors by MPN", index=0)
   

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
    subject = f"Daily Price & Cost changes - {timestamp}"

    body = f"""
    <html>
    <head>
    </head>
    <body>
    <p>Hello Team,</p>
    <p>Quantity of mpn's which have changed more than 5%:</p>
    <p>Changes by brand:</p>
    {html_table_1}
    <p>Changes by Vendor:</p>
    {html_table_2}
    <p>Please find in the file attached information about the changes in price and cost for each item which still have inventory on hands. </p>
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
            'toRecipients': [{'emailAddress': {'address': 'data-team@example.com'}}],
        'attachments': [{
            '@odata.type': '#microsoft.graph.fileAttachment',
            'name': f"Price_Cost_Changes_{timestamp}.xlsx",
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

