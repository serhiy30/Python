# COMMAND ----------
# MAGIC
# MAGIC %pip install xlsxwriter
# MAGIC import pandas as pd
# MAGIC import requests
# MAGIC import base64
# MAGIC import io

# COMMAND ----------
# DBTITLE 1,Hourly
## SQL Query's
# Fetch hourly agent-level data
Agents_df = spark.sql("""
    SELECT Agent_No, Agent_Name, Team_No, Team_Name, Conversion, Orders, Gross_Revenue, Interactions, AOV
    FROM main.reports.hourly.cc_report_current_day_hourly 
    ORDER BY Team_Name, Gross_Revenue DESC
""").toPandas()

# Fetch hourly team-level leaderboard
leaderboard_df = spark.sql("""
    SELECT 
        Team_No, Team_Name, Supervisor_Name, 
        CONCAT(ROUND(IFNULL(SUM(Orders) / SUM(Interactions), 0) * 100, 2), '%') AS Conversion, 
        SUM(Orders) AS Orders, 
        IFNULL(ROUND(SUM(Gross_Revenue), 2), 0) AS Gross_Revenue, 
        SUM(Interactions) AS Interactions, 
        ROUND(IFNULL(TRY_DIVIDE(SUM(Gross_Revenue), SUM(Orders)), 0), 2) AS AOV 
    FROM main.reports.hourly.cc_report_current_day_hourly
    GROUP BY Team_No, Team_Name, Supervisor_Name
    ORDER BY Gross_Revenue DESC
""").toPandas()

# Function to filter dataframe by Team_No
def filter_df(df, team_no):
    if 'Team_No' in df.columns:
        return df[df['Team_No'] == team_no]
    return df

# Function to calculate total values for each team
def add_team_totals(df):
    grouped_df = df.groupby('Team_No').agg({
        'Orders': 'sum',
        'Gross_Revenue': 'sum',
        'Interactions': 'sum'
    }).reset_index()

    grouped_df['Agent_No'] = 'Total'
    grouped_df['Agent_Name'] = ''
    grouped_df['Team_Name'] = ''
    grouped_df['Team_No'] = ''
    grouped_df['Conversion'] = grouped_df.apply(
        lambda row: round(row['Orders'] / row['Interactions'] * 100, 2) if row['Interactions'] > 0 else 0, axis=1
    )
    grouped_df['Conversion'] = grouped_df['Conversion'].astype(str) + '%'
    grouped_df['AOV'] = grouped_df.apply(
        lambda row: round(row['Gross_Revenue'] / row['Orders'], 2) if row['Orders'] > 0 else 0, axis=1
    )
    
    return grouped_df

# Function to calculate grand totals for all teams
def add_team_us_totals(df):
    total_orders = df['Orders'].sum()
    total_gross_revenue = df['Gross_Revenue'].sum()
    total_interactions = df['Interactions'].sum()
    
    totals_df = pd.DataFrame({
        'Team_No':['Total'], 
        'Team_Name': [''],
        'Supervisor_Name': [''],
        'Orders': [total_orders],
        'Gross_Revenue': [total_gross_revenue],
        'Interactions': [total_interactions],
        'Conversion': [round(total_orders / total_interactions * 100, 2) if total_interactions > 0 else 0],
        'AOV': [round(total_gross_revenue / total_orders, 2) if total_orders > 0 else 0]
    })
    
    totals_df['Conversion'] = totals_df['Conversion'].astype(str) + '%'
    
    return totals_df

# COMMAND ----------
# DBTITLE 1,Monthly
# Fetch monthly agent-level data (MTD)
MTD_Agents_df = spark.sql("""
    SELECT Agent_No, Agent_Name, Team_No, Team_Name, Conversion, Orders, Gross_Revenue, Interactions, AOV
    FROM main.reports.hourly.cc_report_current_month_hourly
    ORDER BY Team_Name, Gross_Revenue DESC
""").toPandas()

# Fetch monthly team-level leaderboard (MTD)
MTD_leaderboard_df = spark.sql("""
    SELECT 
        Team_No, Team_Name, Supervisor_Name, 
        CONCAT(ROUND(IFNULL(SUM(Orders) / SUM(Interactions), 0) * 100, 2), '%') AS Conversion, 
        SUM(Orders) AS Orders, 
        IFNULL(ROUND(SUM(Gross_Revenue), 2), 0) AS Gross_Revenue, 
        SUM(Interactions) AS Interactions, 
        ROUND(IFNULL(TRY_DIVIDE(SUM(Gross_Revenue), SUM(Orders)), 0), 2) AS AOV 
    FROM main.reports.hourly.cc_report_current_month_hourly
    GROUP BY Team_No, Team_Name, Supervisor_Name
    ORDER BY Gross_Revenue DESC
""").toPandas()

# Function to filter monthly dataframe by Team_No
def MTD_filter_df(MTD_df, team_no):
    if 'Team_No' in MTD_df.columns:
        return MTD_df[MTD_df['Team_No'] == team_no]
    return MTD_df

# Function to calculate team totals for MTD
def MTD_add_team_totals(MTD_df):
    MTD_grouped_df = MTD_df.groupby('Team_No').agg({
        'Orders': 'sum',
        'Gross_Revenue': 'sum',
        'Interactions': 'sum'
    }).reset_index()

    MTD_grouped_df['Agent_No'] = 'Total'
    MTD_grouped_df['Agent_Name'] = ''
    MTD_grouped_df['Team_Name'] = ''
    MTD_grouped_df['Team_No'] = ''
    MTD_grouped_df['Conversion'] = MTD_grouped_df.apply(
        lambda row: round(row['Orders'] / row['Interactions'] * 100, 2) if row['Interactions'] > 0 else 0, axis=1
    )
    MTD_grouped_df['Conversion'] = MTD_grouped_df['Conversion'].astype(str) + '%'
    MTD_grouped_df['AOV'] = MTD_grouped_df.apply(
        lambda row: round(row['Gross_Revenue'] / row['Orders'], 2) if row['Orders'] > 0 else 0, axis=1
    )
    
    return MTD_grouped_df

# Function to calculate grand totals for MTD
def add_MTD_team_us_totals(MTD_df):
    MTD_total_orders = MTD_df['Orders'].sum()
    MTD_total_gross_revenue = MTD_df['Gross_Revenue'].sum()
    MTD_total_interactions = MTD_df['Interactions'].sum()
    
    MTD_totals_df = pd.DataFrame({
        'Team_No':['Total'], 
        'Team_Name': [''],
        'Supervisor_Name': [''],
        'Orders': [MTD_total_orders],
        'Gross_Revenue': [MTD_total_gross_revenue],
        'Interactions': [MTD_total_interactions],
        'Conversion': [round(MTD_total_orders / MTD_total_interactions * 100, 2) if MTD_total_interactions > 0 else 0],
        'AOV': [round(MTD_total_gross_revenue / MTD_total_orders, 2) if MTD_total_orders > 0 else 0]
    })
    
    MTD_totals_df['Conversion'] = MTD_totals_df['Conversion'].astype(str) + '%'
    
    return MTD_totals_df

# COMMAND ----------
# DBTITLE 1,Yearly
# Fetch yearly agent-level data (YTD)
YTD_Agents_df = spark.sql("""
    SELECT Agent_No, Agent_Name, Team_No, Team_Name,
        CONCAT(ROUND(IFNULL(SUM(Orders) / SUM(Interactions), 0) * 100, 2), '%') AS Conversion, 
        SUM(Orders) AS Orders, 
        IFNULL(ROUND(SUM(Gross_Revenue), 2), 0) AS Gross_Revenue, 
        SUM(Interactions) AS Interactions, 
        ROUND(IFNULL(TRY_DIVIDE(SUM(Gross_Revenue), SUM(Orders)), 0), 2) AS AOV 
    FROM main.reports.hourly.cc_report_current_Year_hourly
    GROUP BY Team_No, Team_Name, Agent_No, Agent_Name
    ORDER BY Team_Name, Gross_Revenue DESC
""").toPandas()

# Fetch yearly team-level leaderboard (YTD)
YTD_leaderboard_df = spark.sql("""
    SELECT 
        Team_No, Team_Name, Supervisor_Name, 
        CONCAT(ROUND(IFNULL(SUM(Orders) / SUM(Interactions), 0) * 100, 2), '%') AS Conversion, 
        SUM(Orders) AS Orders, 
        IFNULL(ROUND(SUM(Gross_Revenue), 2), 0) AS Gross_Revenue, 
        SUM(Interactions) AS Interactions, 
        ROUND(IFNULL(TRY_DIVIDE(SUM(Gross_Revenue), SUM(Orders)), 0), 2) AS AOV 
    FROM main.reports.hourly.cc_report_current_Year_hourly
    GROUP BY Team_No, Team_Name, Supervisor_Name
    ORDER BY Gross_Revenue DESC
""").toPandas()

# Function to filter yearly dataframe by Team_No
def YTD_filter_df(YTD_df, team_no):
    if 'Team_No' in YTD_df.columns:
        return YTD_df[YTD_df['Team_No'] == team_no]
    return YTD_df

# Function to calculate team totals for YTD
def YTD_add_team_totals(YTD_df):
    YTD_grouped_df = YTD_df.groupby('Team_No').agg({
        'Orders': 'sum',
        'Gross_Revenue': 'sum',
        'Interactions': 'sum'
    }).reset_index()

    YTD_grouped_df['Agent_No'] = 'Total'
    YTD_grouped_df['Agent_Name'] = ''
    YTD_grouped_df['Team_Name'] = ''
    YTD_grouped_df['Team_No'] = ''
    YTD_grouped_df['Conversion'] = YTD_grouped_df.apply(
        lambda row: round(row['Orders'] / row['Interactions'] * 100, 2) if row['Interactions'] > 0 else 0, axis=1
    )
    YTD_grouped_df['Conversion'] = YTD_grouped_df['Conversion'].astype(str) + '%'
    YTD_grouped_df['AOV'] = YTD_grouped_df.apply(
        lambda row: round(row['Gross_Revenue'] / row['Orders'], 2) if row['Orders'] > 0 else 0, axis=1
    )
    
    return YTD_grouped_df

# Function to calculate grand totals for YTD
def add_YTD_team_us_totals(df):
    YTD_total_orders = df['Orders'].sum()
    YTD_total_gross_revenue = df['Gross_Revenue'].sum()
    YTD_total_interactions = df['Interactions'].sum()
    
    YTD_totals_df = pd.DataFrame({
        'Team_No':['Total'], 
        'Team_Name': [''],
        'Supervisor_Name': [''],
        'Orders': [YTD_total_orders],
        'Gross_Revenue': [YTD_total_gross_revenue],
        'Interactions': [YTD_total_interactions],
        'Conversion': [round(YTD_total_orders / YTD_total_interactions * 100, 2) if YTD_total_interactions > 0 else 0],
        'AOV': [round(YTD_total_gross_revenue / YTD_total_orders, 2) if YTD_total_orders > 0 else 0]
    })
    
    YTD_totals_df['Conversion'] = YTD_totals_df['Conversion'].astype(str) + '%'
    
    return YTD_totals_df

# COMMAND ----------
# DBTITLE 1,Secrets
# Retrieve OAuth2.0 client credentials from Databricks secret scope
client_id = dbutils.secrets.get(scope="email", key="client_id")
client_secret = dbutils.secrets.get(scope="email", key="client_secret")
tenant_id = dbutils.secrets.get(scope="email", key="tenant_id")
shared_mailbox_user_id = dbutils.secrets.get(scope="email", key="mailbox")

# Function to get access token using client credentials grant
def get_access_token(client_id, client_secret, tenant_id):
    url = f'https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token'
    data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
        'scope': 'https://graph.microsoft.com/.default',
    }

    response = requests.post(url, data=data)
    response.raise_for_status()
    token = response.json().get('access_token')
    return token

# Get the access token to authorize Microsoft Graph API requests
token = get_access_token(client_id, client_secret, tenant_id)

# Define the email sending endpoint using the shared mailbox
send_email_url = f'https://graph.microsoft.com/v1.0/users/{shared_mailbox_user_id}/sendMail'

# COMMAND ----------
# DBTITLE 1,Load Emails from DB
emails_df = spark.sql("""
    SELECT Team_No, email, Type
    FROM main.cc.teams.data.emails
    WHERE Activ='Y'
""").toPandas()

def get_recipients(team_no=None):
    """Returns list of dicts for toRecipients and ccRecipients"""
    df = emails_df.copy()
    if team_no:
        df = df[df['Team_No'] == team_no]
    to_list = df[df['Type'].str.lower()=='to']['email'].tolist()
    cc_list = df[df['Type'].str.lower()=='cc']['email'].tolist()
    return [{'emailAddress': {'address': e}} for e in to_list], [{'emailAddress': {'address': e}}


# COMMAND ----------
# DBTITLE 1,Secrets / Token
client_id = dbutils.secrets.get(scope="email", key="client_id")
client_secret = dbutils.secrets.get(scope="email", key="client_secret")
tenant_id = dbutils.secrets.get(scope="email", key="tenant_id")
shared_mailbox_user_id = dbutils.secrets.get(scope="email", key="mailbox")

def get_access_token(client_id, client_secret, tenant_id):
    url = f'https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token'
    data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
        'scope': 'https://graph.microsoft.com/.default',
    }
    response = requests.post(url, data=data)
    response.raise_for_status()
    return response.json().get('access_token')

token = get_access_token(client_id, client_secret, tenant_id)
send_email_url = f'https://graph.microsoft.com/v1.0/users/{shared_mailbox_user_id}/sendMail'

# COMMAND ----------
# DBTITLE 1,Timestamp for email
utc_minus_5 = pytz.timezone('Etc/GMT+4')
now = datetime.now(utc_minus_5)
rounded_time = now.replace(minute=0, second=0, microsecond=0)
timestamp = rounded_time.strftime("%m-%d-%Y %H:%M:%S")

# COMMAND ----------
# DBTITLE 1,HTML Style
html_style = """
<style>
   table {
        width: 100%;
        border-collapse: collapse;
    }
    th, td {
        border: 1px solid black;
        padding: 8px;
        text-align: center;
    }
    th {
        background-color: #f2f2f2;
    }
</style>
"""

# COMMAND ----------
# DBTITLE 1,Create Excel attachment
output = io.BytesIO()
with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
    # Тут вставити свої таблиці для US/EU/UA відповідно
    US_sales_lb_df.to_excel(writer, sheet_name='Leaderboard', index=False)
output.seek(0)
base64_encoded_content = base64.b64encode(output.getvalue()).decode('utf-8')

# -------------------
# DBTITLE 1,Prepare and send emails for all teams

def send_email(team_no, subject_text, html_tables):
    to_recipients, cc_recipients = get_recipients(team_no)
    subject = f"{subject_text} - {timestamp}"
    body = f"""
    <html>
    <head></head>
    <body>
    <p>Hello Team,</p>
    {html_style}
    {html_tables}
    <p>For any questions, please reach out to <a href="mailto:example0@mail.com">example0@mail.com</a>.</p>
    <p>Thank you!</p>
    </body>
    </html>
    """
    email_message = {
        'message': {
            'subject': subject,
            'body': {'contentType': 'html', 'content': body},
            'toRecipients': to_recipients,
            'ccRecipients': cc_recipients
        }
    }
    headers = {'Authorization': f'Bearer {token}','Content-Type': 'application/json'}
    response = requests.post(send_email_url, headers=headers, json=email_message)
    response.raise_for_status()
    print(f'Email sent successfully to {team_no}!')

# -------------------
# DBTITLE 1,Send US Sales Email
US_html_tables = f"""
<p> Current day Leaderboard:
</p>{html_table_2}
<p> Current day by Teams:
</p>{html_table_1}
<p> Month Through Yesterday Leaderboard:
</p>{html_table_4}
<p> Month Through Yesterday by Teams:
</p>{html_table_3}
<p> Year Through Yesterday Leaderboard:
</p>{html_table_6}
<p> Year Through Yesterday by Teams:
</p>{html_table_5}
<p>For any questions, please reach out to <a href="mailto:example0@mail.com">example0@mail.com</a>.</p>
<p>Thank you!</p>
</body>
</html>
"""
send_email('US', "Hourly Report (US Sales)", US_html_tables)

# -------------------
# DBTITLE 1,Send EU Sales Email
EU_html_tables = f"""
<p> Current day Leaderboard:
</p>{html_table_43}
<p> Current day by Teams:
</p>{html_table_10}
<p> Month Through Yesterday Leaderboard:
</p>{html_table_44}
<p> Month Through Yesterday by Teams:
</p>{html_table_11}
<p> Year Through Yesterday Leaderboard:
</p>{html_table_45}
<p> Year Through Yesterday by Teams:
</p>{html_table_12}
<p>For any questions, please reach out to <a href="mailto:example0@mail.com">example0@mail.com</a>.</p>
<p>Thank you!</p>
</body>
</html>
"""
send_email('EU', "Hourly Report (EU Sales)", EU_html_tables)

# -------------------
# DBTITLE 1,Send UA Sales Email
UA_html_tables = f"""
<p> Current day Leaderboard:
</p>{html_table_14}
<p> Current day by Team:
</p>{html_table_13}
<p> Month Through Yesterday Leaderboard:
</p>{html_table_16}
<p> Month Through Yesterday by Team:
</p>{html_table_15}
<p> Year Through Yesterday Leaderboard:
</p>{html_table_18}
<p> Year Through Yesterday by Team:
</p>{html_table_17}
<p>For any questions, please reach out to <a href="mailto:example0@mail.com">example0@mail.com</a>.</p>
<p>Thank you!</p>
</body>
</html>
"""
send_email('UA', "Hourly Report (UA Sales)", UA_html_tables)

print('Emails sent successfully!')