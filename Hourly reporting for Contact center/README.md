Sales Reports Automation



This repository contains a Python-based solution for generating and sending automated hourly, monthly, and yearly sales reports via email using Databricks, Pandas, and Microsoft Graph API. The solution supports multiple regions (US, EU, UA) and provides both agent-level and team-level leaderboards.



Features



Hourly, Monthly (MTD), and Yearly (YTD) reports



Agent-level and team-level summaries



Automatic calculation of:



Orders



Gross Revenue



Interactions



Conversion rate



Average Order Value (AOV)



Excel attachment generation using XlsxWriter



HTML email body with styled tables



Sending emails via Microsoft Graph API using a shared mailbox



Supports filtering by team



Grand totals and team totals included in reports



Repository Structure

.

├── README.md                  # This file

├── sales\_report.py            # Main script for generating and sending reports

├── requirements.txt           # Python dependencies

├── sql\_queries/               # Folder with SQL queries used in Databricks

│   ├── hourly.sql

│   ├── monthly\_mtd.sql

│   └── yearly\_ytd.sql

├── templates/                 # HTML templates for email formatting

│   └── email\_template.html

└── secrets\_config.md          # Instructions for storing secrets in Databricks



Prerequisites



Python 3.8+



Databricks environment with Spark



Microsoft 365 account with a shared mailbox



Access to Databricks Secret Scope for storing credentials



Python Dependencies

pip install pandas requests xlsxwriter



Setup



Store secrets in Databricks Secret Scope:



Secret Key	Description

client\_id	OAuth2 client ID

client\_secret	OAuth2 client secret

tenant\_id	Microsoft tenant ID

mailbox	Shared mailbox email address



Define SQL queries to fetch sales data:



Hourly: cc\_report\_current\_day\_hourly



Monthly (MTD): cc\_report\_current\_month\_hourly



Yearly (YTD): cc\_report\_current\_Year\_hourly



Update region-specific tables for email content:



US: US\_html\_tables



EU: EU\_html\_tables



UA: UA\_html\_tables



How It Works



Fetch data from Databricks SQL tables using Spark SQL.



Convert Spark DataFrames to Pandas for processing.



Calculate totals:



Agent totals



Team totals



Grand totals



Generate Excel attachments with Pandas and XlsxWriter.



Prepare HTML email with inline tables and styling.



Send emails via Microsoft Graph API to team members based on their region.



Example Usage

\# Send US Sales Email

send\_email('US', "Hourly Report (US Sales)", US\_html\_tables)



\# Send EU Sales Email

send\_email('EU', "Hourly Report (EU Sales)", EU\_html\_tables)



\# Send UA Sales Email

send\_email('UA', "Hourly Report (UA Sales)", UA\_html\_tables)



Email Format



Each email contains:



Current day leaderboard



Current day by teams



Month-to-date leaderboard



Month-to-date by teams



Year-to-date leaderboard



Year-to-date by teams



All tables are formatted with borders, centered text, and alternating headers for readability.



Notes



Timestamps in the email are in UTC-4 timezone, rounded to the hour.



Make sure all secret keys and email recipients are correctly configured.



The script uses a shared mailbox to send emails to multiple recipients with To and CC fields.





