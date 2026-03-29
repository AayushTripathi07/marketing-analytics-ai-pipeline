import pandas as pd
import numpy as np
import sqlite3
import os

# Define relative paths
raw_camp_path = '../data/raw/Campaign_Raw.csv'
raw_shop_path = '../data/raw/Raw_Shopify_Sales.csv'
db_path = '../data/final/analytics.db'
report_path = 'data_quality_report.md'

print("Loading raw data...")
df_camp = pd.read_csv(raw_camp_path, low_memory=False)
df_shop = pd.read_csv(raw_shop_path, low_memory=False)

report_lines = ["# Data Quality & Cleaning Report\n\nThis report outlines the structural issues found in the raw datasets and the Python processing strategies employed to rectify them prior to SQL ingestion.\n"]

# ==========================================
# 1. CAMPAIGNS DATA CLEANING
# ==========================================
report_lines.append("## 1. Platform Campaigns Data (`Campaign_Raw.csv`)\n")

# A. Handle Duplicates
init_len = len(df_camp)
dups = df_camp.duplicated().sum()
df_camp = df_camp.drop_duplicates()
report_lines.append(f"- **Duplicates**: Discovered and removed **{dups}** exact duplicate rows.\n")

# B. Date Normalization
# Convert strings to datetime. Coerce bad formats to NaT.
df_camp['Date'] = pd.to_datetime(df_camp['Date'], errors='coerce', dayfirst=True)
invalids_dates = df_camp['Date'].isnull().sum()
df_camp = df_camp.dropna(subset=['Date'])
report_lines.append(f"- **Dates**: Identified **{invalids_dates}** rows with completely missing or malformed Date values (e.g., 'NAN'). These rows were dropped since `Date` is our primary dimension for the Star Schema.\n")

# C. Handle Missing/Negative Numeric Values
num_cols = ['Amount Spent (INR)', 'Impressions', 'Clicks (all)', 'Purchases', 'Purchases Conversion Value (INR)']
report_lines.append("- **Numeric Anomalies**: \n")
for col in num_cols:
    df_camp[col] = pd.to_numeric(df_camp[col], errors='coerce').fillna(0)
    negatives = (df_camp[col] < 0).sum()
    if negatives > 0:
        report_lines.append(f"  - `{col}`: Corrected **{negatives}** negative values by taking their absolute value.\n")
        df_camp[col] = df_camp[col].abs()

# D. Standardize Categorical Strings
cat_cols = ['Data Source name', 'Campaign Effective Status', 'Country Funnel']
for col in cat_cols:
    df_camp[col] = df_camp[col].astype(str).str.strip().str.title()
    df_camp.loc[df_camp[col].isin(['Nan', 'None', 'Null']), col] = 'Unknown'

# E. Recalculate Metrics correctly
df_camp['CTR_Pct'] = (df_camp['Clicks (all)'] / df_camp['Impressions'] * 100).fillna(0)
df_camp['CPC_INR'] = (df_camp['Amount Spent (INR)'] / df_camp['Clicks (all)']).fillna(0)
df_camp['CPM_INR'] = (df_camp['Amount Spent (INR)'] / df_camp['Impressions'] * 1000).fillna(0)
df_camp['ROAS'] = (df_camp['Purchases Conversion Value (INR)'] / df_camp['Amount Spent (INR)']).fillna(0)
df_camp.replace([np.inf, -np.inf], 0, inplace=True)
report_lines.append("- **Metric Recalculation**: Ignored raw CTR/CPC fields mapped natively in CSV due to discrepancies. Programmatically recalculated `CTR`, `CPC`, `CPM`, and `ROAS`. Replaced Inf values representing Division-by-Zero with 0.\n")

# Clean column names to be SQL-friendly
df_camp = df_camp.rename(columns={
    'Data Source name': 'brand',
    'Date': 'date',
    'Campaign Name': 'campaign_name',
    'Campaign Effective Status': 'status',
    'Country Funnel': 'region',
    'Amount Spent (INR)': 'spend_inr',
    'Impressions': 'impressions',
    'Clicks (all)': 'clicks',
    'Purchases': 'purchases',
    'Purchases Conversion Value (INR)': 'conversion_value_inr'
})

# F. Anomaly Detection (Z-Score on Spend and CPC)
df_camp['spend_zscore'] = (df_camp['spend_inr'] - df_camp['spend_inr'].mean()) / df_camp['spend_inr'].std(ddof=0)
df_camp['cpc_zscore'] = (df_camp['CPC_INR'] - df_camp['CPC_INR'].mean()) / df_camp['CPC_INR'].std(ddof=0)

# Protect against constant columns producing NaN z-scores
df_camp['spend_zscore'] = df_camp['spend_zscore'].fillna(0)
df_camp['cpc_zscore'] = df_camp['cpc_zscore'].fillna(0)

df_camp['is_unusual_spend'] = (df_camp['spend_zscore'].abs() > 2.0).astype(int)
df_camp['is_unusual_cpc'] = (df_camp['cpc_zscore'].abs() > 2.0).astype(int)

report_lines.append(f"- **Anomaly Detection**: Flagged {df_camp['is_unusual_spend'].sum()} campaigns with unusual Spend and {df_camp['is_unusual_cpc'].sum()} campaigns with unusual CPC based on Z-Score > 2.0.\n")

# Select final columns
df_camp = df_camp[['brand', 'date', 'campaign_name', 'status', 'region', 'spend_inr', 
                   'impressions', 'clicks', 'purchases', 'conversion_value_inr', 
                   'CTR_Pct', 'CPC_INR', 'CPM_INR', 'ROAS', 'is_unusual_spend', 'is_unusual_cpc']]

# ==========================================
# 2. SHOPIFY DATA CLEANING
# ==========================================
report_lines.append("\n## 2. Shopify Sales Data (`Raw_Shopify_Sales.csv`)\n")

# A. Handle Duplicates
shop_dups = df_shop.duplicated().sum()
df_shop = df_shop.drop_duplicates()
report_lines.append(f"- **Duplicates**: Discovered and removed **{shop_dups}** exact duplicate rows.\n")

# B. Date
df_shop['Date'] = pd.to_datetime(df_shop['Date'], errors='coerce', dayfirst=True)
shop_invalid_dates = df_shop['Date'].isnull().sum()
df_shop = df_shop.dropna(subset=['Date'])
report_lines.append(f"- **Dates**: Dropped **{shop_invalid_dates}** rows with invalid dates.\n")

# C. Handle Numeric
shop_num_cols = ['Total Sales (INR)', 'Returns (INR)', 'Orders']
report_lines.append("- **Numeric Anomalies**: \n")
for col in shop_num_cols:
    df_shop[col] = pd.to_numeric(df_shop[col], errors='coerce').fillna(0)
    negatives = (df_shop[col] < 0).sum()
    if negatives > 0:
        report_lines.append(f"  - `{col}`: Corrected **{negatives}** negative values.\n")
        df_shop[col] = df_shop[col].abs()

# D. Handle Categorical
shop_cats = ['Data Source name', 'Sales Channel', 'Country Funnel']
for col in shop_cats:
    df_shop[col] = df_shop[col].astype(str).str.strip().str.title()
    df_shop.loc[df_shop[col].isin(['Nan', 'None', 'Null']), col] = 'Unknown'

df_shop = df_shop.rename(columns={
    'Data Source name': 'brand',
    'Date': 'date',
    'Order ID': 'order_id',
    'Sales Channel': 'sales_channel',
    'Country Funnel': 'region',
    'Total Sales (INR)': 'total_sales_inr',
    'Returns (INR)': 'returns_inr',
    'Orders': 'total_orders'
})

df_shop = df_shop[['brand', 'date', 'order_id', 'sales_channel', 'region', 'total_sales_inr', 'returns_inr', 'total_orders']]


# ==========================================
# 3. DATABASE INGESTION
# ==========================================
os.makedirs(os.path.dirname(db_path), exist_ok=True)
conn = sqlite3.connect(db_path)

df_camp.to_sql('campaigns_clean', conn, if_exists='replace', index=False)
df_shop.to_sql('shopify_clean', conn, if_exists='replace', index=False)

conn.close()
report_lines.append("\n## 3. SQLite Final Ingestion\n")
report_lines.append(f"- Data successfully committed to `{db_path}` under tables `campaigns_clean` and `shopify_clean`.\n")

with open(report_path, 'w') as f:
    f.writelines(report_lines)

print(f"Success! Final db written to {db_path} and report saved.")
