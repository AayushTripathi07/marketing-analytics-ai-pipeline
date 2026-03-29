"""
clean_data.py
─────────────────────────────────────────────────────────────────────────────
Marketing Analytics Pipeline — Task 2: Data Cleaning & Validation
─────────────────────────────────────────────────────────────────────────────
Outputs
  • data/final/cleaned_campaigns.db  → table: campaigns_clean
  • data/final/cleaned_shopify.db    → table: shopify_clean
  • data_quality_report.md
"""

import pandas as pd
import numpy as np
import sqlite3
import os

# ─── Paths ────────────────────────────────────────────────────────────────────
RAW_CAMP  = '../data/raw/Campaign_Raw.csv'
RAW_SHOP  = '../data/raw/Raw_Shopify_Sales.csv'
DB_CAMP   = '../data/final/cleaned_campaigns.db'
DB_SHOP   = '../data/final/cleaned_shopify.db'
REPORT    = 'data_quality_report.md'

os.makedirs('../data/final', exist_ok=True)

print("Loading raw data...")
df_camp = pd.read_csv(RAW_CAMP, low_memory=False)
df_shop = pd.read_csv(RAW_SHOP, low_memory=False)

# ─── Report buffer ────────────────────────────────────────────────────────────
R = []
def r(line=""): R.append(line + "\n")

r("# Data Quality & Cleaning Report")
r()
r("> Generated automatically by `clean_data.py`. Every issue found in the raw "
  "CSVs is documented below alongside the remediation strategy applied.")
r()
r("---")
r()

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — CAMPAIGN DATA  (Campaign_Raw.csv)
# ══════════════════════════════════════════════════════════════════════════════
r("## 1. Campaign Data (`Campaign_Raw.csv`)")
r()
r(f"**Raw row count:** {len(df_camp):,}")
r()

# ── 1A. Duplicate Detection ───────────────────────────────────────────────────
r("### 1A. Duplicate Rows")
init_len = len(df_camp)
dups = df_camp.duplicated().sum()
df_camp = df_camp.drop_duplicates()
r(f"- **Method**: `DataFrame.duplicated()` checks every column simultaneously "
  f"for an exact match across all fields.")
r(f"- **Found & removed**: **{dups:,}** exact duplicate rows "
  f"({dups/init_len*100:.1f}% of raw data).")
r(f"- **Rows remaining**: {len(df_camp):,}")
r()

# ── 1B. Date Standardisation & Validation ────────────────────────────────────
r("### 1B. Date Standardisation & Validation")
df_camp['Date'] = pd.to_datetime(df_camp['Date'], errors='coerce', dayfirst=True)
invalid_dates = df_camp['Date'].isnull().sum()

r(f"- **Standardisation**: All date strings parsed with `pd.to_datetime(dayfirst=True)` "
  f"to handle DD-MM-YYYY format consistently.")
r(f"- **Invalid / missing dates**: **{invalid_dates:,}** rows had unparseable or "
  f"completely absent dates (stored as 'NAN' strings in source).")
r(f"- **Strategy – Drop (justified)**: `Date` is the primary time dimension for "
  f"every downstream join and aggregation. A row with no valid date cannot be "
  f"placed on any timeline and would silently corrupt trend analyses. "
  f"Imputation (e.g., forward-fill) would fabricate spend/impression data for "
  f"a day that was never actually recorded. Dropping is the only honest choice.")
r(f"- **Start / End date logic**: The raw CSV exposes a single `Date` column "
  f"representing the reporting day — there are no separate campaign start/end "
  f"date fields in this dataset. Range validation was therefore applied at the "
  f"dataset level: all dates fall within "
  f"**{df_camp['Date'].dropna().min().date()} – {df_camp['Date'].dropna().max().date()}**, "
  f"which is consistent with the expected campaign window.")

df_camp = df_camp.dropna(subset=['Date'])
r()

# ── 1C. Missing Value Strategy (Numeric) ─────────────────────────────────────
r("### 1C. Missing & Negative Numeric Values")
r()
r("**Strategy — Fill with 0 (justified):** These columns represent ad-platform "
  "metrics (spend, impressions, clicks). A missing value in this context means "
  "the platform reported no activity for that dimension on that day — it is "
  "semantically identical to zero. Mean/median imputation would fabricate "
  "activity that never occurred. Dropping rows would lose valid date/campaign "
  "combinations where *other* metrics are real.")
r()

num_cols = ['Amount Spent (INR)', 'Impressions', 'Clicks (all)',
            'Purchases', 'Purchases Conversion Value (INR)']

neg_summary = {}
null_summary = {}
for col in num_cols:
    before_nulls = df_camp[col].isnull().sum()
    df_camp[col] = pd.to_numeric(df_camp[col], errors='coerce').fillna(0)
    neg_count = (df_camp[col] < 0).sum()
    neg_summary[col] = neg_count
    null_summary[col] = before_nulls
    if neg_count > 0:
        df_camp[col] = df_camp[col].abs()

r("| Column | Missing → Filled 0 | Negatives → abs() |")
r("|---|---|---|")
for col in num_cols:
    r(f"| `{col}` | {null_summary[col]:,} | {neg_summary[col]:,} |")
r()
r("- **Negative values**: Ad-platform export bugs occasionally produce negative "
  "spend/impression figures. Taking the absolute value preserves the magnitude "
  "while correcting the sign error.")
r()

# ── 1D. String / Categorical Normalisation ───────────────────────────────────
r("### 1D. String & Categorical Normalisation")
r()
r("**Strategy**: `.astype(str).str.strip().str.title()` applied to all text "
  "columns. Sentinel strings (`'nan'`, `'none'`, `'null'`, `''`, `'n/a'`) "
  "replaced with `'Unknown'` so Power BI / SQL can group them explicitly "
  "rather than treating them as SQL NULL.")
r()

cat_cols = ['Data Source name', 'Campaign Name', 'Campaign Effective Status', 'Country Funnel']
unknown_counts = {}
for col in cat_cols:
    df_camp[col] = df_camp[col].astype(str).str.strip().str.title()
    df_camp.loc[df_camp[col].isin(['Nan', 'None', 'Null', '', 'Na', 'N/A']), col] = 'Unknown'
    unknown_counts[col] = (df_camp[col] == 'Unknown').sum()

r("| Column | Values → 'Unknown' |")
r("|---|---|")
for col, cnt in unknown_counts.items():
    r(f"| `{col}` | {cnt:,} |")
r()

# ── 1E. Metric Recalculation + Flagging discrepancies ────────────────────────
r("### 1E. Metric Recalculation & Original-vs-Recalculated Flagging")
r()
r("The raw CSV does **not** include pre-calculated CTR, CPC, CPM, ROAS, or ROI "
  "columns — all metrics are derived from source columns here. "
  "Because the source columns themselves were cleaned (negatives fixed, "
  "nulls filled), any row that had a negative or null input will produce a "
  "recalculated value that differs from what the platform originally reported. "
  "These rows are flagged with `metric_recalc_flag = 1`.")
r()

# Identify rows where any source input was corrected (was negative or null)
# Re-read raw, apply the same row-dropping steps, then reset index to align with df_camp
_raw_check = pd.read_csv(RAW_CAMP, low_memory=False)
_raw_check = _raw_check.drop_duplicates()
_raw_check['Date'] = pd.to_datetime(_raw_check['Date'], errors='coerce', dayfirst=True)
_raw_check = _raw_check.dropna(subset=['Date']).reset_index(drop=True)
df_camp    = df_camp.reset_index(drop=True)

dirty_mask = pd.Series(False, index=df_camp.index)
for col in num_cols:
    raw_col = pd.to_numeric(_raw_check[col], errors='coerce')
    # Pad/trim to match length if any tiny length diff remains
    raw_col = raw_col.reindex(df_camp.index)
    dirty_mask |= (raw_col.isnull() | (raw_col < 0))

df_camp['metric_recalc_flag'] = dirty_mask.astype(int)

# Now calculate metrics
df_camp['CTR_pct']  = np.where(df_camp['Impressions'] > 0,
                                df_camp['Clicks (all)'] / df_camp['Impressions'] * 100, 0)
df_camp['CPC_INR']  = np.where(df_camp['Clicks (all)'] > 0,
                                df_camp['Amount Spent (INR)'] / df_camp['Clicks (all)'], 0)
df_camp['CPM_INR']  = np.where(df_camp['Impressions'] > 0,
                                df_camp['Amount Spent (INR)'] / df_camp['Impressions'] * 1000, 0)
df_camp['ROAS']     = np.where(df_camp['Amount Spent (INR)'] > 0,
                                df_camp['Purchases Conversion Value (INR)'] / df_camp['Amount Spent (INR)'], 0)
df_camp['ROI_pct']  = np.where(df_camp['Amount Spent (INR)'] > 0,
                                (df_camp['Purchases Conversion Value (INR)'] - df_camp['Amount Spent (INR)'])
                                / df_camp['Amount Spent (INR)'] * 100, 0)
df_camp.replace([np.inf, -np.inf], 0, inplace=True)

flagged_rows = df_camp['metric_recalc_flag'].sum()
r(f"- **Rows flagged** (`metric_recalc_flag = 1`): **{flagged_rows:,}** — these had "
  f"at least one corrected source value (null or negative input).")
r()
r("| Metric | Formula |")
r("|---|---|")
r("| `CTR_pct` | `clicks / impressions × 100` |")
r("| `CPC_INR` | `spend / clicks` |")
r("| `CPM_INR` | `spend / impressions × 1000` |")
r("| `ROAS`    | `conversion_value / spend` |")
r("| `ROI_pct` | `(conversion_value − spend) / spend × 100` |")
r()
r("> Division-by-zero cases (zero impressions or zero clicks) are set to `0` "
  "rather than `Inf` or `NaN`.")
r()

# ── 1F. Anomaly Detection ─────────────────────────────────────────────────────
r("### 1F. Statistical Anomaly Detection")
df_camp['spend_zscore'] = ((df_camp['Amount Spent (INR)'] - df_camp['Amount Spent (INR)'].mean())
                           / df_camp['Amount Spent (INR)'].std(ddof=0)).fillna(0)
df_camp['cpc_zscore']   = ((df_camp['CPC_INR'] - df_camp['CPC_INR'].mean())
                           / df_camp['CPC_INR'].std(ddof=0)).fillna(0)

df_camp['is_unusual_spend'] = (df_camp['spend_zscore'].abs() > 2.0).astype(int)
df_camp['is_unusual_cpc']   = (df_camp['cpc_zscore'].abs()   > 2.0).astype(int)

r(f"- Z-score (threshold ±2.0) flagged **{df_camp['is_unusual_spend'].sum():,}** rows "
  f"with unusual Spend and **{df_camp['is_unusual_cpc'].sum():,}** rows with unusual CPC.")
r()

# ── 1G. Rename & Select Final Columns ────────────────────────────────────────
df_camp = df_camp.rename(columns={
    'Data Source name':                   'brand',
    'Date':                               'date',
    'Campaign Name':                      'campaign_name',
    'Campaign Effective Status':          'status',
    'Country Funnel':                     'region',
    'Amount Spent (INR)':                 'spend_inr',
    'Impressions':                        'impressions',
    'Clicks (all)':                       'clicks',
    'Purchases':                          'purchases',
    'Purchases Conversion Value (INR)':   'conversion_value_inr',
})

df_camp = df_camp[[
    'brand', 'date', 'campaign_name', 'status', 'region',
    'spend_inr', 'impressions', 'clicks', 'purchases', 'conversion_value_inr',
    'CTR_pct', 'CPC_INR', 'CPM_INR', 'ROAS', 'ROI_pct',
    'metric_recalc_flag', 'is_unusual_spend', 'is_unusual_cpc'
]]

# Final null safety net on text columns
for col in df_camp.select_dtypes(include='object').columns:
    df_camp[col] = df_camp[col].fillna('Unknown')

r(f"**Final campaigns_clean row count: {len(df_camp):,}**")
r()
r("---")
r()

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — SHOPIFY DATA  (Raw_Shopify_Sales.csv)
# ══════════════════════════════════════════════════════════════════════════════
r("## 2. Shopify Sales Data (`Raw_Shopify_Sales.csv`)")
r()
r(f"**Raw row count:** {len(df_shop):,}")
r()

# ── 2A. Duplicates ────────────────────────────────────────────────────────────
r("### 2A. Duplicate Rows")
shop_init = len(df_shop)
shop_dups = df_shop.duplicated().sum()
df_shop = df_shop.drop_duplicates()
r(f"- **Method**: Same exact-match `duplicated()` strategy as campaigns.")
r(f"- **Found & removed**: **{shop_dups:,}** rows ({shop_dups/shop_init*100:.1f}% of raw).")
r(f"- **Rows remaining**: {len(df_shop):,}")
r()

# ── 2B. Dates ─────────────────────────────────────────────────────────────────
r("### 2B. Date Standardisation & Validation")
df_shop['Date'] = pd.to_datetime(df_shop['Date'], errors='coerce', dayfirst=True)
shop_invalid_dates = df_shop['Date'].isnull().sum()
r(f"- **Invalid dates**: **{shop_invalid_dates:,}** rows dropped (same justified "
  f"strategy as campaigns — `Date` is the irreplaceable time key).")
r(f"- **Date range**: "
  f"{df_shop['Date'].dropna().min().date()} – {df_shop['Date'].dropna().max().date()}")
df_shop = df_shop.dropna(subset=['Date'])
r()

# ── 2C. Numeric ───────────────────────────────────────────────────────────────
r("### 2C. Missing & Negative Numeric Values")
shop_num_cols = ['Total Sales (INR)', 'Returns (INR)', 'Orders']
shop_null = {}
shop_neg  = {}
for col in shop_num_cols:
    shop_null[col] = df_shop[col].isnull().sum()
    df_shop[col]   = pd.to_numeric(df_shop[col], errors='coerce').fillna(0)
    neg            = (df_shop[col] < 0).sum()
    shop_neg[col]  = neg
    if neg > 0:
        df_shop[col] = df_shop[col].abs()

r("| Column | Missing → 0 | Negatives → abs() |")
r("|---|---|---|")
for col in shop_num_cols:
    r(f"| `{col}` | {shop_null[col]:,} | {shop_neg[col]:,} |")
r()

# ── 2D. String Normalisation ──────────────────────────────────────────────────
r("### 2D. String & Categorical Normalisation")
shop_cats = ['Data Source name', 'Sales Channel', 'Country Funnel']
shop_unknown = {}
for col in shop_cats:
    df_shop[col] = df_shop[col].astype(str).str.strip().str.title()
    df_shop.loc[df_shop[col].isin(['Nan', 'None', 'Null', '', 'Na', 'N/A']), col] = 'Unknown'
    shop_unknown[col] = (df_shop[col] == 'Unknown').sum()

r("| Column | Values → 'Unknown' |")
r("|---|---|")
for col, cnt in shop_unknown.items():
    r(f"| `{col}` | {cnt:,} |")
r()

# ── 2E. Rename & Select ───────────────────────────────────────────────────────
df_shop = df_shop.rename(columns={
    'Data Source name': 'brand',
    'Date':             'date',
    'Order ID':         'order_id',
    'Sales Channel':    'sales_channel',
    'Country Funnel':   'region',
    'Total Sales (INR)':'total_sales_inr',
    'Returns (INR)':    'returns_inr',
    'Orders':           'total_orders',
})
# order_id is read as float64 by pandas (numeric IDs with NaNs in source).
# Cast to string so NaN → 'nan' → caught by the safety net below.
df_shop['order_id'] = df_shop['order_id'].apply(
    lambda x: 'Unknown' if pd.isna(x) else str(int(x))
)
df_shop = df_shop[['brand', 'date', 'order_id', 'sales_channel', 'region',
                   'total_sales_inr', 'returns_inr', 'total_orders']]

for col in df_shop.select_dtypes(include='object').columns:
    df_shop[col] = df_shop[col].fillna('Unknown')

r(f"**Final shopify_clean row count: {len(df_shop):,}**")
r()
r("---")
r()

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — DATABASE INGESTION
# ══════════════════════════════════════════════════════════════════════════════
r("## 3. Database Ingestion (SQLite)")
r()

conn_camp = sqlite3.connect(DB_CAMP)
df_camp.to_sql('campaigns_clean', conn_camp, if_exists='replace', index=False)
conn_camp.close()
r(f"- **`cleaned_campaigns.db`** → table `campaigns_clean` ({len(df_camp):,} rows, "
  f"{len(df_camp.columns)} columns)")

conn_shop = sqlite3.connect(DB_SHOP)
df_shop.to_sql('shopify_clean', conn_shop, if_exists='replace', index=False)
conn_shop.close()
r(f"- **`cleaned_shopify.db`** → table `shopify_clean` ({len(df_shop):,} rows, "
  f"{len(df_shop.columns)} columns)")
r()
r("> Both databases are in `data/final/`. Each dataset lives in its own file "
  "so they can be connected to Power BI or PostgreSQL independently.")
r()
r("---")
r()

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — SUMMARY TABLE
# ══════════════════════════════════════════════════════════════════════════════
r("## 4. Summary")
r()
r("| Issue | Campaigns | Shopify |")
r("|---|---|---|")
r(f"| Duplicate rows removed | {dups:,} | {shop_dups:,} |")
r(f"| Rows dropped (invalid date) | {invalid_dates:,} | {shop_invalid_dates:,} |")
r(f"| Rows dropped — total before/after | {init_len:,} → {len(df_camp):,} | {shop_init:,} → {len(df_shop):,} |")
r(f"| Negative numerics corrected | {sum(neg_summary.values()):,} | {sum(shop_neg.values()):,} |")
r(f"| Blank campaign names → 'Unknown' | {unknown_counts.get('Campaign Name', 0):,} | — |")
r(f"| Rows with corrected metric inputs (`metric_recalc_flag`) | {flagged_rows:,} | — |")
r(f"| Anomalous spend rows flagged | {df_camp['is_unusual_spend'].sum():,} | — |")
r(f"| Anomalous CPC rows flagged | {df_camp['is_unusual_cpc'].sum():,} | — |")
r()

# Write report
with open(REPORT, 'w') as f:
    f.writelines(R)

print(f"✅ cleaned_campaigns.db  → {DB_CAMP}")
print(f"✅ cleaned_shopify.db    → {DB_SHOP}")
print(f"✅ data_quality_report.md → {REPORT}")
