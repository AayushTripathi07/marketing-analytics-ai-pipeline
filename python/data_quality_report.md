# Data Quality & Cleaning Report

> Generated automatically by `clean_data.py`. Every issue found in the raw CSVs is documented below alongside the remediation strategy applied.

---

## 1. Campaign Data (`Campaign_Raw.csv`)

**Raw row count:** 10,028

### 1A. Duplicate Rows
- **Method**: `DataFrame.duplicated()` checks every column simultaneously for an exact match across all fields.
- **Found & removed**: **310** exact duplicate rows (3.1% of raw data).
- **Rows remaining**: 9,718

### 1B. Date Standardisation & Validation
- **Standardisation**: All date strings parsed with `pd.to_datetime(dayfirst=True)` to handle DD-MM-YYYY format consistently.
- **Invalid / missing dates**: **1,054** rows had unparseable or completely absent dates (stored as 'NAN' strings in source).
- **Strategy – Drop (justified)**: `Date` is the primary time dimension for every downstream join and aggregation. A row with no valid date cannot be placed on any timeline and would silently corrupt trend analyses. Imputation (e.g., forward-fill) would fabricate spend/impression data for a day that was never actually recorded. Dropping is the only honest choice.
- **Start / End date logic**: The raw CSV exposes a single `Date` column representing the reporting day — there are no separate campaign start/end date fields in this dataset. Range validation was therefore applied at the dataset level: all dates fall within **2026-01-01 – 2026-03-28**, which is consistent with the expected campaign window.

### 1C. Missing & Negative Numeric Values

**Strategy — Fill with 0 (justified):** These columns represent ad-platform metrics (spend, impressions, clicks). A missing value in this context means the platform reported no activity for that dimension on that day — it is semantically identical to zero. Mean/median imputation would fabricate activity that never occurred. Dropping rows would lose valid date/campaign combinations where *other* metrics are real.

| Column | Missing → Filled 0 | Negatives → abs() |
|---|---|---|
| `Amount Spent (INR)` | 572 | 377 |
| `Impressions` | 584 | 390 |
| `Clicks (all)` | 574 | 332 |
| `Purchases` | 587 | 29 |
| `Purchases Conversion Value (INR)` | 570 | 34 |

- **Negative values**: Ad-platform export bugs occasionally produce negative spend/impression figures. Taking the absolute value preserves the magnitude while correcting the sign error.

### 1D. String & Categorical Normalisation

**Strategy**: `.astype(str).str.strip().str.title()` applied to all text columns. Sentinel strings (`'nan'`, `'none'`, `'null'`, `''`, `'n/a'`) replaced with `'Unknown'` so Power BI / SQL can group them explicitly rather than treating them as SQL NULL.

| Column | Values → 'Unknown' |
|---|---|
| `Data Source name` | 578 |
| `Campaign Name` | 585 |
| `Campaign Effective Status` | 571 |
| `Country Funnel` | 707 |

### 1E. Metric Recalculation & Original-vs-Recalculated Flagging

The raw CSV does **not** include pre-calculated CTR, CPC, CPM, ROAS, or ROI columns — all metrics are derived from source columns here. Because the source columns themselves were cleaned (negatives fixed, nulls filled), any row that had a negative or null input will produce a recalculated value that differs from what the platform originally reported. These rows are flagged with `metric_recalc_flag = 1`.

- **Rows flagged** (`metric_recalc_flag = 1`): **3,350** — these had at least one corrected source value (null or negative input).

| Metric | Formula |
|---|---|
| `CTR_pct` | `clicks / impressions × 100` |
| `CPC_INR` | `spend / clicks` |
| `CPM_INR` | `spend / impressions × 1000` |
| `ROAS`    | `conversion_value / spend` |
| `ROI_pct` | `(conversion_value − spend) / spend × 100` |

> Division-by-zero cases (zero impressions or zero clicks) are set to `0` rather than `Inf` or `NaN`.

### 1F. Statistical Anomaly Detection
- Z-score (threshold ±2.0) flagged **224** rows with unusual Spend and **227** rows with unusual CPC.

**Final campaigns_clean row count: 8,664**

---

## 2. Shopify Sales Data (`Raw_Shopify_Sales.csv`)

**Raw row count:** 5,680

### 2A. Duplicate Rows
- **Method**: Same exact-match `duplicated()` strategy as campaigns.
- **Found & removed**: **21** rows (0.4% of raw).
- **Rows remaining**: 5,659

### 2B. Date Standardisation & Validation
- **Invalid dates**: **728** rows dropped (same justified strategy as campaigns — `Date` is the irreplaceable time key).
- **Date range**: 2026-01-01 – 2026-03-28

### 2C. Missing & Negative Numeric Values
| Column | Missing → 0 | Negatives → abs() |
|---|---|---|
| `Total Sales (INR)` | 393 | 200 |
| `Returns (INR)` | 406 | 4 |
| `Orders` | 406 | 56 |

### 2D. String & Categorical Normalisation
| Column | Values → 'Unknown' |
|---|---|
| `Data Source name` | 420 |
| `Sales Channel` | 415 |
| `Country Funnel` | 411 |

**Final shopify_clean row count: 4,931**

---

## 3. Database Ingestion (SQLite)

- **`cleaned_campaigns.db`** → table `campaigns_clean` (8,664 rows, 18 columns)
- **`cleaned_shopify.db`** → table `shopify_clean` (4,931 rows, 8 columns)

> Both databases are in `data/final/`. Each dataset lives in its own file so they can be connected to Power BI or PostgreSQL independently.

---

## 4. Summary

| Issue | Campaigns | Shopify |
|---|---|---|
| Duplicate rows removed | 310 | 21 |
| Rows dropped (invalid date) | 1,054 | 728 |
| Rows dropped — total before/after | 10,028 → 8,664 | 5,680 → 4,931 |
| Negative numerics corrected | 1,162 | 260 |
| Blank campaign names → 'Unknown' | 585 | — |
| Rows with corrected metric inputs (`metric_recalc_flag`) | 3,350 | — |
| Anomalous spend rows flagged | 224 | — |
| Anomalous CPC rows flagged | 227 | — |

