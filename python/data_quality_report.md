# Data Quality & Cleaning Report

This report outlines the structural issues found in the raw datasets and the Python processing strategies employed to rectify them prior to SQL ingestion.
## 1. Platform Campaigns Data (`Campaign_Raw.csv`)
- **Duplicates**: Discovered and removed **310** exact duplicate rows.
- **Dates**: Identified **1054** rows with completely missing or malformed Date values (e.g., 'NAN'). These rows were dropped since `Date` is our primary dimension for the Star Schema.
- **Numeric Anomalies**: 
  - `Amount Spent (INR)`: Corrected **377** negative values by taking their absolute value.
  - `Impressions`: Corrected **390** negative values by taking their absolute value.
  - `Clicks (all)`: Corrected **332** negative values by taking their absolute value.
  - `Purchases`: Corrected **29** negative values by taking their absolute value.
  - `Purchases Conversion Value (INR)`: Corrected **34** negative values by taking their absolute value.
- **Metric Recalculation**: Ignored raw CTR/CPC fields mapped natively in CSV due to discrepancies. Programmatically recalculated `CTR`, `CPC`, `CPM`, and `ROAS`. Replaced Inf values representing Division-by-Zero with 0.
- **Anomaly Detection**: Flagged 224 campaigns with unusual Spend and 227 campaigns with unusual CPC based on Z-Score > 2.0.

## 2. Shopify Sales Data (`Raw_Shopify_Sales.csv`)
- **Duplicates**: Discovered and removed **21** exact duplicate rows.
- **Dates**: Dropped **728** rows with invalid dates.
- **Numeric Anomalies**: 
  - `Total Sales (INR)`: Corrected **200** negative values.
  - `Returns (INR)`: Corrected **4** negative values.
  - `Orders`: Corrected **56** negative values.

## 3. SQLite Final Ingestion
- Data successfully committed to `../data/final/analytics.db` under tables `campaigns_clean` and `shopify_clean`.
