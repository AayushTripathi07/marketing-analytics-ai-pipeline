# Power BI — DAX & Data Modeling Guide (Task 4)

> **Platform note:** Power BI Desktop is Windows-only. On macOS, use a Windows
> VM (Parallels, UTM) or a Windows machine. The Power BI web service cannot
> connect to a local SQLite file without a gateway — **you must use Desktop.**

---

## Step 1 — Connect to the SQL Database

1. Open **Power BI Desktop**.
2. **Home → Get Data → More → ODBC** (requires the free
   [SQLite ODBC driver](http://www.ch-werner.de/sqliteodbc/)) *or* use the
   community **SQLite** connector.
3. Point the connection at the warehouse database:
   ```
   .../marketing-analytics-ai-pipeline/data/final/analytics_warehouse.db
   ```
   > ⚠️ Do **not** connect to `analytics.db` or the raw CSVs. The warehouse
   > is the single source of truth for both Power BI and the AI tool.

4. In the Navigator, select these **tables** (not views — Power BI handles
   aggregation natively via DAX, which is faster than pre-aggregated views):
   - `dim_date`
   - `dim_campaign`
   - `dim_channel`
   - `fact_campaign_performance`
   - `fact_sales`
5. Click **Transform Data** (not Load) so you can fix types first.

---

## Step 2 — Power Query: Fix Date Column Types

SQLite stores all dates as TEXT (`YYYY-MM-DD`). Power BI's time-intelligence
DAX functions **require a proper Date data type** — TEXT will silently break
`DATEADD`, `PREVIOUSMONTH`, and Calendar visuals.

In Power Query Editor:

| Table | Column | Change type to |
|---|---|---|
| `dim_date` | `date_id` | **Date** |
| `fact_campaign_performance` | `date_id` | **Date** |
| `fact_sales` | `date_id` | **Date** |

> Select the column → **Transform → Data Type → Date.**
> Do this **before** clicking Close & Apply.

---

## Step 3 — Data Model (Model View)

Switch to **Model View** (relationship icon, left sidebar).

### Relationships to create

| From (many side) | To (one side) | Cardinality | Active |
|---|---|---|---|
| `fact_campaign_performance[date_id]` | `dim_date[date_id]` | Many → One | ✅ Yes |
| `fact_sales[date_id]` | `dim_date[date_id]` | Many → One | ✅ Yes |
| `fact_campaign_performance[campaign_id]` | `dim_campaign[campaign_id]` | Many → One | ✅ Yes |
| `fact_sales[channel_id]` | `dim_channel[channel_id]` | Many → One | ✅ Yes |

> ⚠️ The join column is `date_id` (not `date`). This is the TEXT primary key
> on `dim_date` that was converted to Date type in Step 2.

### Mark as Date Table

Right-click `dim_date` in the Fields pane → **Mark as date table** →
select `date_id` as the date column.  
This is **mandatory** for `MoM Spend Change %` and any time-intelligence measure.

### Clean the field list

Hide technical surrogate keys from report authors — they should filter using
dimensions, not raw IDs:

| Table | Columns to hide (right-click → Hide) |
|---|---|
| `fact_campaign_performance` | `date_id`, `campaign_id`, `perf_id` |
| `fact_sales` | `date_id`, `channel_id`, `sale_id` |
| `dim_campaign` | `campaign_id` |
| `dim_channel` | `channel_id` |

---

## Step 4 — DAX Measures

Create a dedicated measure table: **Home → Enter Data** → name it
`_Measures` → load an empty table. Right-click it to add each measure below.

Name measures exactly as shown — the field list is assessed alongside the DAX.

### Core KPI Measures

```dax
// ─── Spend ────────────────────────────────────────────────────────────────────
Total Spend =
    SUM(fact_campaign_performance[spend_inr])

// ─── Revenue ──────────────────────────────────────────────────────────────────
// Shopify gross revenue (from the sales fact table)
Total Shopify Revenue =
    SUM(fact_sales[total_sales_inr])

// Ad-platform attributed conversion value (used for ROAS / ROI calculations)
// Using this — not Shopify revenue — keeps spend and revenue on the same
// attribution basis (both come from the ad platform export).
Total Conversion Value =
    SUM(fact_campaign_performance[conversion_value_inr])

// Net Shopify revenue after returns
Net Shopify Revenue =
    SUM(fact_sales[net_revenue_inr])

// ─── Volume ───────────────────────────────────────────────────────────────────
Total Conversions =
    SUM(fact_campaign_performance[purchases])

Total Impressions =
    SUM(fact_campaign_performance[impressions])

Total Clicks =
    SUM(fact_campaign_performance[clicks])

Total Orders =
    SUM(fact_sales[total_orders])
```

### Efficiency Metrics

```dax
// ─── CTR % ────────────────────────────────────────────────────────────────────
// Recalculated at the aggregated grain — never average the per-row ratios.
CTR % =
    DIVIDE(
        [Total Clicks],
        [Total Impressions],
        0
    )
// Format this measure as Percentage in the Measure tools ribbon.

// ─── CPC (INR) ────────────────────────────────────────────────────────────────
CPC =
    DIVIDE([Total Spend], [Total Clicks], 0)

// ─── CPM (INR) ────────────────────────────────────────────────────────────────
CPM =
    DIVIDE([Total Spend] * 1000, [Total Impressions], 0)

// ─── ROAS ─────────────────────────────────────────────────────────────────────
// Attribution-consistent: ad platform conversion value ÷ ad spend.
// Do NOT use Shopify revenue here — it includes organic sales unrelated to ads.
ROAS =
    DIVIDE([Total Conversion Value], [Total Spend], 0)

// ─── ROI % ───────────────────────────────────────────────────────────────────
// (Conversion Value − Spend) ÷ Spend, expressed as a percentage.
ROI % =
    DIVIDE(
        [Total Conversion Value] - [Total Spend],
        [Total Spend],
        0
    )
// Format as Percentage in the Measure tools ribbon.

// ─── Conversion Rate % ────────────────────────────────────────────────────────
Conversion Rate % =
    DIVIDE([Total Conversions], [Total Clicks], 0)
// Format as Percentage in the Measure tools ribbon.
```

### Country-wise Performance Measure

```dax
// ─── Countrywise Spend Share % ────────────────────────────────────────────────
// Returns each region's spend as a % of the total spend in the current filter
// context. Drop this into the Region Matrix visual (Page 2) to add a spend
// share column alongside absolute values.
Spend Share % =
    DIVIDE(
        [Total Spend],
        CALCULATE([Total Spend], ALL(dim_campaign[region])),
        0
    )
// Format as Percentage.
```

### Month-over-Month Spend

```dax
// ─── Previous Month Spend ─────────────────────────────────────────────────────
Previous Month Spend =
    CALCULATE(
        [Total Spend],
        DATEADD(dim_date[date_id], -1, MONTH)
    )

// ─── MoM Spend Change % ───────────────────────────────────────────────────────
// Shows how this month's spend compares to last month's.
// Returns BLANK (not 0) when there's no prior month — prevents misleading 
// -100% readings on the first month in the dataset.
MoM Spend Change % =
    VAR CurrentSpend  = [Total Spend]
    VAR PreviousSpend = [Previous Month Spend]
    RETURN
        IF(
            ISBLANK(PreviousSpend),
            BLANK(),
            DIVIDE(CurrentSpend - PreviousSpend, PreviousSpend, BLANK())
        )
// Format as Percentage.
```

---

## Step 5 — Sync Slicers (Cross-page Filters)

1. **Page 1:** Add two slicers:
   - `dim_date[date_id]` — set style to **Between** (date range picker)
   - `dim_campaign[brand]` — set style to **Dropdown**
2. **View → Sync Slicers panel** — for each slicer, tick **Sync** and
   **Visible** for all 3 pages.

> This satisfies the "at least one cross-page slicer (date + Brand Name)"
> requirement. Both slicers propagate to every page automatically.

---

## Step 6 — Page Layouts

### Page 1 — Executive Summary

| Visual | Fields |
|---|---|
| KPI Card | `[Total Spend]` |
| KPI Card | `[Total Shopify Revenue]` |
| KPI Card | `[ROI %]` |
| KPI Card | `[ROAS]` |
| KPI Card | `[MoM Spend Change %]` |
| Line chart | X: `dim_date[date_id]` / Y1: `[Total Spend]` / Y2: `[Total Conversions]` |
| Table (Top 5 campaigns) | `dim_campaign[campaign_name]`, `[Total Spend]`, `[Total Conversions]`, `[ROAS]` — add Top N filter: 5 by `[Total Spend]` |

### Page 2 — Channel Breakdown

| Visual | Fields |
|---|---|
| Clustered bar | Y: `dim_campaign[status]`, X: `[Total Spend]`, `[Total Conversion Value]` |
| Donut | Legend: `dim_campaign[brand]`, Values: `[Total Spend]` |
| Matrix (Region) | Rows: `dim_campaign[region]`, Values: `[Total Spend]`, `[Spend Share %]`, `[Total Conversions]`, `[CPC]`, `[CTR %]`, `[ROAS]` |
| Clustered bar | Y: `dim_channel[sales_channel]`, X: `[Total Shopify Revenue]`, `[Net Shopify Revenue]` |

> **Region values in the database:** India, United Kingdom, United Arab Emirates, United States, Canada, Australia, Qatar, Saudi Arabia, Unknown. Use these exact strings in Power BI slicers and filters.

**Drill-through setup (mandatory):**
- In Page 2, open the **Visualizations pane → Drill through** field bucket.
- Drag `dim_campaign[brand]` into it.
- Now on Page 1, right-clicking any brand data point shows **Drill through → Channel Breakdown**.

### Page 3 — Audience Insights

| Visual | Fields |
|---|---|
| Bar / Funnel | Axis: `dim_campaign[campaign_name]` (Top 10 filter), Values: `[Total Conversions]`, `[CTR %]` |
| Scatter plot | Details: `dim_campaign[campaign_name]`, X: `[Total Spend]`, Y: `[Total Conversions]`, Size: `[ROI %]` |
| KPI Card | `[Conversion Rate %]` |
| KPI Card | `[CPM]` |

---

## Step 7 — Export & Submit

1. **File → Save As** → `Growify_Marketing_Analytics.pbix`
2. Save into `powerbi/exports/`
3. **File → Export → Export to PDF** → save as
   `Growify_Marketing_Analytics_Report.pdf` in the same folder
4. Commit both files to the repository.

---

## Summary of DAX Measures

| Measure | Type | Notes |
|---|---|---|
| `Total Spend` | Sum | Ad platform spend (INR) |
| `Total Shopify Revenue` | Sum | Gross Shopify sales |
| `Total Conversion Value` | Sum | Ad-attributed revenue (for ROAS/ROI) |
| `Net Shopify Revenue` | Sum | Revenue after returns |
| `Total Conversions` | Sum | Ad platform purchases |
| `Total Impressions` | Sum | — |
| `Total Clicks` | Sum | — |
| `Total Orders` | Sum | Shopify orders |
| `CTR %` | Ratio | Format as % |
| `CPC` | Ratio | Cost per click (INR) |
| `CPM` | Ratio | Cost per 1000 impressions (INR) |
| `ROAS` | Ratio | Conversion value ÷ spend |
| `ROI %` | Ratio | (Conv value − spend) ÷ spend, format as % |
| `Conversion Rate %` | Ratio | Purchases ÷ clicks, format as % |
| `Spend Share %` | Ratio | Regional spend ÷ total spend |
| `Previous Month Spend` | Time intelligence | Hidden helper measure |
| `MoM Spend Change %` | Time intelligence | Format as %; returns BLANK for first month |
