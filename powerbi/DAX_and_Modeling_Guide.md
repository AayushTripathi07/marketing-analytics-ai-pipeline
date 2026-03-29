# Power BI Dashboard Implementation Guide (Task 4)

Because Power BI Desktop is not natively compatible with macOS, you have two options to build this:
1. Run Power BI Desktop inside a **Windows Virtual Machine** (like Parallels or UTM) on your Mac.
2. Borrow a Windows PC for 1-2 hours to assemble the `.pbix` file.

*Note: The Power BI Website (Service) does NOT allow you to connect directly to local SQL files or run Power Query without a Desktop Gateway. You **must** do this inside Power BI Desktop to construct the data model and DAX.*

Follow this step-by-step guide precisely to secure maximum layout and DAX quality points.

---

## Step 1: Connecting to the SQL Database
1. Open **Power BI Desktop**.
2. Go to **Home > Get Data > More...**
3. Search for **ODBC** or **SQLite**. (If you installed an SQLite ODBC driver, select ODBC. Alternatively, you can install the third-party Power BI SQLite connector).
4. Connect to the absolute file path of your SQLite database: `.../marketing-analytics-ai-pipeline/data/final/analytics.db`.
5. Check the boxes for the following tables to import them:
   - `dim_date`
   - `dim_campaign`
   - `fact_campaign_performance`
   - `fact_sales`
6. Click **Load**. *(Do not import the CSVs)*.

---

## Step 2: Data Modeling (Star Schema)
To ensure DAX time-intelligence functions properly, we must structure the relationships correctly.

1. Go to the **Model View** (the relationship icon on the far left panel).
2. **Date Relationship**:
   - Drag `date` from `dim_date` to `date` on `fact_campaign_performance`. (1-to-many 🌟).
   - Drag `date` from `dim_date` to `date` on `fact_sales`. (1-to-many 🌟).
3. **Campaign Relationship**:
   - Drag `campaign_id` from `dim_campaign` to `campaign_id` on `fact_campaign_performance`. (1-to-many 🌟).
4. **Mark as Date Table**:
   - Right-click the `dim_date` table in the fields pane -> **Mark as date table**. Select the `date` column. This is **mandatory** for the MoM calculation!
5. **Clean Field List**:
   - Hide the `campaign_id`, and `date` columns inside your *Fact Tables* by clicking the little "eye" icon. Users should only filter using Dimensions!

---

## Step 3: DAX Measures
Create a dedicated folder for your measures. Click **Enter Data** on the Home tab, name it `_Key Measures`, and load an empty table. Right-click this table to add your DAX formulas.

```dax
// 1. Total Spend
Total Spend = SUM(fact_campaign_performance[spend_inr])

// 2. Total Sales
Total Sales = SUM(fact_sales[total_sales_inr])

// 3. Total Conversions
Total Conversions = SUM(fact_campaign_performance[purchases])

// 4. ROI (%)
ROI % = DIVIDE([Total Sales] - [Total Spend], [Total Spend], 0)

// 5. CTR %
CTR % = DIVIDE(SUM(fact_campaign_performance[clicks]), SUM(fact_campaign_performance[impressions]), 0)

// 6. CPC (Cost Per Click)
CPC = DIVIDE([Total Spend], SUM(fact_campaign_performance[clicks]), 0)

// 7. ROAS
ROAS = DIVIDE([Total Sales], [Total Spend], 0)

// 8. Month-over-Month (MoM) Spend Change %
MoM Spend Change % = 
VAR PreviousMonthSpend = CALCULATE([Total Spend], DATEADD(dim_date[date], -1, MONTH))
RETURN DIVIDE([Total Spend] - PreviousMonthSpend, PreviousMonthSpend, 0)
```
*(Format percentages natively in Power BI by clicking the % icon at the top).*

---

## Step 4: Building the 3-Page Dashboard

### Page Setup & Sync Slicers (Mandatory)
1. Add a **Slicer** for `dim_date[date]` (set as Between/Timeline).
2. Add a **Slicer** for `dim_campaign[brand]` (set as Dropdown).
3. Go to **View > Sync Slicers**. Check the boxes to sync and make these two slicers visible across **all 3 pages**.

### Page 1 — Executive Summary
*Goal: High-level overview of the pipeline.*
- **KPI Cards**: Place 5 visually prominent cards at the top using your DAX measures: `Total Spend`, `Total Sales`, `ROI %`, `ROAS`, and `MoM Spend Change %`.
- **Line Chart (Spend vs Conversions)**:
  - X-axis: `dim_date[date]`
  - Y-axis (Left): `[Total Spend]`
  - Y-axis (Right): `[Total Conversions]`
- **Table (Top 5 Campaigns)**:
  - Columns: `dim_campaign[campaign_name]`, `[Total Spend]`, `[Total Conversions]`, `[ROAS]`
  - Use the Filters pane on this visual to filter by "Top N" = 5 based on `Total Spend`.

### Page 2 — Channel Breakdown
*Goal: Granular breakdown by platform and geography.*
- **Bar Chart (Performance by Platform/Status)**:
  - Y-axis: `dim_campaign[status]` or `sales_channel`
  - X-axis: `[Total Sales]`
- **Donut Chart (Channel Mix)**:
  - Legend: `dim_campaign[brand]`
  - Values: `[Total Spend]`
- **Matrix (Region Performance)**:
  - Rows: `dim_campaign[region]`
  - Values: `[Total Spend]`, `[Total Conversions]`, `[CPC]`, `[CTR %]`
  - *(This fulfills the "Countrywise Performance" metric visually)*.

### Page 3 — Audience Insights
*Goal: Deep dive into conversion efficiency.*
- **Funnel or Bar Chart (Conversion Rate by Segment)**:
  - Group by: `dim_campaign[campaign_name]`
  - Values: `[Total Conversions]` and `[CTR %]`
- **Scatter Plot (Spend vs Conversions)**:
  - Details/Values: `dim_campaign[campaign_name]`
  - X-axis: `[Total Spend]`
  - Y-axis: `[Total Conversions]`
  - Size: `[ROI %]` (Optional but creates a great aesthetic).

---

## Step 5: Drill-Through Capability
To earn maximum points, enable Drill-through so executives can right-click a high-level summary and "drill" into the details.

1. Go to **Page 2 (Channel Breakdown)**.
2. Under the Visualizations pane, locate the **Drill-through** field bucket.
3. Drag `dim_campaign[brand]` into this bucket.
4. Now, go back to **Page 1**. Right-click on any Brand/Campaign data point inside your visuals. You will see a new option: **Drill through > Channel Breakdown**. This transports the user to Page 2 automatically filtered for exactly that brand!

---

## Step 6: Export & Submit
1. Double-check your layout aesthetics (use a cohesive dark theme or matching company colors, ensure all visuals align cleanly).
2. Save the file locally as `Growify_Marketing_Analytics.pbix`.
3. Go to **File > Export > Export to PDF** to generate the requested static PDF of the 3 pages.
4. Upload both the `.pbix` and `.pdf` files to your GitHub repository or include them in your final ZIP handoff. 
