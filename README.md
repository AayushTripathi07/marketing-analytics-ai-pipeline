# 📊 Marketing Analytics AI Pipeline

An end-to-end marketing analytics pipeline built for a performance marketing agency. Raw campaign and Shopify sales data flows through Python cleaning, into a SQL Star Schema database, which powers both a Power BI dashboard and a custom AI insight tool.

---

## 🏗️ Architecture

```
Raw CSVs (Campaign + Shopify)
        ↓
Python Cleaning Engine (clean_data.py)
        ↓
SQLite Database — analytics.db (Star Schema)
        ↓
    ┌───────────────────────────┐
    │                           │
Power BI Dashboard         AI Insight Tool
(3-page report + DAX)      (Streamlit + Gemini)
```

---

## 📁 Project Structure

```
marketing-analytics-ai-pipeline/
├── data/
│   ├── raw/                        # Original raw CSV files
│   │   ├── Campaign_Raw.csv
│   │   └── Raw_Shopify_Sales.csv
│   └── final/
│       └── analytics.db            # Cleaned SQLite Star Schema database
│
├── python/
│   ├── clean_data.py               # Main cleaning + anomaly detection pipeline
│   └── data_quality_report.md      # Auto-generated data quality report
│
├── sql/
│   └── schema.sql                  # Star Schema DDL (run against analytics.db)
│
├── ai_tool/
│   ├── llm_agent.py                # Streamlit AI app (Text-to-SQL + Budget Optimizer)
│   └── README.md                   # AI tool documentation
│
├── powerbi/
│   ├── DAX_and_Modeling_Guide.md   # Step-by-step Power BI setup guide + DAX formulas
│   └── exports/                    # Place your .pbix and .pdf files here
│
├── requirements.txt
└── README.md
```

---

## ⚙️ Setup & Running the Pipeline

### Prerequisites
- Python 3.10+
- SQLite3 (built into Python)

### Step 1: Install Dependencies
```bash
cd marketing-analytics-ai-pipeline
python -m venv .venv
source .venv/bin/activate       # Mac/Linux
# .venv\Scripts\activate        # Windows
pip install -r requirements.txt
```

### Step 2: Run the Data Cleaning Pipeline
```bash
cd python
python clean_data.py
```
This will:
- Clean and validate the raw CSV data
- Recalculate all marketing metrics (CTR, CPC, CPM, ROAS)
- Flag anomalous campaigns using Z-Score detection
- Write the cleaned data to `data/final/analytics.db`

### Step 3: Apply the SQL Star Schema
```bash
sqlite3 data/final/analytics.db < sql/schema.sql
```
This creates the following tables:
- `dim_date` — Date dimension
- `dim_campaign` — Campaign dimension
- `fact_campaign_performance` — Campaign metrics fact table (with anomaly flags)
- `fact_sales` — Shopify sales fact table
- `vw_daily_brand_summary` — Reporting view
- `vw_overall_performance` — KPI summary view

### Step 4: Launch the AI Insight Tool
```bash
cd marketing-analytics-ai-pipeline
source .venv/bin/activate
streamlit run ai_tool/llm_agent.py
```
Open your browser at `http://localhost:8501`.

---

## 🤖 AI Insight Tool Features

### Tab 1 — 💬 Insight Q&A (Text-to-SQL)
Ask any natural language question about your marketing data:
- *"Which campaign had the highest ROAS?"*
- *"What was total ad spend vs revenue in October?"*
- *"Which brand had the lowest CPC?"*

The AI automatically generates the SQL query, executes it, and explains the results in plain English.

### Tab 2 — 💰 AI Budget Optimizer
Enter a total monthly budget and the AI will:
1. Pull historical ROAS data per brand from the database
2. Recommend an optimized budget allocation strategy
3. Justify each allocation based on past performance data

---

## 📊 Power BI Dashboard

The dashboard connects directly to `analytics.db` using the SQLite ODBC driver.

**3 Pages:**
- **Page 1 — Executive Summary:** KPI cards, Spend vs Conversions trend, Top 5 campaigns
- **Page 2 — Channel Breakdown:** Performance by platform, channel mix donut, region matrix
- **Page 3 — Audience Insights:** CTR by campaign, Spend vs Conversions scatter

**Features:**
- Cross-page Date + Brand Name slicers
- Drill-through from Page 1 → Page 2
- 8 custom DAX measures

See `powerbi/DAX_and_Modeling_Guide.md` for complete build instructions.

---

## 🚨 Anomaly Detection

The pipeline automatically flags unusual campaigns using statistical Z-Scores:
- `is_unusual_spend = 1` → Campaign spend is > 2 standard deviations from the mean
- `is_unusual_cpc = 1` → Campaign CPC is > 2 standard deviations from the mean

Use these columns in Power BI's **Filter pane** to build a dedicated Anomaly Alerts page.

---

## 🔑 Tech Stack

| Layer | Technology |
|-------|-----------|
| Data Cleaning | Python, Pandas, NumPy |
| Database | SQLite (Star Schema) |
| AI / LLM | Google Gemini API |
| Dashboard | Power BI Desktop |
| AI Tool UI | Streamlit |
