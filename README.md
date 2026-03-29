# 📊 Marketing Analytics AI Pipeline

An end-to-end marketing analytics pipeline built for a performance marketing agency. Raw campaign and Shopify sales data flows through a Python cleaning engine, into a SQL star schema warehouse, which powers both a 3-page Power BI dashboard and a custom AI insight tool.

---

## 🏗️ Architecture

```
Raw CSVs (Campaign_Raw.csv + Raw_Shopify_Sales.csv)
                    │
                    ▼
       python/clean_data.py
       ─────────────────────────────────────────
       • Remove duplicates
       • Standardise & validate dates
       • Fill / justify every missing value
       • Correct negative numerics
       • Recalculate CTR, CPC, CPM, ROAS, ROI
       • Flag metric-corrected rows
       • Z-score anomaly detection
       • Normalise all string columns
                    │
          ┌─────────┴──────────┐
          ▼                    ▼
cleaned_campaigns.db     cleaned_shopify.db
          │                    │
          └─────────┬──────────┘
                    ▼
       python/load_schema.py  →  sql/schema.sql
       ──────────────────────────────────────────
       Star Schema (SQLite)
       • dim_date      — day / week / month / quarter / year
       • dim_campaign  — brand, campaign, status, region
       • dim_channel   — brand, sales_channel
       • fact_campaign_performance
       • fact_sales    (net_revenue computed column)
       • 4 Power BI views  (vw_powerbi_*)
       • 2 AI tool views   (vw_ai_*)
       • 11 indexes with documented rationale
                    │
          ┌─────────┴──────────┐
          ▼                    ▼
  Power BI Dashboard       AI Insight Tool
  analytics_warehouse.db   analytics_warehouse.db
  (3-page report + DAX)    (Streamlit + Gemini)
```

---

## 📁 Project Structure

```
marketing-analytics-ai-pipeline/
├── data/
│   ├── raw/
│   │   ├── Campaign_Raw.csv            # Ad platform export (Facebook/Meta)
│   │   └── Raw_Shopify_Sales.csv       # Shopify orders export
│   └── final/
│       ├── cleaned_campaigns.db        # Core campaign data (cleaned)
│       ├── cleaned_shopify.db          # Shopify sales data (cleaned)
│       └── analytics_warehouse.db     # Unified star schema warehouse
│
├── python/
│   ├── clean_data.py                   # Data cleaning, validation, and enrichment
│   ├── load_schema.py                  # Warehouse ingestion and star schema build
│   └── data_quality_report.md         # Automated quality audit report
│
├── sql/
│   └── schema.sql                      # Full star schema DDL + views + indexes
│
├── ai_tool/
│   ├── llm_agent.py                    # Streamlit app: Text-to-SQL + Budget Optimizer
│   └── README.md                       # AI tool documentation
│
├── powerbi/
│   ├── DAX_and_Modeling_Guide.md       # Step-by-step Power BI setup + all DAX
│   └── exports/
│       ├── Marketing Analytics Dashboard.pbix
│       └── Marketing Analytics Dashboard.pdf
│
├── requirements.txt
└── README.md
```

---

## ⚙️ Running the Pipeline

### Prerequisites

- Python 3.10+
- SQLite3 (bundled with Python — no install needed)
- Google Gemini API key (for the AI tool only)

### 1. Install dependencies

```bash
cd marketing-analytics-ai-pipeline
python -m venv .venv
source .venv/bin/activate        # Mac / Linux
# .venv\Scripts\activate         # Windows
pip install -r requirements.txt
```

### 2. Set up environment variables

Create a `.env` file in the project root:

```
GEMINI_API_KEY=your_key_here
```

### 3. Run the data cleaning pipeline

```bash
cd python
python clean_data.py
```

**Outputs:**
- `data/final/cleaned_campaigns.db` — 8,664 clean campaign rows
- `data/final/cleaned_shopify.db` — 4,931 clean Shopify rows
- `python/data_quality_report.md` — full audit of every fix applied

What the cleaner does:
| Step | Detail |
|---|---|
| Duplicates | Detected via exact-match `duplicated()`, removed 310 campaign + 21 Shopify rows |
| Dates | Parsed `DD-MM-YYYY` → ISO; rows with unparseable dates dropped (justified: date is the irreplaceable time key) |
| Missing numerics | Filled with `0` (missing ad activity = zero activity, not unknown) |
| Negative numerics | `abs()` applied — platform export artefact |
| Blank strings | Replaced with `'Unknown'` (queryable; not SQL NULL) |
| Metrics | CTR, CPC, CPM, ROAS, ROI recalculated from source; `metric_recalc_flag=1` on 3,350 rows where inputs were corrected |
| Anomaly flags | Z-score > 2.0 → `is_unusual_spend`, `is_unusual_cpc` |

### 4. Build the SQL star schema warehouse

```bash
python load_schema.py
```

**Output:** `data/final/analytics_warehouse.db`

The warehouse contains:

| Object | Type | Rows |
|---|---|---|
| `dim_date` | Dimension | 87 |
| `dim_campaign` | Dimension | 291 |
| `dim_channel` | Dimension | 12 |
| `fact_campaign_performance` | Fact | 8,664 |
| `fact_sales` | Fact | 4,931 |
| `vw_powerbi_monthly_performance` | View | — |
| `vw_powerbi_campaign_performance` | View | — |
| `vw_powerbi_region_performance` | View | — |
| `vw_powerbi_channel_performance` | View | — |
| `vw_ai_flexible_performance` | View | — |
| `vw_ai_kpi_summary` | View | — |

### 5. Launch the AI Insight Tool

```bash
cd ..
streamlit run ai_tool/llm_agent.py
```

Open `http://localhost:8501` in your browser.

---

## 📊 Power BI Dashboard

Connects directly to `data/final/analytics_warehouse.db` — no CSV files.

| Page | Content |
|---|---|
| **1 — Executive Summary** | KPI cards (Spend, Revenue, ROI, ROAS, MoM Change) · Spend vs Conversions trend · Top 5 campaigns table |
| **2 — Channel Breakdown** | Performance by status (bar) · Brand spend mix (donut) · Region matrix with Spend Share % · Channel revenue breakdown |
| **3 — Audience Insights** | Conversion rate by campaign · Spend vs Conversions scatter (sized by ROI) · CTR & Conversion Rate KPIs |

**Data model:** 4-table star schema with `dim_date` marked as date table.  
**DAX measures:** 17 named measures in a dedicated `_Measures` table.  
**Cross-page slicers:** Date range + Brand (synced across all 3 pages).  
**Drill-through:** Page 1 campaign → Page 2 Channel Breakdown.

See `powerbi/DAX_and_Modeling_Guide.md` for the complete build guide including all DAX formulas.

---

## 🤖 AI Insight Tool

Powered by **Google Gemini** + **Streamlit**. Both tabs query `analytics_warehouse.db` exclusively — no CSV access.

### Tab 1 — 💬 Insight Q&A (Text-to-SQL)

Ask any natural language question about your marketing data:
- *"Which campaign had the highest ROAS last month?"*
- *"What was total ad spend vs revenue by region in January?"*
- *"Which brand had the lowest CPC?"*

The AI generates SQL against `vw_ai_flexible_performance`, executes it, and explains the results in plain English.

### Tab 2 — 💰 AI Budget Optimizer

Enter a monthly budget and the AI will:
1. Pull historical ROAS and spend data per brand from `vw_ai_kpi_summary`
2. Recommend an optimised allocation strategy
3. Justify each allocation based on past performance

---

## 📇 CRM & Lead Management Integration

Beyond analytics, the pipeline includes an integrated Lead Management system via Airtable. This module allows for:
- **Lead Sorting**: Automatic grouping by priority (High, Med, Low).
- **Communication Logs**: Centralised tracking of lead contact information.
- **Workflow Sync**: Seamless transition from marketing insight to sales action.

![Lead Management Airtable](images/lead_management.png)

---

## 🚨 Anomaly Detection

The pipeline flags statistically unusual campaigns using Z-scores:

| Flag | Condition | Use in Power BI |
|---|---|---|
| `is_unusual_spend = 1` | Spend > 2σ from mean | Filter pane alert |
| `is_unusual_cpc = 1` | CPC > 2σ from mean | Filter pane alert |
| `metric_recalc_flag = 1` | Source inputs were null or negative | Data quality audit |

---

## 🔑 Tech Stack

| Layer | Technology |
|---|---|
| Data Cleaning | Python 3, Pandas, NumPy |
| Database | SQLite 3.31+ (star schema) |
| AI / LLM | Google Gemini API |
| Dashboard | Power BI Desktop |
| AI Tool UI | Streamlit |

---

## 🎥 Video Demo

[![Watch the demo](https://img.shields.io/badge/YouTube-Watch%20Demo-red?logo=youtube)](https://youtu.be/7upqh8jjECM)

[https://youtu.be/7upqh8jjECM](https://youtu.be/7upqh8jjECM)

---

## 👤 Author

**Aayush Tripathi**
