# AI Insight Tool — LLM over SQL

A natural language analytics interface built on top of the marketing analytics
star schema. Ask questions in plain English — the tool writes the SQL, runs it
against the warehouse, and returns a plain-English answer. Only the result rows
(not the full dataset) ever enter an LLM prompt.

---

## Setup

### 1. Prerequisites

- Python 3.10+
- The data pipeline must have been run first:
  ```bash
  cd python
  python clean_data.py    # creates cleaned_campaigns.db + cleaned_shopify.db
  python load_schema.py   # creates analytics_warehouse.db
  ```

### 2. Install dependencies

```bash
cd marketing-analytics-ai-pipeline
python -m venv .venv
source .venv/bin/activate      # Mac / Linux
# .venv\Scripts\activate       # Windows
pip install -r requirements.txt
```

### 3. Set your API key

Create a `.env` file in the project root (one level above `ai_tool/`):

```
GEMINI_API_KEY=your_key_here
```

Get a free key at [https://aistudio.google.com/app/apikey](https://aistudio.google.com/app/apikey).

### 4. Run the app

```bash
streamlit run ai_tool/llm_agent.py
```

Open [http://localhost:8501](http://localhost:8501) in your browser.

---

## Features

### Tab 1 — 💬 Insight Q&A

Ask any natural language question. The tool:

1. Translates your question to a targeted SQLite query using Gemini
2. Executes the query against `analytics_warehouse.db`
3. Passes **only the result rows** (not the full dataset) to Gemini
4. Returns a plain-English answer alongside the SQL and data table

**Conversation memory** is maintained across turns in the same session.
Follow-up questions like *"which of those had the best ROAS?"* or
*"break that down by region"* resolve correctly because the last 2 Q&A
exchanges are injected as context into every new SQL generation call.

Click **Clear conversation** in the sidebar to start fresh at any time.

### Tab 2 — 💰 Budget Optimizer

Enter a total monthly budget. The optimizer:

1. Fetches historical ROAS per brand/region from `vw_ai_kpi_summary`
2. Passes the performance summary to Gemini
3. Returns an exact INR allocation table weighted by historical ROAS

Optional region filter narrows the recommendation to a specific geography.

### Tab 3 — 🚨 Anomaly Alerts

Surfaces campaigns pre-flagged by the data pipeline using Z-score detection:

- `is_unusual_spend = 1` — spend is > 2 standard deviations from the mean
- `is_unusual_cpc = 1` — CPC is > 2 standard deviations from the mean

Rows are colour-coded (red = spend anomaly, amber = CPC anomaly) and Gemini
generates an executive summary of the most important patterns to investigate.

---

## Design Decisions

### 1. Schema narrowing
Only the two AI views (`vw_ai_flexible_performance`, `vw_ai_kpi_summary`) are
described to the LLM — not the full star schema. This reduces token usage,
prevents the model from writing brittle joins across raw fact tables, and
improves SQL accuracy significantly.

The schema description uses **plain English** (with units, semantics, and
query hints) rather than raw DDL. This lets the model reason about *what the
data means*, not just what types the columns have.

### 2. Two-step pipeline (SQL → interpret)
SQL generation and result interpretation are two separate LLM calls with
different prompts and temperatures:

| Step | Goal | Context passed |
|---|---|---|
| SQL generation | Correctness | Schema + question + conversation history |
| Interpretation | Readability | SQL + result rows + previous answer |

This keeps each call focused and prevents the model from hallucinating numbers
(it can only report what the SQL returned).

### 3. Conversation memory
`st.session_state.messages` stores every Q&A turn as
`{role, content, sql, data}`. The **last 2 exchanges** (4 messages) are
injected into the SQL generation prompt as conversation context. Older history
is excluded to keep token counts predictable.

### 4. Hard LIMIT on all queries
Every generated query is automatically capped at 50 rows (100 for summaries).
This prevents large result sets from bloating the interpretation prompt and
keeps API costs low.

### 5. Pre-computed anomaly flags
`is_unusual_spend` and `is_unusual_cpc` are computed once during data cleaning
using Z-scores (threshold: ±2.0) and stored in the fact table. The AI tool
reads these flags directly — it doesn't recompute statistics at query time.

### 6. Budget optimizer uses aggregated view
Tab 2 queries `vw_ai_kpi_summary` (pre-aggregated to brand × region × month)
rather than scanning the raw fact table. This is faster and produces exactly
the grain the optimizer needs: total spend and ROAS per brand.

---

## 10 Example Questions

These questions cover the two mandatory examples plus a full range of use cases:

| # | Question | View used |
|---|---|---|
| 1 | Which campaign had the worst CPC in March? | `vw_ai_flexible_performance` |
| 2 | Summarise UK region performance | `vw_ai_kpi_summary` |
| 3 | Which brand had the highest ROAS overall? | `vw_ai_kpi_summary` |
| 4 | What was the total ad spend vs Shopify revenue in January? | `vw_ai_kpi_summary` |
| 5 | Show me the top 5 campaigns by conversion value | `vw_ai_flexible_performance` |
| 6 | Which region had the lowest ROI last quarter? | `vw_ai_kpi_summary` |
| 7 | Compare average CTR across all brands | `vw_ai_kpi_summary` |
| 8 | Which campaigns are flagged as anomalies this month? | `vw_ai_flexible_performance` |
| 9 | What is the month-over-month spend trend for Brand A? | `vw_ai_kpi_summary` |
| 10 | Which campaign had the most purchases in February? | `vw_ai_flexible_performance` |

### Follow-up question examples (conversation memory)

```
User:   "Which campaign had the worst CPC in March?"
AI:     "Campaign X had a CPC of ₹842 in March..."

User:   "What was its ROAS?"          ← follow-up, no need to repeat campaign name
AI:     "Campaign X had a ROAS of 1.2, meaning it returned ₹1.2 for every ₹1 spent..."

User:   "Break that down by region"   ← follow-up on the same campaign
AI:     "Across regions, Campaign X performed as follows..."
```

---

## Data Sources

The tool connects exclusively to:

```
data/final/analytics_warehouse.db
```

It reads from two views:

| View | Description |
|---|---|
| `vw_ai_flexible_performance` | Row-level daily campaign metrics + Shopify join |
| `vw_ai_kpi_summary` | Pre-aggregated KPIs by brand / region / month |

**No CSV files are ever read by this tool.**

---

## Requirements

Key dependencies (see `requirements.txt` for pinned versions):

```
streamlit
google-genai
pandas
python-dotenv
```
