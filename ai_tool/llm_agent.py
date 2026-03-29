"""
llm_agent.py
─────────────────────────────────────────────────────────────────────────────
Marketing Analytics AI Pipeline — Task 5: LLM over SQL
─────────────────────────────────────────────────────────────────────────────
Design decisions:
  1. Schema-narrowing  — Only the two AI views (vw_ai_flexible_performance,
                         vw_ai_kpi_summary) are described to the LLM. Passing
                         internal staging or fact tables would waste tokens and
                         risk the model writing brittle joins.
  2. Two-step pipeline — SQL is generated first, executed against the real DB,
                         and only the result rows are passed to the LLM for
                         interpretation. The full dataset never enters a prompt.
  3. Curated schema    — We describe columns in plain English rather than raw
                         DDL so the model can reason about units and semantics,
                         not just types.
  4. Conversation memory — st.session_state.messages stores every Q&A turn.
                         The last 2 exchanges are injected as context so
                         follow-up questions ("which of those had the best
                         ROAS?") resolve correctly.
  5. Hard LIMIT        — Every generated query is capped at 100 rows so large
                         result sets don't bloat the interpretation prompt.
  6. Anomaly flags     — is_unusual_spend / is_unusual_cpc (Z-score > 2.0)
                         were pre-computed in clean_data.py and are surfaced
                         in the dedicated Anomaly Alerts tab.

Usage:
    streamlit run ai_tool/llm_agent.py
"""

import os
import re
import sqlite3

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

# ─── Config ────────────────────────────────────────────────────────────────────
load_dotenv()
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")

DB_PATH = os.path.join(
    os.path.dirname(__file__), "../data/final/analytics_warehouse.db"
)

st.set_page_config(
    page_title="Marketing Analytics AI",
    page_icon="📊",
    layout="wide",
)

# ─── Curated schema for the LLM ───────────────────────────────────────────────
# We describe only the two AI views in plain English.
# Raw DDL is intentionally omitted — it wastes tokens and confuses the model
# with surrogate keys and internal column names it should never write.
AI_SCHEMA = """
You have access to two SQLite views in the analytics_warehouse database.

━━━ VIEW: vw_ai_flexible_performance ━━━
Grain: one row per (campaign × date). Use for campaign-level or daily analysis.

  date_id          TEXT    'YYYY-MM-DD'. Filter: WHERE date_id BETWEEN '2026-01-01' AND '2026-01-31'
  month            INTEGER 1-12. Filter: WHERE month = 3  (March = 3)
  month_name       TEXT    'January' … 'December'
  quarter          INTEGER 1-4
  week             INTEGER ISO week number
  year             INTEGER e.g. 2026
  day_of_week      INTEGER 0=Sunday … 6=Saturday

  brand            TEXT    e.g. 'Brand A', 'Brand B'
  campaign_name    TEXT    full campaign name
  campaign_status  TEXT    'Active', 'Paused', 'Unknown'
  region           TEXT    Full country name. Values: 'India', 'United Kingdom', 'United Arab Emirates',
                            'United States', 'Canada', 'Australia', 'Qatar', 'Saudi Arabia', 'Unknown'

  spend_inr        REAL    ad spend in INR
  impressions      REAL
  clicks           REAL
  purchases        REAL    ad-attributed conversions
  conversion_value_inr REAL  ad-attributed revenue INR

  ctr_pct          REAL    clicks / impressions × 100
  cpc_inr          REAL    spend / clicks  ← higher = worse
  cpm_inr          REAL    spend / impressions × 1000
  roas             REAL    conversion_value / spend
  roi_pct          REAL    (conversion_value − spend) / spend × 100

  is_unusual_spend INTEGER  1 = spend Z-score > 2.0 (anomaly)
  is_unusual_cpc   INTEGER  1 = CPC Z-score > 2.0 (anomaly)
  metric_recalc_flag INTEGER 1 = source inputs were corrected during cleaning

  shopify_sales_inr          REAL  Shopify gross revenue (same brand+date, may be NULL)
  shopify_returns_inr        REAL
  shopify_net_revenue_inr    REAL
  shopify_orders             REAL
  shopify_avg_order_value_inr REAL

━━━ VIEW: vw_ai_kpi_summary ━━━
Grain: one row per (brand × region × year × quarter × month). Use for aggregated KPI questions.

  brand, region, year, quarter, month, month_name
  active_campaigns         INTEGER  count of distinct campaigns
  total_spend_inr          REAL
  total_impressions        REAL
  total_clicks             REAL
  total_purchases          REAL
  total_conversion_value_inr REAL
  agg_ctr_pct              REAL
  agg_cpc_inr              REAL
  agg_cpm_inr              REAL
  agg_roas                 REAL
  agg_roi_pct              REAL
  shopify_revenue_inr      REAL
  shopify_returns_inr      REAL
  shopify_net_revenue_inr  REAL
  shopify_total_orders     REAL

━━━ QUERY RULES ━━━
- Return ONLY raw SQL — no markdown fences, no explanation.
- Use ONLY vw_ai_flexible_performance or vw_ai_kpi_summary.
- Always add LIMIT 50 unless the user asks for a full summary (then LIMIT 100).
- "Worst CPC" = highest cpc_inr value → ORDER BY cpc_inr DESC
- "Best ROAS" = highest roas → ORDER BY roas DESC
- Region values are full country names: 'United Kingdom', 'United Arab Emirates', 'United States', 'India', etc.
- Month names: January=1, February=2, March=3 … December=12
- For follow-up references like "those campaigns" or "that brand", use the
  context from the conversation history to identify the correct filter.
"""


# ─── DB helpers ───────────────────────────────────────────────────────────────
def get_db_connection():
    if not os.path.exists(DB_PATH):
        st.error(
            f"Database not found at `{DB_PATH}`. "
            "Run `python clean_data.py` then `python load_schema.py` first."
        )
        st.stop()
    return sqlite3.connect(DB_PATH)


def run_query(sql: str) -> tuple[pd.DataFrame | None, str | None]:
    """Execute a SQL query and return (DataFrame, error_string)."""
    conn = get_db_connection()
    try:
        df = pd.read_sql_query(sql, conn)
        return df, None
    except Exception as exc:
        return None, str(exc)
    finally:
        conn.close()


# ─── Gemini helpers ───────────────────────────────────────────────────────────
def _gemini_client():
    from google import genai
    return genai.Client(api_key=GEMINI_API_KEY)


def _call_gemini(prompt: str, model: str, max_retries: int = 3) -> str:
    """
    Call Gemini with automatic retry on 429 rate-limit errors.
    Extracts the suggested retry delay from the API response and waits
    that long before retrying — up to max_retries times.
    Raises a clean, human-readable exception on daily quota exhaustion.
    """
    import time, re as _re
    client = _gemini_client()
    last_error = None

    for attempt in range(max_retries):
        try:
            response = client.models.generate_content(model=model, contents=prompt)
            return response.text.strip()
        except Exception as exc:
            msg = str(exc)
            last_error = exc

            if "429" in msg or "RESOURCE_EXHAUSTED" in msg:
                # Extract retryDelay if present  e.g. "retryDelay": "18s"
                match = _re.search(r"retryDelay.*?(\d+)s", msg)
                wait = int(match.group(1)) + 2 if match else 25

                # Check for daily quota exhaustion (limit: 0 means the cap itself is 0)
                if "GenerateRequestsPerDayPerProjectPerModel" in msg:
                    raise RuntimeError(
                        "🚫 **Daily API quota exhausted** for your Gemini free-tier key.\n\n"
                        "**Options:**\n"
                        "1. Wait until midnight Pacific time for the quota to reset.\n"
                        "2. Switch to a different Gemini model in the sidebar.\n"
                        "3. Enable billing on your Google AI Studio project for higher limits: "
                        "https://aistudio.google.com"
                    ) from exc

                if attempt < max_retries - 1:
                    time.sleep(wait)
                    continue
                else:
                    raise RuntimeError(
                        f"⏳ **Rate limit hit** — still throttled after {max_retries} retries "
                        f"(waited ~{wait}s each). Try again in a minute, or switch models."
                    ) from exc
            else:
                raise  # Non-rate-limit errors bubble up immediately

    raise last_error


def generate_sql(question: str, conversation_history: list) -> str:
    """
    Step 1 of 2 — translate a natural language question into a SQLite query.
    Injects the last 2 Q&A turns so follow-up questions resolve correctly.
    """
    # Build context from last 2 exchanges (4 messages: 2 user + 2 assistant)
    recent = conversation_history[-4:] if len(conversation_history) >= 4 else conversation_history
    history_block = ""
    if recent:
        history_block = "\nCONVERSATION HISTORY (for follow-up context):\n"
        for msg in recent:
            prefix = "User" if msg["role"] == "user" else "Assistant"
            # Truncate long assistant answers to keep the prompt lean
            content = msg["content"][:300] if msg["role"] == "assistant" else msg["content"]
            history_block += f"  {prefix}: {content}\n"

    prompt = f"""You are an expert SQLite analyst embedded in a marketing analytics tool.

DATABASE SCHEMA:
{AI_SCHEMA}
{history_block}
CURRENT QUESTION: {question}

Write a single SQLite SELECT query that answers the question. Follow the QUERY RULES above.
"""
    client = _gemini_client()
    sql = _call_gemini(prompt, model=st.session_state.get("model", "gemini-2.0-flash"))

    # Strip markdown code fences if the model disobeyed
    sql = re.sub(r"^```sql\s*", "", sql, flags=re.IGNORECASE)
    sql = re.sub(r"```$", "", sql).strip()

    # Safety: inject LIMIT if missing
    if "LIMIT" not in sql.upper():
        sql = sql.rstrip(";") + "\nLIMIT 50;"

    return sql


def interpret_result(question: str, sql: str, df: pd.DataFrame, conversation_history: list) -> str:
    """
    Step 2 of 2 — pass only the query result rows to the LLM for a plain-
    English answer. The full dataset never enters this prompt.
    """
    # Only pass last assistant answer for continuity
    prev_answer = ""
    for msg in reversed(conversation_history):
        if msg["role"] == "assistant":
            prev_answer = f"\nPrevious answer (for continuity): {msg['content'][:400]}\n"
            break

    prompt = f"""You are a senior marketing analyst interpreting SQL query results.

User's question: "{question}"
{prev_answer}
SQL query executed:
{sql}

Query results ({len(df)} rows):
{df.to_csv(index=False, float_format="%.2f")}

Provide a concise, professional plain-English answer. Rules:
- Highlight the key metric(s) that directly answer the question.
- Name specific campaigns, brands, or regions from the data.
- If the result is empty, say so clearly and suggest why.
- Do NOT repeat the SQL. Do NOT use bullet soup — write in short paragraphs.
- Use INR for currency values.
"""
    return _call_gemini(prompt, model=st.session_state.get("model", "gemini-2.0-flash"))


# ─── Session state initialisation ─────────────────────────────────────────────
if "messages" not in st.session_state:
    st.session_state.messages = []   # [{role, content, sql?, data?}]

if "model" not in st.session_state:
    st.session_state.model = "gemini-2.0-flash"

# ─── Sidebar ──────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("📊 Marketing Analytics AI")
    st.caption("Powered by Gemini + SQLite")

    st.session_state.model = st.selectbox(
        "Gemini model",
        ["gemini-2.0-flash", "gemini-2.5-flash", "gemini-2.5-pro"],
        help="Flash is faster and free-tier friendly. Pro gives richer answers.",
    )

    st.divider()
    st.markdown("**Example questions**")
    examples = [
        "Which campaign had the worst CPC in March?",
        "Summarise UK region performance",
        "Which brand had the highest ROAS overall?",
        "What was total ad spend vs Shopify revenue in January?",
        "Show me the top 5 campaigns by conversion value",
        "Which region had the lowest ROI last quarter?",
        "Compare CTR across all brands",
        "Which campaigns are flagged as anomalies?",
        "What is the month-over-month spend trend for Brand A?",
        "Which campaign had the most purchases in February?",
    ]
    for i, q in enumerate(examples):
        if st.button(q, use_container_width=True, key=f"ex_{i}"):
            st.session_state.pending_question = q

    st.divider()
    if st.button("🗑 Clear conversation", use_container_width=True):
        st.session_state.messages = []
        st.rerun()

# ─── Tabs ─────────────────────────────────────────────────────────────────────
tab_chat, tab_budget, tab_anomaly = st.tabs(
    ["💬 Insight Q&A", "💰 Budget Optimizer", "🚨 Anomaly Alerts"]
)

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — Insight Q&A with conversation memory
# ══════════════════════════════════════════════════════════════════════════════
with tab_chat:
    st.subheader("Ask anything about campaign or Shopify performance")
    st.caption(
        "Questions are translated to SQL, executed against the warehouse, "
        "and only the result rows are sent to the LLM — not the full dataset."
    )

    # Replay conversation history
    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
            if "sql" in msg:
                with st.expander("SQL query"):
                    st.code(msg["sql"], language="sql")
            if "data" in msg and msg["data"] is not None:
                with st.expander(f"Data ({len(msg['data'])} rows)"):
                    st.dataframe(msg["data"], use_container_width=True)

    # Accept new question — either typed or clicked from sidebar
    pending = st.session_state.pop("pending_question", None)
    user_input = st.chat_input("Ask a question…") or pending

    if user_input:
        if not GEMINI_API_KEY:
            st.error("GEMINI_API_KEY not set. Add it to your .env file.")
            st.stop()

        # Show user message immediately
        with st.chat_message("user"):
            st.markdown(user_input)
        st.session_state.messages.append({"role": "user", "content": user_input})

        with st.chat_message("assistant"):
            with st.spinner("Generating SQL…"):
                try:
                    sql = generate_sql(user_input, st.session_state.messages[:-1])
                except Exception as exc:
                    st.error(f"SQL generation failed: {exc}")
                    st.stop()

            with st.expander("SQL query", expanded=True):
                st.code(sql, language="sql")

            with st.spinner("Executing query…"):
                df, err = run_query(sql)

            if err:
                answer = f"⚠️ The generated SQL returned an error: `{err}`"
                st.error(answer)
                st.session_state.messages.append(
                    {"role": "assistant", "content": answer, "sql": sql, "data": None}
                )
            elif df is None or df.empty:
                answer = "The query ran successfully but returned no matching rows. Try broadening your filter (e.g. remove the month constraint or check the region spelling)."
                st.info(answer)
                st.session_state.messages.append(
                    {"role": "assistant", "content": answer, "sql": sql, "data": df}
                )
            else:
                with st.expander(f"Data ({len(df)} rows)"):
                    st.dataframe(df, use_container_width=True)

                with st.spinner("Interpreting results…"):
                    try:
                        answer = interpret_result(
                            user_input, sql, df, st.session_state.messages[:-1]
                        )
                    except Exception as exc:
                        answer = f"Interpretation failed: {exc}"

                st.markdown(answer)
                st.session_state.messages.append(
                    {"role": "assistant", "content": answer, "sql": sql, "data": df}
                )

# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — Budget Optimizer
# ══════════════════════════════════════════════════════════════════════════════
with tab_budget:
    st.subheader("AI-Powered Budget Allocation")
    st.markdown(
        "The optimizer reads historical ROAS from `vw_ai_kpi_summary` "
        "to recommend how to split a new monthly budget across brands, "
        "weighting allocation toward channels that have delivered the best "
        "return on ad spend."
    )

    col1, col2 = st.columns([2, 1])
    with col1:
        budget = st.number_input(
            "Total monthly budget to allocate (INR)",
            min_value=10_000,
            max_value=50_000_000,
            value=500_000,
            step=10_000,
            format="%d",
        )
    with col2:
        region_filter = st.selectbox(
            "Filter by region (optional)",
            ["All regions", "India", "United Kingdom", "United Arab Emirates",
             "United States", "Canada", "Australia", "Qatar", "Saudi Arabia"],
        )

    if st.button("Calculate optimal allocation", type="primary"):
        if not GEMINI_API_KEY:
            st.error("GEMINI_API_KEY not set in .env")
            st.stop()

        region_clause = (
            "" if region_filter == "All regions"
            else f"WHERE region = '{region_filter}'"
        )

        roas_sql = f"""
SELECT
    brand,
    region,
    SUM(total_spend_inr)            AS historical_spend_inr,
    SUM(total_conversion_value_inr) AS historical_conversion_value_inr,
    ROUND(
        SUM(total_conversion_value_inr) / NULLIF(SUM(total_spend_inr), 0), 3
    )                               AS historical_roas,
    ROUND(
        100.0 * (SUM(total_conversion_value_inr) - SUM(total_spend_inr))
              / NULLIF(SUM(total_spend_inr), 0), 1
    )                               AS historical_roi_pct,
    SUM(shopify_revenue_inr)        AS shopify_revenue_inr
FROM vw_ai_kpi_summary
{region_clause}
GROUP BY brand, region
HAVING SUM(total_spend_inr) > 0
ORDER BY historical_roas DESC;
"""
        with st.spinner("Fetching historical performance…"):
            df_roas, err = run_query(roas_sql)

        if err or df_roas is None or df_roas.empty:
            st.error(f"Could not fetch ROAS data: {err}")
        else:
            st.markdown("**Historical performance by brand/region:**")
            st.dataframe(df_roas, use_container_width=True)

            prompt = f"""You are a Senior Media Buyer at a performance marketing agency.

We have a new budget of ₹{budget:,} INR to allocate across our brands.
{"Region filter applied: " + region_filter if region_filter != "All regions" else "No region filter — consider all brands globally."}

Historical performance data from the database:
{df_roas.to_csv(index=False, float_format="%.2f")}

Task: Recommend an exact percentage and absolute INR allocation for each brand.
Rules:
- Weight allocation proportionally to historical ROAS — higher ROAS → more budget.
- If a brand has negative ROI, recommend 0% with a brief explanation.
- Show your working: explain the allocation logic before the final table.
- Present the final allocation as a clean table: | Brand | Region | Allocation % | Amount (INR) | Rationale |
- Total allocations must sum to exactly ₹{budget:,} INR.
"""
            with st.spinner("Gemini is optimising your budget…"):
                try:
                    result = _call_gemini(prompt, model=st.session_state.model)
                    st.success("**Allocation recommendation:**")
                    st.markdown(result)
                except RuntimeError as exc:
                    st.error(str(exc))
                except Exception as exc:
                    st.error(f"Gemini API error: {exc}")

# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — Anomaly Alerts
# ══════════════════════════════════════════════════════════════════════════════
with tab_anomaly:
    st.subheader("Statistical Anomaly Alerts")
    st.markdown(
        "Campaigns where **spend** or **CPC** is more than 2 standard deviations "
        "from the mean are flagged automatically by the data pipeline "
        "(`is_unusual_spend` / `is_unusual_cpc`). "
        "Use these alerts to catch budget leaks or bidding errors early."
    )

    col_a, col_b = st.columns(2)
    with col_a:
        anomaly_type = st.selectbox(
            "Flag type", ["Unusual Spend", "Unusual CPC", "Either"]
        )
    with col_b:
        # Fetch distinct brands dynamically from the DB so we never hardcode brand names
        _brands = []
        try:
            _conn = get_db_connection()
            _brands = [r[0] for r in _conn.execute(
                "SELECT DISTINCT brand FROM vw_ai_kpi_summary ORDER BY brand"
            ).fetchall()]
            _conn.close()
        except Exception:
            pass
        brand_filter = st.selectbox(
            "Brand filter",
            ["All brands"] + _brands,
        )

    flag_clause = {
        "Unusual Spend": "is_unusual_spend = 1",
        "Unusual CPC":   "is_unusual_cpc = 1",
        "Either":        "(is_unusual_spend = 1 OR is_unusual_cpc = 1)",
    }[anomaly_type]

    brand_clause = (
        "" if brand_filter == "All brands"
        else f"AND brand = '{brand_filter}'"
    )

    anomaly_sql = f"""
SELECT
    date_id,
    brand,
    campaign_name,
    region,
    ROUND(spend_inr, 2)   AS spend_inr,
    ROUND(cpc_inr, 2)     AS cpc_inr,
    ROUND(roas, 3)        AS roas,
    is_unusual_spend,
    is_unusual_cpc
FROM vw_ai_flexible_performance
WHERE {flag_clause}
  {brand_clause}
ORDER BY spend_inr DESC
LIMIT 100;
"""

    if st.button("Load anomalies", type="primary"):
        df_anom, err = run_query(anomaly_sql)

        if err:
            st.error(f"Query error: {err}")
        elif df_anom is None or df_anom.empty:
            st.success("✅ No anomalies detected for the selected filters.")
        else:
            st.warning(f"⚠️ {len(df_anom)} anomalous campaign-days detected")

            # Colour-code: red = unusual spend, amber = unusual CPC
            def highlight_row(row):
                if row.get("is_unusual_spend") == 1:
                    return ["background-color: #ffd6d6"] * len(row)
                elif row.get("is_unusual_cpc") == 1:
                    return ["background-color: #fff3cd"] * len(row)
                return [""] * len(row)

            st.dataframe(
                df_anom.style.apply(highlight_row, axis=1),
                use_container_width=True,
            )

            if GEMINI_API_KEY:
                with st.spinner("Gemini is summarising the anomalies…"):
                    try:
                        client = _gemini_client()
                        summ_prompt = f"""You are a marketing analyst reviewing anomalous ad spend data.

Here are campaigns flagged as statistical outliers (Z-score > 2.0):
{df_anom.head(20).to_csv(index=False, float_format="%.2f")}

Provide a brief (3-5 sentence) executive summary:
- What patterns do you see?
- Which brands or regions appear most?
- What should the media buyer investigate first?
"""
                        result = _call_gemini(summ_prompt, model=st.session_state.model)
                        st.info(f"**Gemini summary:** {result}")
                    except RuntimeError as exc:
                        st.caption(str(exc))
                    except Exception as exc:
                        st.caption(f"Could not generate summary: {exc}")
