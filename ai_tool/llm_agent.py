import streamlit as st
import sqlite3
import pandas as pd
import os
import re
from dotenv import load_dotenv

load_dotenv()  # Load variables from .env file
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")

st.set_page_config(page_title="Marketing Analytics AI Pipeline", layout="wide")

DB_PATH = os.path.join(os.path.dirname(__file__), '../data/final/analytics.db')

def get_db_connection():
    return sqlite3.connect(DB_PATH)

def get_schema():
    """Extract table and view schemas from SQLite."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT type, name, sql FROM sqlite_master WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%'")
    schema_info = ""
    for row_type, name, sql in cursor.fetchall():
        schema_info += f"--- {row_type.upper()}: {name} ---\n{sql}\n\n"
    conn.close()
    return schema_info

@st.cache_data(ttl=600)
def execute_sql(query):
    """Execute SQL query safely and return a DataFrame."""
    conn = get_db_connection()
    try:
        df = pd.read_sql_query(query, conn)
        return df, None
    except Exception as e:
        return None, str(e)
    finally:
        conn.close()

def generate_sql(api_provider, api_key, model_name, user_query, schema):
    prompt = f"""
You are an expert Data Analyst working at a marketing agency.
Your task is to translate the user's plain English question into a valid SQLite SQL query based on the database schema below.

SCHEMA:
{schema}

USER QUESTION: {user_query}

RULES:
1. Return ONLY the raw SQL query.
2. Do not include markdown formatting like ```sql...``` or any additional text.
3. Only use the tables and columns provided in the schema.
4. If a calculation is requested (e.g. CTR, CPC, ROAS), use the existing pre-calculated columns if possible, otherwise write the formula correctly handling divide by zero by either using NULLIF or matching formulas.
"""
    
    if api_provider == "OpenAI":
        from openai import OpenAI
        client = OpenAI(api_key=api_key)
        response = client.chat.completions.create(
            model=model_name,
            messages=[{"role": "user", "content": prompt}],
            temperature=0
        )
        sql = response.choices[0].message.content.strip()
    elif api_provider == "Google Gemini":
        from google import genai
        client = genai.Client(api_key=api_key)
        response = client.models.generate_content(
            model=model_name,
            contents=prompt,
        )
        sql = response.text.strip()
    else:
        raise ValueError("Invalid Provider")
    
    # Strip markdown if LLM disobeyed
    if sql.startswith("```sql"):
        sql = sql[6:]
    if sql.endswith("```"):
        sql = sql[:-3]
    return sql.strip()

def interpret_result(api_provider, api_key, model_name, user_query, sql_query, df_result):
    
    prompt = f"""
You are a data reporting assistant.
The user asked: "{user_query}"
We executed this SQL query to get the answer:
{sql_query}

Here are the results from the database (in CSV format):
{df_result.to_csv(index=False)}

Please provide a concise, natural language answer summarizing the data. Highlight key metrics and do not repeat the SQL code. Keep it professional.
"""
    if api_provider == "OpenAI":
        from openai import OpenAI
        client = OpenAI(api_key=api_key)
        response = client.chat.completions.create(
            model=model_name,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3
        )
        return response.choices[0].message.content.strip()
    elif api_provider == "Google Gemini":
        import google.genai as genai
        client = genai.Client(api_key=api_key)
        response = client.models.generate_content(
            model=model_name,
            contents=prompt,
        )
        return response.text.strip()

# ==================== UI ====================

st.title("🤖 Marketing Analytics AI Pipeline")
st.markdown("Ask natural language questions about marketing campaigns and Shopify sales performance.")

with st.sidebar:
    st.header("⚙️ Configuration")
    model = st.selectbox("Model", ["gemini-2.5-flash", "gemini-2.5-pro"])
        
    st.markdown("---")
    st.markdown("### Suggested Questions")
    st.info("1. What was the total Ad Spend and Revenue across all brands?")
    st.info("2. Which campaign had the highest ROAS?")
    st.info("3. Give me the daily spend and sales for Brand A in October.")


tab1, tab2 = st.tabs(["💬 Insight Q&A", "💰 AI Budget Optimizer"])

with tab1:
    user_question = st.text_area("What would you like to know?", height=100, placeholder="e.g. Which campaign had the highest CTR?")

    if st.button("Generate Answer", type="primary"):
        if not user_question:
            st.warning("⚠️ Please ask a question.")
        else:
            # Backend API Key (loaded from .env)
            api_key = GEMINI_API_KEY
            provider = "Google Gemini"
            
            with st.spinner("Thinking..."):
                st.write("Extracting database schema...")
                db_schema = get_schema()
                
                st.write("Translating question to SQL...")
                try:
                    sql_query = generate_sql(provider, api_key, model, user_question, db_schema)
                    st.code(sql_query, language="sql")
                    
                    st.write("Executing Query...")
                    df, err = execute_sql(sql_query)
                    
                    if err:
                        st.error(f"SQL Execution Error: {err}")
                    else:
                        st.write("Analyzing Results...")
                        st.dataframe(df)
                        
                        if not df.empty:
                            interpretation = interpret_result(provider, api_key, model, user_question, sql_query, df)
                            st.write("Generating natural language summary...")
                            st.success(interpretation)
                        else:
                            st.info("Query returned no results.")
                            
                except Exception as e:
                    st.error(f"An error occurred: {str(e)}")

with tab2:
    st.subheader("Data-Driven Budget Allocation")
    st.markdown("Automatically aggregate historical Return on Ad Spend (ROAS) per brand/channel to intelligently assign new budgets.")
    
    budget_input = st.number_input("Total Monthly Budget to Allocate (INR)", min_value=1000, max_value=10000000, value=500000, step=10000)
    
    if st.button("Calculate Optimal Allocation", type="primary"):
        api_key = GEMINI_API_KEY
        with st.spinner("Analyzing Historical ROAS..."):
            # Aggregate historical performance from DB
            roas_query = """
            SELECT 
                d.brand, 
                SUM(f.spend_inr) as historical_spend, 
                SUM(f.conversion_value_inr) as historical_returns,
                (SUM(f.conversion_value_inr) / NULLIF(SUM(f.spend_inr), 0)) as roas
            FROM fact_campaign_performance f
            JOIN dim_campaign d ON f.campaign_id = d.campaign_id
            GROUP BY d.brand
            HAVING SUM(f.spend_inr) > 0
            ORDER BY roas DESC;
            """
            df_roas, err = execute_sql(roas_query)
            
            if err:
                st.error("Could not fetch historical ROAS.")
            else:
                st.write("Historical Channel Performance:")
                st.dataframe(df_roas)
                
                st.write("Calling Gemini to formulate strategic distribution...")
                prompt = f"""
                You are a Senior Media Buyer. We have a new budget of {budget_input} INR to allocate across our brands.
                Here is our historical profitability (ROAS) per brand:
                {df_roas.to_csv(index=False)}
                
                Please recommend an exact percentage and absolute monetary allocation of the {budget_input} INR budget across the available brands based heavily on which ones have the best historical ROAS.
                Provide your reasoning, focusing strongly on maximizing ROI. Present the final allocation clearly in a bulleted list.
                """
                
                try:
                    import google.genai as genai
                    client = genai.Client(api_key=api_key)
                    response = client.models.generate_content(
                        model=model,
                        contents=prompt,
                    )
                    
                    st.success(response.text.strip())
                except Exception as e:
                    st.error(f"Gemini API Error: {str(e)}")
