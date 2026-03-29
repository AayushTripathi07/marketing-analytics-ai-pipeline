# Marketing Analytics AI Pipeline - Insight Tool

This Text-to-SQL agent provides a natural language interface to query the SQLite database. It leverages modern LLMs to translate plain English queries into accurate SQL, execute them, and return a human-readable interpretation of the results.

## Requirements

Ensure your virtual environment is activated and the dependencies are installed:
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

*(Note: If you haven't yet, ensure you ran the `clean_data.py` and `schema.sql` files to generate the `analytics.db` database before starting this application).*

## Starting the Application

Navigate to the root directory and run the Streamlit application:
```bash
streamlit run ai_tool/llm_agent.py
```

## How to use:

1. Visit the generated local url (usually `http://localhost:8501`).
2. Input a question into the text area. 
   **Example questions:**
   - "What was the total spend and revenue by brand?"
   - "Which campaign generated the most sales?"
   - "Give me the average ROAS for 'Brand A' compared to 'Brand B'."
   - "Show me a daily breakdown of Revenue and Ad Spend in November 2024 for Brand C."
5. Click **Generate Answer**.
6. The app will:
   - Extract the entire database schema from `analytics.db`.
   - Use the LLM to write a valid SQLite query.
   - Run the Query against the Database.
   - Produce a final Natural Language Summary alongside the underlying SQL and Table Data.
