import streamlit as st
import pandas as pd
from datetime import datetime
import snowflake.connector
from snowflake.snowpark import Session

from cortex_file_qa import (
    ingest_uploaded_file,
    answer_structured_question,
    answer_document_question,
    generate_and_run_warehouse_sql,
)

# Configuration
HOST = "XYUHKAV-XRB12650.snowflakecomputing.com"
ACCOUNT = "XYUHKAV-XRB12650"
DATABASE = "INVENTORY_DW"
SCHEMA = "GOLD"
WAREHOUSE = "COMPUTE_WH"

# Page Configuration
st.set_page_config(page_title="Dilytics Inventory AI", page_icon="📦", layout="wide")

# Custom UI Styling
st.markdown("""
<style>
    .status-pill {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        background-color: #ecfdf5;
        color: #065f46;
        border: 1px solid #a7f3d0;
        border-radius: 20px;
        padding: 2px 10px;
        font-size: 0.75rem;
        font-weight: 600;
    }
    div[data-testid="stButton"] > button {
        border-radius: 8px;
        font-weight: 500;
        transition: all 0.2s ease-in-out;
    }
</style>
""", unsafe_allow_html=True)


# ===================================================================
# SCHEMA DESCRIPTION — this is what replaces the hardcoded if/elif
# matcher. Cortex reads this once per question and writes SQL against
# your REAL tables for ANY question, not just a fixed list.
# Edit this if your GOLD schema changes.
# ===================================================================
WAREHOUSE_SCHEMA_DESCRIPTION = """
Table: INVENTORY_DW.GOLD.FACT_INVENTORY_DAILY_SNAPSHOT (one row per product per warehouse per day)
  - SNAPSHOT_DATE_KEY (join key to DIM_DATE.DATE_KEY)
  - WAREHOUSE_KEY (join key to DIM_WAREHOUSE.WAREHOUSE_KEY)
  - PRODUCT_KEY (join key to DIM_PRODUCT.PRODUCT_KEY)
  - ON_HAND_QTY (number, physical quantity on hand)
  - INVENTORY_VALUE_AMT (number, dollar value of inventory)
  - EXCESS_STOCK_VALUE_AMT (number, dollar value of stock above safety buffer)
  - IS_STOCKOUT_FLAG (boolean, true if product is out of stock)
  - IS_REORDER_NEEDED_FLAG (boolean, true if below reorder threshold)

Table: INVENTORY_DW.GOLD.DIM_WAREHOUSE
  - WAREHOUSE_KEY
  - WAREHOUSE_NAME

Table: INVENTORY_DW.GOLD.DIM_PRODUCT
  - PRODUCT_KEY
  - PRODUCT_SKU
  - PRODUCT_NAME
  - CATEGORY_NAME
  - SUBCATEGORY_NAME
  - BRAND_NAME
  - ABC_CLASSIFICATION

Table: INVENTORY_DW.GOLD.DIM_DATE
  - DATE_KEY
  - FULL_DATE (actual calendar date)
"""

# Cheap guardrail so obviously off-topic chat doesn't burn a Cortex call.
# This does NOT limit which questions can be asked about inventory — it only
# filters things with zero domain relevance at all.
DOMAIN_KEYWORDS = [
    "inventory", "warehouse", "product", "stock", "stockout", "excess", "quarantine",
    "reorder", "category", "subcategory", "brand", "abc", "hazardous", "perishable",
    "cold-chain", "sku", "supply", "quantity", "value", "profit", "sales", "on hand",
]

GREETING_PATTERNS = ["how are you", "how's it going", "what's up", "whats up", "hi", "hello", "hey",
                      "good morning", "good evening", "help", "what can you do", "what can i ask"]


# ===================================================================
# 1. STREAMLIT CLOUD LOGIN SCREEN
# ===================================================================
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False
    st.session_state.username = "PBCS"
    st.session_state.password = ""
    st.session_state.snowpark_session = None

if not st.session_state.authenticated:
    st.title("Welcome to Dilytics Inventory AI")
    st.markdown("Please login to connect to your Snowflake Data Warehouse.")
    st.session_state.username = st.text_input("Enter Snowflake Username:", value=st.session_state.username)
    st.session_state.password = st.text_input("Enter Password:", type="password")
    if st.button("Login"):
        try:
            with st.spinner("Connecting to Snowflake..."):
                conn = snowflake.connector.connect(
                    user=st.session_state.username,
                    password=st.session_state.password,
                    account=ACCOUNT,
                    host=HOST,
                    port=443,
                    warehouse=WAREHOUSE,
                    role="ACCOUNTADMIN",
                    database=DATABASE,
                    schema=SCHEMA
                )
                st.session_state.snowpark_session = Session.builder.configs({"connection": conn}).create()
                st.session_state.authenticated = True
                st.rerun()
        except Exception as e:
            st.error(f"Authentication failed: {e}")
    st.stop()


# ===================================================================
# 2. MAIN APP LOGIC (Runs only after login)
# ===================================================================
session = st.session_state.snowpark_session

# Session State Management for Multi-Chat History
if "chat_sessions" not in st.session_state:
    st.session_state.chat_sessions = {}
if "current_session_id" not in st.session_state:
    init_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    st.session_state.current_session_id = init_id
    st.session_state.chat_sessions[init_id] = {"title": "New Conversation", "messages": []}

current_id = st.session_state.current_session_id
messages = st.session_state.chat_sessions[current_id]["messages"]

# Per-chat-session Q&A history (used for follow-up resolution), keyed off current_id
if "qa_histories" not in st.session_state:
    st.session_state.qa_histories = {}
qa_history = st.session_state.qa_histories.setdefault(current_id, [])


# ---------------------------------------------------------------
# Helper: Interactive Chart Renderer (unchanged from your original)
# ---------------------------------------------------------------
def display_chart_tab(df: pd.DataFrame, key_prefix: str = ""):
    if df is None or len(df.columns) < 2:
        st.info("Need at least 2 columns to render a chart.")
        return

    all_cols = list(df.columns)
    col1, col2, col3 = st.columns(3)
    x_key = f"{key_prefix}_x" if key_prefix else "x_axis"
    y_key = f"{key_prefix}_y" if key_prefix else "y_axis"
    t_key = f"{key_prefix}_type" if key_prefix else "chart_type"
    x_col = col1.selectbox("Dimension (X-axis)", all_cols, index=0, key=x_key)
    remaining_cols = [c for c in all_cols if c != x_col]
    y_col = col2.selectbox("Metric (Y-axis)", remaining_cols, index=0 if remaining_cols else 0, key=y_key)
    chart_type = col3.selectbox("Chart Type", ["Bar Chart", "Line Chart", "Area Chart", "Scatter Plot"], key=t_key)

    chart_df = df.copy()
    if any(k in x_col.lower() for k in ["year", "quarter", "month", "day", "date"]):
        chart_df[x_col] = chart_df[x_col].apply(lambda x: str(int(x)) if pd.notnull(x) and isinstance(x, (int, float)) else str(x))

    if chart_type == "Bar Chart":
        st.bar_chart(chart_df.set_index(x_col)[y_col])
    elif chart_type == "Line Chart":
        st.line_chart(chart_df.set_index(x_col)[y_col])
    elif chart_type == "Area Chart":
        st.area_chart(chart_df.set_index(x_col)[y_col])
    elif chart_type == "Scatter Plot":
        st.scatter_chart(chart_df, x=x_col, y=y_col)


# ---------------------------------------------------------------
# REPLACES your old hardcoded generate_sql_from_prompt().
# Handles ANY question — routes to an uploaded file if one is active,
# otherwise runs schema-grounded Cortex SQL generation against your
# real GOLD warehouse tables.
# ---------------------------------------------------------------
def answer_any_question(user_prompt: str):
    p = user_prompt.lower().strip()

    # Lightweight greeting/help handling (kept cheap — no Cortex call needed)
    if any(greet == p or greet in p for greet in GREETING_PATTERNS[:4]):
        return ("I'm doing well, thank you! I can analyze inventory levels, stockouts, "
                "warehouses, product categories — or answer questions about anything you upload. "
                "What would you like to explore?"), None, None
    if p in ["hi", "hello", "hey", "good morning", "good evening"]:
        return ("Hello! I'm your Inventory Intelligence Assistant. Ask me anything about stock, "
                "warehouses, products, supply — or upload a file and ask about that instead."), None, None
    if any(h in p for h in ["what can i ask", "what questions", "what can you do", "examples"]):
        return ("Ask me anything about your inventory data in plain English — I'm not limited to a fixed "
                "list of questions anymore. For example: \"what's the total inventory value in the "
                "electronics category\", \"which warehouse has the most stockouts\", \"how much excess "
                "stock do we have in the west region\". You can also upload a CSV, Excel, PDF, or Word "
                "file in the sidebar and ask questions about that specific document instead."), None, None

    active_file = st.session_state.get("active_file")

    # --- Route 1: an uploaded file is active -> answer from that file ---
    if active_file:
        if active_file["kind"] == "structured":
            result = answer_structured_question(user_prompt, active_file, session, history=qa_history)
        else:
            result = answer_document_question(user_prompt, active_file, session, history=qa_history)
        qa_history.append({"question": user_prompt, "answer": result["answer"]})
        return result["answer"], result["sql"], result["result"]

    # --- Route 2: no file -> query the real warehouse schema via Cortex ---
    if not any(word in p for word in DOMAIN_KEYWORDS):
        return ("I'm specialized in inventory data. I don't have general web knowledge — "
                "ask me about stock, warehouses, products, or supply, or upload a document "
                "in the sidebar to ask about that instead."), None, None

    result = generate_and_run_warehouse_sql(
        user_prompt, session, WAREHOUSE_SCHEMA_DESCRIPTION, history=qa_history
    )
    qa_history.append({"question": user_prompt, "answer": result["answer"]})
    return result["answer"], result["sql"], result["result"]


# ----------------- LEFT NATIVE SIDEBAR PANEL -----------------
with st.sidebar:
    st.markdown("### ⚡Dilytics AI")
    st.markdown('<span class="status-pill">● Semantic Mart Live</span>', unsafe_allow_html=True)
    st.write("")

    if st.button("➕ New Chat", use_container_width=True, type="primary"):
        new_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        st.session_state.current_session_id = new_id
        st.session_state.chat_sessions[new_id] = {"title": f"Chat {len(st.session_state.chat_sessions) + 1}", "messages": []}
        st.rerun()

    st.markdown("---")
    st.markdown("##### 🕒 Recent Conversations")
    for s_id, s_data in reversed(list(st.session_state.chat_sessions.items())):
        is_active = (s_id == st.session_state.current_session_id)
        session_label = s_data["title"]
        if len(session_label) > 20:
            session_label = session_label[:18] + "..."
        if st.button(f"{'👉 ' if is_active else '🗨️ '}{session_label}", key=f"sess_{s_id}", use_container_width=True):
            st.session_state.current_session_id = s_id
            st.rerun()

    st.markdown("---")
    if st.button("🗑️ Clear All Sessions", use_container_width=True):
        st.session_state.chat_sessions = {}
        st.session_state.qa_histories = {}
        init_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        st.session_state.current_session_id = init_id
        st.session_state.chat_sessions[init_id] = {"title": "New Conversation", "messages": []}
        st.rerun()

    # -------------------- NEW: Document Analysis upload --------------------
    st.markdown("---")
    st.markdown("##### 📂 Document Analysis")
    st.caption("Upload a document or report to ask questions about it instead of the warehouse.")
    uploaded_file = st.file_uploader(
        "Upload",
        type=["csv", "xlsx", "xls", "pdf", "docx"],
        label_visibility="collapsed",
        help="200MB per file • CSV, XLSX, XLS, PDF, DOCX",
    )

    if uploaded_file is not None:
        if st.session_state.get("active_file_name") != uploaded_file.name:
            with st.spinner(f"Reading {uploaded_file.name}..."):
                try:
                    file_meta = ingest_uploaded_file(uploaded_file, session)
                    st.session_state["active_file"] = file_meta
                    st.session_state["active_file_name"] = uploaded_file.name
                    st.session_state.qa_histories[current_id] = []  # reset follow-up context for new file
                    st.success(f"Loaded {uploaded_file.name}")
                except Exception as e:
                    st.error(f"Couldn't read that file: {e}")

    active_file = st.session_state.get("active_file")
    if active_file:
        st.info(f"📄 Active file: **{active_file['name']}**\n\nQuestions will be answered from this file.")
        if active_file["kind"] == "structured":
            st.caption(f"{active_file['row_count']} rows • columns: {', '.join(active_file['columns'][:6])}"
                       + ("..." if len(active_file["columns"]) > 6 else ""))
        if st.button("✖ Clear uploaded file", use_container_width=True):
            st.session_state.pop("active_file", None)
            st.session_state.pop("active_file_name", None)
            st.rerun()


# ----------------- MAIN CHAT & ANALYTICS AREA -----------------
head_col1, head_col2 = st.columns([4.5, 1.2])
with head_col1:
    st.title("💬 Dilytics Inventory AI")
    st.caption("Ask questions in natural language to explore stock levels, warehouse capacity, and product segments — or upload a file to ask about that instead.")
with head_col2:
    st.write("")
    if st.button("🔄 Reset Thread", use_container_width=True, help="Clear message history in this specific thread"):
        st.session_state.chat_sessions[current_id]["messages"] = []
        st.session_state.chat_sessions[current_id]["title"] = "New Conversation"
        st.session_state.qa_histories[current_id] = []
        st.rerun()


with st.expander("💡 What can I ask this assistant?", expanded=False):
    st.markdown(
        "This assistant now answers **any** question about your inventory data in plain English — "
        "it's not limited to a fixed list. A few examples to get you started:"
    )
    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown("""
        **💰 Inventory Value & Quantity**
        * "What is the total available inventory value?"
        * "What is the inventory value by warehouse?"
        * "Which product category holds the most value?"
        **📦 Products & Categories**
        * "Top 10 products by inventory value"
        * "Inventory value by brand for the East warehouse"
        """)
    with col_b:
        st.markdown("""
        **⚠️ Stockouts & Exceptions**
        * "How many products are out of stock?"
        * "Which warehouse has the highest excess stock value?"
        * "How many products need reordering in the Electronics category?"
        **📄 Or upload a file** (sidebar) and ask about that document specifically.
        """)


# Onboarding / Verified Questions Selector (kept as quick-start shortcuts, not the only options)
st.markdown("##### 💡 Quick Questions:")
q_col1, q_col2, q_col3, q_col4, q_col5 = st.columns(5)
quick_prompt = None
if q_col1.button("💰 Total Inv. Value", use_container_width=True):
    quick_prompt = "What is the total available inventory value?"
if q_col2.button("🏭 Value by Warehouse", use_container_width=True):
    quick_prompt = "What is the inventory value by warehouse?"
if q_col3.button("📦 Value by Category", use_container_width=True):
    quick_prompt = "What is the inventory value by product category?"
if q_col4.button("📉 Stockout Count", use_container_width=True):
    quick_prompt = "How many products are out of stock?"
if q_col5.button("⚠️ Excess Stock", use_container_width=True):
    quick_prompt = "What is the total excess inventory value by warehouse?"

# Display Current Thread's History
for idx, msg in enumerate(messages):
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])
        if msg.get("sql"):
            with st.expander("Generated SQL", expanded=False):
                st.code(msg["sql"], language="sql")
        if msg.get("data") is not None:
            tab_data, tab_chart = st.tabs(["Data 📄", "Chart 📈"])
            with tab_data:
                st.dataframe(msg["data"])
            with tab_chart:
                display_chart_tab(msg["data"], key_prefix=f"hist_{current_id}_{idx}")

# Handle Inputs
placeholder = "Ask a question about your uploaded document..." if st.session_state.get("active_file") \
    else "Ask a question about inventory, warehouses, products, or stockouts..."
user_prompt = st.chat_input(placeholder) or quick_prompt

if user_prompt:
    if len(messages) == 0:
        st.session_state.chat_sessions[current_id]["title"] = user_prompt[:25] + ("..." if len(user_prompt) > 25 else "")

    messages.append({"role": "user", "content": user_prompt})
    with st.chat_message("user"):
        st.markdown(user_prompt)

    with st.chat_message("assistant"):
        with st.spinner("Thinking..."):
            explanation, sql_query, df = answer_any_question(user_prompt)

        st.markdown(explanation)
        if sql_query:
            with st.expander("Generated SQL", expanded=False):
                st.code(sql_query, language="sql")
        if df is not None:
            tab_data, tab_chart = st.tabs(["Data 📄", "Chart 📈"])
            with tab_data:
                st.dataframe(df)
            with tab_chart:
                display_chart_tab(df, key_prefix=f"live_{current_id}")

        messages.append({"role": "assistant", "content": explanation, "sql": sql_query, "data": df})
        st.rerun()
