import streamlit as st
import pandas as pd
from datetime import datetime
import snowflake.connector
from snowflake.snowpark import Session
import io
import PyPDF2

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
                # Create the Snowpark session
                st.session_state.snowpark_session = Session.builder.configs({"connection": conn}).create()
                st.session_state.authenticated = True
                st.rerun()
        except Exception as e:
            st.error(f"Authentication failed: {e}")
            
    # Stop execution here until the user logs in
    st.stop()


# ===================================================================
# 2. MAIN APP LOGIC (Runs only after login)
# ===================================================================

# Get the authenticated session
session = st.session_state.snowpark_session

# Session State Management for Multi-Chat History
if "chat_sessions" not in st.session_state:
    st.session_state.chat_sessions = {}
if "current_session_id" not in st.session_state:
    init_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    st.session_state.current_session_id = init_id
    st.session_state.chat_sessions[init_id] = {
        "title": "New Conversation",
        "messages": []
    }

current_id = st.session_state.current_session_id
messages = st.session_state.chat_sessions[current_id]["messages"]

# Helper: Interactive Chart Renderer
def display_chart_tab(df: pd.DataFrame, key_prefix: str = ""):
    if len(df.columns) < 2:
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

# YAML Verified Query Matcher & Rule-Based SQL Engine
def generate_sql_from_prompt(prompt: str):
    p = prompt.lower().strip()
    
    if any(greet in p for greet in ["how are you", "how's it going", "what's up"]):
        return "I'm doing well, thank you! I am ready to help you analyze inventory levels, stockouts, warehouses, and product categories. What metric would you like to explore?", None

    elif any(help_word in p for help_word in ["what can i ask", "what questions", "what can you do", "examples", "help"]):
        return ("You can ask me questions about your inventory data! Here are some exact questions you can try:\n\n**Inventory Value:**\n- What is the total available inventory value?\n- What is the inventory value by warehouse?\n- What is the inventory value by product category?\n\n**Stock & Reordering:**\n- How many products are out of stock?\n- What is the total excess inventory value by warehouse?\n- How many products need to be reordered?\n\n*(You can also open the Lightbulb drop-down menu above for the full list!)*"), None

    elif p in ["hi", "hello", "hey", "good morning", "good evening"]:
        return "Hello! I am your Inventory Intelligence Assistant powered by your semantic data model. Ask any question about stock, warehouses, products, or supply!", None

    if "total available inventory" in p or ("inventory value" in p and "warehouse" not in p and "category" not in p and "brand" not in p):
        sql = "SELECT SUM(INVENTORY_VALUE_AMT) AS TOTAL_INVENTORY_VALUE FROM INVENTORY_DW.GOLD.FACT_INVENTORY_DAILY_SNAPSHOT WHERE SNAPSHOT_DATE_KEY = (SELECT MAX(SNAPSHOT_DATE_KEY) FROM INVENTORY_DW.GOLD.FACT_INVENTORY_DAILY_SNAPSHOT)"
        return "Calculating total inventory value across all warehouses as of the latest snapshot.", sql

    elif "quantity" in p and "on hand" in p and "product" not in p:
        sql = "SELECT SUM(ON_HAND_QTY) AS TOTAL_ON_HAND_QTY FROM INVENTORY_DW.GOLD.FACT_INVENTORY_DAILY_SNAPSHOT WHERE SNAPSHOT_DATE_KEY = (SELECT MAX(SNAPSHOT_DATE_KEY) FROM INVENTORY_DW.GOLD.FACT_INVENTORY_DAILY_SNAPSHOT)"
        return "Calculating the total physical quantity of inventory currently on hand.", sql

    elif "inventory value by warehouse" in p:
        sql = "SELECT w.WAREHOUSE_NAME, SUM(f.INVENTORY_VALUE_AMT) AS INVENTORY_VALUE FROM INVENTORY_DW.GOLD.FACT_INVENTORY_DAILY_SNAPSHOT f JOIN INVENTORY_DW.GOLD.DIM_WAREHOUSE w ON f.WAREHOUSE_KEY = w.WAREHOUSE_KEY WHERE f.SNAPSHOT_DATE_KEY = (SELECT MAX(SNAPSHOT_DATE_KEY) FROM INVENTORY_DW.GOLD.FACT_INVENTORY_DAILY_SNAPSHOT) GROUP BY w.WAREHOUSE_NAME ORDER BY INVENTORY_VALUE DESC"
        return "Aggregating total inventory value grouped by warehouse location.", sql

    elif "inventory value by product category" in p or "by category" in p:
        sql = "SELECT p.CATEGORY_NAME, SUM(f.INVENTORY_VALUE_AMT) AS TOTAL_INVENTORY_VALUE FROM INVENTORY_DW.GOLD.FACT_INVENTORY_DAILY_SNAPSHOT f JOIN INVENTORY_DW.GOLD.DIM_PRODUCT p ON f.PRODUCT_KEY = p.PRODUCT_KEY WHERE f.SNAPSHOT_DATE_KEY = (SELECT MAX(SNAPSHOT_DATE_KEY) FROM INVENTORY_DW.GOLD.FACT_INVENTORY_DAILY_SNAPSHOT) GROUP BY p.CATEGORY_NAME ORDER BY TOTAL_INVENTORY_VALUE DESC"
        return "Aggregating inventory value by product category.", sql

    elif "subcategory" in p:
        sql = "SELECT p.SUBCATEGORY_NAME, SUM(f.INVENTORY_VALUE_AMT) AS TOTAL_INVENTORY_VALUE FROM INVENTORY_DW.GOLD.FACT_INVENTORY_DAILY_SNAPSHOT f JOIN INVENTORY_DW.GOLD.DIM_PRODUCT p ON f.PRODUCT_KEY = p.PRODUCT_KEY WHERE f.SNAPSHOT_DATE_KEY = (SELECT MAX(SNAPSHOT_DATE_KEY) FROM INVENTORY_DW.GOLD.FACT_INVENTORY_DAILY_SNAPSHOT) GROUP BY p.SUBCATEGORY_NAME ORDER BY TOTAL_INVENTORY_VALUE DESC"
        return "Aggregating inventory value by product subcategory.", sql

    elif "brand" in p:
        sql = "SELECT p.BRAND_NAME, SUM(f.INVENTORY_VALUE_AMT) AS TOTAL_INVENTORY_VALUE FROM INVENTORY_DW.GOLD.FACT_INVENTORY_DAILY_SNAPSHOT f JOIN INVENTORY_DW.GOLD.DIM_PRODUCT p ON f.PRODUCT_KEY = p.PRODUCT_KEY WHERE f.SNAPSHOT_DATE_KEY = (SELECT MAX(SNAPSHOT_DATE_KEY) FROM INVENTORY_DW.GOLD.FACT_INVENTORY_DAILY_SNAPSHOT) GROUP BY p.BRAND_NAME ORDER BY TOTAL_INVENTORY_VALUE DESC"
        return "Aggregating inventory value by product brand.", sql

    elif "stockout" in p or "out of stock" in p:
        if "warehouse" in p:
            sql = "SELECT w.WAREHOUSE_NAME, COUNT_IF(f.IS_STOCKOUT_FLAG) AS STOCKOUT_COUNT FROM INVENTORY_DW.GOLD.FACT_INVENTORY_DAILY_SNAPSHOT f JOIN INVENTORY_DW.GOLD.DIM_WAREHOUSE w ON f.WAREHOUSE_KEY = w.WAREHOUSE_KEY WHERE f.SNAPSHOT_DATE_KEY = (SELECT MAX(SNAPSHOT_DATE_KEY) FROM INVENTORY_DW.GOLD.FACT_INVENTORY_DAILY_SNAPSHOT) GROUP BY w.WAREHOUSE_NAME ORDER BY STOCKOUT_COUNT DESC"
            return "Calculating the number of stockouts organized by warehouse.", sql
        else:
            sql = "SELECT COUNT(*) AS STOCKOUT_COUNT FROM INVENTORY_DW.GOLD.FACT_INVENTORY_DAILY_SNAPSHOT WHERE SNAPSHOT_DATE_KEY = (SELECT MAX(SNAPSHOT_DATE_KEY) FROM INVENTORY_DW.GOLD.FACT_INVENTORY_DAILY_SNAPSHOT) AND IS_STOCKOUT_FLAG = TRUE"
            return "Counting how many products are completely out of stock.", sql

    elif "excess" in p and "warehouse" in p:
        sql = "SELECT w.WAREHOUSE_NAME, SUM(f.EXCESS_STOCK_VALUE_AMT) AS TOTAL_EXCESS_VALUE FROM INVENTORY_DW.GOLD.FACT_INVENTORY_DAILY_SNAPSHOT f JOIN INVENTORY_DW.GOLD.DIM_WAREHOUSE w ON f.WAREHOUSE_KEY = w.WAREHOUSE_KEY WHERE f.SNAPSHOT_DATE_KEY = (SELECT MAX(SNAPSHOT_DATE_KEY) FROM INVENTORY_DW.GOLD.FACT_INVENTORY_DAILY_SNAPSHOT) GROUP BY w.WAREHOUSE_NAME ORDER BY TOTAL_EXCESS_VALUE DESC"
        return "Aggregating the financial value of excess stock held above safety buffers by warehouse.", sql

    elif "top 10" in p and "inventory value" in p:
        sql = "SELECT p.PRODUCT_SKU, p.PRODUCT_NAME, SUM(f.INVENTORY_VALUE_AMT) AS INVENTORY_VALUE FROM INVENTORY_DW.GOLD.FACT_INVENTORY_DAILY_SNAPSHOT f JOIN INVENTORY_DW.GOLD.DIM_PRODUCT p ON f.PRODUCT_KEY = p.PRODUCT_KEY WHERE f.SNAPSHOT_DATE_KEY = (SELECT MAX(SNAPSHOT_DATE_KEY) FROM INVENTORY_DW.GOLD.FACT_INVENTORY_DAILY_SNAPSHOT) GROUP BY p.PRODUCT_SKU, p.PRODUCT_NAME ORDER BY INVENTORY_VALUE DESC LIMIT 10"
        return "Ranking the top 10 products carrying the highest inventory value.", sql

    elif "reorder" in p:
        sql = "SELECT COUNT(*) AS REORDER_NEEDED_COUNT FROM INVENTORY_DW.GOLD.FACT_INVENTORY_DAILY_SNAPSHOT WHERE SNAPSHOT_DATE_KEY = (SELECT MAX(SNAPSHOT_DATE_KEY) FROM INVENTORY_DW.GOLD.FACT_INVENTORY_DAILY_SNAPSHOT) AND IS_REORDER_NEEDED_FLAG = TRUE"
        return "Counting products that have fallen below their reorder threshold.", sql

    elif "abc" in p:
        sql = "SELECT p.ABC_CLASSIFICATION, SUM(f.INVENTORY_VALUE_AMT) AS TOTAL_INVENTORY_VALUE FROM INVENTORY_DW.GOLD.FACT_INVENTORY_DAILY_SNAPSHOT f JOIN INVENTORY_DW.GOLD.DIM_PRODUCT p ON f.PRODUCT_KEY = p.PRODUCT_KEY WHERE f.SNAPSHOT_DATE_KEY = (SELECT MAX(SNAPSHOT_DATE_KEY) FROM INVENTORY_DW.GOLD.FACT_INVENTORY_DAILY_SNAPSHOT) GROUP BY p.ABC_CLASSIFICATION ORDER BY p.ABC_CLASSIFICATION"
        return "Evaluating inventory value across ABC classification tiers.", sql

    domain_keywords = ["inventory", "warehouse", "product", "stock", "stockout", "excess", "quarantine", "reorder", "category", "subcategory", "brand", "abc", "hazardous", "perishable", "cold-chain", "sku", "supply", "quantity"]
    if not any(word in p for word in domain_keywords):
        return "I am specialized strictly as an **Inventory Domain Intelligence**.\n\nI don't have external web data to answer general knowledge or non-inventory queries. Please ask a question related to stock, warehouses, products, or supply!", None

    else:
        sql = "SELECT d.FULL_DATE, p.PRODUCT_NAME, w.WAREHOUSE_NAME, f.ON_HAND_QTY, f.INVENTORY_VALUE_AMT, f.IS_STOCKOUT_FLAG FROM INVENTORY_DW.GOLD.FACT_INVENTORY_DAILY_SNAPSHOT f JOIN INVENTORY_DW.GOLD.DIM_DATE d ON f.SNAPSHOT_DATE_KEY = d.DATE_KEY JOIN INVENTORY_DW.GOLD.DIM_PRODUCT p ON f.PRODUCT_KEY = p.PRODUCT_KEY JOIN INVENTORY_DW.GOLD.DIM_WAREHOUSE w ON f.WAREHOUSE_KEY = w.WAREHOUSE_KEY WHERE f.SNAPSHOT_DATE_KEY = (SELECT MAX(SNAPSHOT_DATE_KEY) FROM INVENTORY_DW.GOLD.FACT_INVENTORY_DAILY_SNAPSHOT) ORDER BY f.INVENTORY_VALUE_AMT DESC LIMIT 20"
        return "Displaying a recent snapshot overview of inventory by product and warehouse:", sql

# ----------------- LEFT NATIVE SIDEBAR PANEL -----------------
with st.sidebar:
    st.markdown("### ⚡Dilytics AI")
    st.markdown('<span class="status-pill">● Semantic Mart Live</span>', unsafe_allow_html=True)
    st.write("")
    
    if st.button("➕ New Chat", use_container_width=True, type="primary"):
        new_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        st.session_state.current_session_id = new_id
        st.session_state.chat_sessions[new_id] = {
            "title": f"Chat {len(st.session_state.chat_sessions) + 1}",
            "messages": []
        }
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
        init_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        st.session_state.current_session_id = init_id
        st.session_state.chat_sessions[init_id] = {
            "title": "New Conversation",
            "messages": []
        }
        st.rerun()

# ----------------- MAIN UI TABS -----------------
tab_inventory, tab_doc_ai = st.tabs(["📦 Inventory SQL Assistant", "📄 Document Intelligence AI"])

# ===================================================================
# TAB 1: EXISTING INVENTORY AI (Rule-Based SQL)
# ===================================================================
with tab_inventory:
    head_col1, head_col2 = st.columns([4.5, 1.2])
    with head_col1:
        st.title("💬 Dilytics Inventory AI")
        st.caption("Ask questions in natural language to explore stock levels, warehouse capacity, and product segments.")
    with head_col2:
        st.write("")
        if st.button("🔄 Reset Thread", use_container_width=True, help="Clear message history in this specific thread"):
            st.session_state.chat_sessions[current_id]["messages"] = []
            st.session_state.chat_sessions[current_id]["title"] = "New Conversation"
            st.rerun()

    with st.expander("💡 What exact questions can I ask this assistant?", expanded=False):
        st.markdown("This assistant is currently programmed to perfectly answer the following specific questions:")
        col_a, col_b = st.columns(2)
        with col_a:
            st.markdown("**💰 Inventory Value & Quantity**\n* What is the total available inventory value?\n* What is the total quantity of inventory currently on hand?\n* What is the inventory value by warehouse?\n\n**📦 Products & Categories**\n* What is the inventory value by product category?\n* What is the inventory value by product subcategory?\n* What is the inventory value by brand?\n* What are the top 10 products by inventory value?\n* What is the inventory value by ABC classification?")
        with col_b:
            st.markdown("**⚠️ Stockouts & Exceptions**\n* How many products are out of stock?\n* What is the stockout count by warehouse?\n* What is the total excess inventory value by warehouse?\n* How many products need to be reordered?")
        st.info("💡 **Pro-Tip:** You can copy and paste any of these exact questions directly into the chat bar below!")

    st.markdown("##### 💡 Verified Onboarding Questions:")
    q_col1, q_col2, q_col3, q_col4, q_col5 = st.columns(5)
    quick_prompt = None
    if q_col1.button("💰 Total Inv. Value", use_container_width=True): quick_prompt = "What is the total available inventory?"
    if q_col2.button("🏭 Value by Warehouse", use_container_width=True): quick_prompt = "What is the inventory value by warehouse?"
    if q_col3.button("📦 Value by Category", use_container_width=True): quick_prompt = "What is the inventory value by product category?"
    if q_col4.button("📉 Stockout Count", use_container_width=True): quick_prompt = "How many products are out of stock?"
    if q_col5.button("⚠️ Excess Stock", use_container_width=True): quick_prompt = "What is the total excess inventory value by warehouse?"

    for idx, msg in enumerate(messages):
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
            if "sql" in msg and msg["sql"]:
                with st.expander("Generated SQL", expanded=False):
                    st.code(msg["sql"], language="sql")
            if "data" in msg and msg["data"] is not None:
                tab_data, tab_chart = st.tabs(["Data 📄", "Chart 📈"])
                with tab_data:
                    st.dataframe(msg["data"])
                with tab_chart:
                    display_chart_tab(msg["data"], key_prefix=f"hist_{current_id}_{idx}")

    user_prompt = st.chat_input("Ask a question about inventory, warehouses, products, or stockouts...") or quick_prompt

    if user_prompt:
        if len(messages) == 0:
            st.session_state.chat_sessions[current_id]["title"] = user_prompt[:25] + ("..." if len(user_prompt) > 25 else "")

        messages.append({"role": "user", "content": user_prompt})
        with st.chat_message("user"):
            st.markdown(user_prompt)

        with st.chat_message("assistant"):
            explanation, sql_query = generate_sql_from_prompt(user_prompt)
            st.markdown(explanation)
            df = None
            if sql_query:
                with st.expander("Generated SQL", expanded=False):
                    st.code(sql_query, language="sql")
                try:
                    df = session.sql(sql_query).to_pandas()
                    tab_data, tab_chart = st.tabs(["Data 📄", "Chart 📈"])
                    with tab_data:
                        st.dataframe(df)
                    with tab_chart:
                        display_chart_tab(df, key_prefix=f"live_{current_id}")
                except Exception as e:
                    st.error(f"SQL Execution Error: {str(e)}")

            messages.append({"role": "assistant", "content": explanation, "sql": sql_query, "data": df})
            st.rerun()


# ===================================================================
# TAB 2: DOCUMENT & DATA ANALYSIS (Powered by Snowflake Cortex)
# ===================================================================
with tab_doc_ai:
    st.header("📄 Document & Data Analysis Assistant")
    st.markdown("Upload any Excel, CSV, PDF, or Text file (up to 100MB) to analyze it and ask questions using Snowflake Cortex LLM.")
    
    uploaded_file = st.file_uploader("Upload Document", type=["csv", "xlsx", "xls", "pdf", "txt"], help="Max size 100MB")
    
    if uploaded_file is not None:
        if st.button("Analyze Data", type="primary"):
            with st.spinner("Analyzing document..."):
                file_name = uploaded_file.name
                st.session_state.doc_type = file_name.split('.')[-1].lower()
                st.session_state.doc_name = file_name
                
                try:
                    if st.session_state.doc_type in ['csv']:
                        df = pd.read_csv(uploaded_file)
                        # Clean columns for Snowflake standard
                        df.columns = [str(c).upper().replace(" ", "_").replace("-", "_").replace("(", "").replace(")", "").replace("\n", "") for c in df.columns]
                        st.session_state.uploaded_df = df
                        # Upload to Snowflake temp table
                        session.write_pandas(df, "TEMP_UPLOADED_DOC", auto_create_table=True, table_type="temp", overwrite=True)
                        st.session_state.doc_ready = True
                        st.success(f"Successfully analyzed CSV with {len(df)} rows.")
                        
                    elif st.session_state.doc_type in ['xlsx', 'xls']:
                        xls = pd.ExcelFile(uploaded_file)
                        sheet_names = xls.sheet_names
                        df = pd.read_excel(uploaded_file, sheet_name=sheet_names[0])
                        # Clean columns for Snowflake standard
                        df.columns = [str(c).upper().replace(" ", "_").replace("-", "_").replace("(", "").replace(")", "").replace("\n", "") for c in df.columns]
                        st.session_state.uploaded_df = df
                        session.write_pandas(df, "TEMP_UPLOADED_DOC", auto_create_table=True, table_type="temp", overwrite=True)
                        st.session_state.doc_ready = True
                        st.success(f"Successfully analyzed Excel file (Sheet: {sheet_names[0]}) with {len(df)} rows.")
                        
                    elif st.session_state.doc_type == 'pdf':
                        pdf_reader = PyPDF2.PdfReader(uploaded_file)
                        text = ""
                        for page in pdf_reader.pages:
                            text += page.extract_text() + "\n"
                        st.session_state.uploaded_text = text
                        st.session_state.doc_ready = True
                        st.success(f"Successfully analyzed PDF ({len(pdf_reader.pages)} pages).")
                        
                    elif st.session_state.doc_type == 'txt':
                        text = uploaded_file.getvalue().decode("utf-8")
                        st.session_state.uploaded_text = text
                        st.session_state.doc_ready = True
                        st.success("Successfully analyzed Text file.")
                        
                except Exception as e:
                    st.error(f"Error processing file: {str(e)}")
                    st.session_state.doc_ready = False
                    
    # Document Chat Interface
    if st.session_state.get("doc_ready", False):
        st.markdown("---")
        st.subheader(f"Ask questions about: {st.session_state.doc_name}")
        
        # Preview Section
        with st.expander("Preview Uploaded Content"):
            if st.session_state.doc_type in ['csv', 'xlsx', 'xls']:
                st.dataframe(st.session_state.uploaded_df.head(10))
            else:
                preview_text = st.session_state.uploaded_text[:1000]
                st.text(preview_text + ("..." if len(st.session_state.uploaded_text) > 1000 else ""))
                
        doc_prompt = st.chat_input("Ask a question about the uploaded document...", key="doc_prompt_input")
        
        if doc_prompt:
            st.chat_message("user").markdown(doc_prompt)
            
            with st.chat_message("assistant"):
                with st.spinner("Analyzing document with Cortex AI..."):
                    try:
                        if st.session_state.doc_type in ['csv', 'xlsx', 'xls']:
                            # Structured Data -> Text-to-SQL generation using Cortex
                            columns = list(st.session_state.uploaded_df.columns)
                            cortex_prompt = f"""
                            You are a data analyst. I have a Snowflake table named TEMP_UPLOADED_DOC.
                            The columns are: {', '.join(columns)}.
                            Based on the user's question, write a Snowflake SQL query to answer it.
                            User Question: {doc_prompt}
                            IMPORTANT: Return ONLY the raw SQL query. Do not include markdown formatting like ```sql. Do not include any explanations.
                            """
                            
                            cortex_sql = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3-70b', $${cortex_prompt}$$)"
                            generated_sql = session.sql(cortex_sql).collect()[0][0].strip()
                            
                            # Clean up markdown if LLM includes it by accident
                            generated_sql = generated_sql.replace("```sql", "").replace("```", "").strip()
                            
                            st.markdown("Here is the data based on your question:")
                            with st.expander("View Generated Cortex SQL", expanded=False):
                                st.code(generated_sql, language="sql")
                            
                            # Execute the AI generated query
                            result_df = session.sql(generated_sql).to_pandas()
                            st.dataframe(result_df)
                            
                        else:
                            # Unstructured Data -> Question Answering using Cortex
                            # Limit text length to avoid token limits on Llama3 (safe limit ~25k chars)
                            safe_text = st.session_state.uploaded_text[:25000]
                            cortex_prompt = f"""
                            Based strictly on the following document content, answer the user's question.
                            If the answer is not in the document, say "I cannot find the answer in the document."
                            
                            DOCUMENT CONTENT:
                            {safe_text}
                            
                            USER QUESTION: {doc_prompt}
                            """
                            
                            cortex_sql = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3-70b', $${cortex_prompt}$$)"
                            answer = session.sql(cortex_sql).collect()[0][0].strip()
                            st.markdown(answer)
                            
                    except Exception as e:
                        st.error(f"Cortex AI failed to generate an answer: {str(e)}")