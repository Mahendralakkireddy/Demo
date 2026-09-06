import streamlit as st
import pandas as pd
from datetime import datetime
import snowflake.connector
from snowflake.snowpark import Session
import yaml
 
# ===================================================================
# Configuration
# ===================================================================
HOST = "WDSDGTL-XCC29288.snowflakecomputing.com" 
ACCOUNT = "WDSDGTL-XCC29288"
DATABASE = "INVENTORY_DW_DEMO"
SCHEMA = "GOLD"
WAREHOUSE = "COMPUTE_WH"

# Semantic Model Stage Paths (Two Databases for Two YML)
INVENTORY_YAML_STAGE_PATH = '@"INVENTORY_DW_DEMO"."INVENTORY_SCHEMA"."YAML"/INV_ANALYST_DEMO_90_VERIFIED.yaml'
SALES_YAML_STAGE_PATH = '@"CORTEX_DEMO"."CORTEX_SCHEMA"."YAML"/sales_intelligence_model_80_queries_fixed.yaml'
 
# Page Configuration
st.set_page_config(page_title="Dilytics Enterprise AI", page_icon="📦", layout="wide")
 
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
    st.title("Welcome to Dilytics Enterprise AI")
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
# 2. DYNAMIC DUAL YAML STAGE LOADER
# ===================================================================
session = st.session_state.snowpark_session

def load_yaml_queries(stage_path):
    """Dynamically reads a YAML file from a Snowflake Internal Stage"""
    try:
        stream = session.file.get_stream(stage_path)
        data = yaml.safe_load(stream)
        if isinstance(data, dict):
            return data.get("verified_queries", [])
        return []
    except Exception as e:
        st.error(f"🚨 **Syntax Error in your YAML File!**\n\nCould not load `{stage_path}`. Please fix the formatting/indentation in Snowflake!\n\nError details: `{e}`")
        return []

# We use session_state instead of st.cache to ensure it doesn't get permanently stuck on errors!
if "sales_queries" not in st.session_state:
    st.session_state.sales_queries = load_yaml_queries(SALES_YAML_STAGE_PATH)
    
if "inventory_queries" not in st.session_state:
    st.session_state.inventory_queries = load_yaml_queries(INVENTORY_YAML_STAGE_PATH)

def find_sql_for_prompt(prompt: str, queries_list: list, schema_prefix: str):
    """Parses a loaded YAML list to find a matching question"""
    if not queries_list:
        return None, None
        
    p = prompt.lower().strip().replace('?', '')
    
    for q in queries_list:
        yaml_q = q.get('question', '').lower().replace('?', '').strip()
        
        if p == yaml_q or p in yaml_q or yaml_q in p:
            sql = q.get('sql', '')
            # Convert Semantic table references (__) to actual database schema
            sql = sql.replace('__', schema_prefix)
            return q.get('question', 'Query Found'), sql
            
    return None, None


# ===================================================================
# 3. ORIGINAL HARDCODED INVENTORY ENGINE (Safety Net Fallback)
# ===================================================================
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
 
def generate_hardcoded_inventory_sql(prompt: str):
    p = prompt.lower().strip()
    if any(greet in p for greet in ["how are you", "how's it going", "what's up", "whats up"]):
        explanation = "I'm doing well, thank you! I am ready to help you analyze inventory levels, stockouts, warehouses, and product categories. What metric would you like to explore?"
        return explanation, None
 
    elif any(help_word in p for help_word in ["what can i ask", "what questions", "what can you do", "examples", "help"]):
        explanation = "You can ask me questions about your inventory data or sales data! Look at the tabs above for examples."
        return explanation, None
 
    elif p in ["hi", "hello", "hey", "good morning", "good evening"]:
        explanation = "Hello! I am your Intelligence Assistant powered by your semantic data model. Ask any question about stock, warehouses, products, or sales!"
        return explanation, None

    if "total available inventory" in p or ("inventory value" in p and "warehouse" not in p and "category" not in p and "brand" not in p):
        explanation = "Calculating total inventory value across all warehouses as of the latest snapshot."
        sql = """
        SELECT SUM(INVENTORY_VALUE_AMT) AS TOTAL_INVENTORY_VALUE
        FROM INVENTORY_DW_DEMO.GOLD.FACT_INVENTORY_DAILY_SNAPSHOT
        WHERE SNAPSHOT_DATE_KEY = (SELECT MAX(SNAPSHOT_DATE_KEY) FROM INVENTORY_DW_DEMO.GOLD.FACT_INVENTORY_DAILY_SNAPSHOT)
        """
        return explanation, sql.strip()
 
    elif "quantity" in p and "on hand" in p and "product" not in p:
        explanation = "Calculating the total physical quantity of inventory currently on hand."
        sql = """
        SELECT SUM(ON_HAND_QTY) AS TOTAL_ON_HAND_QTY
        FROM INVENTORY_DW_DEMO.GOLD.FACT_INVENTORY_DAILY_SNAPSHOT
        WHERE SNAPSHOT_DATE_KEY = (SELECT MAX(SNAPSHOT_DATE_KEY) FROM INVENTORY_DW_DEMO.GOLD.FACT_INVENTORY_DAILY_SNAPSHOT)
        """
        return explanation, sql.strip()
 
    elif "inventory value by warehouse" in p:
        explanation = "Aggregating total inventory value grouped by warehouse location."
        sql = """
        SELECT w.WAREHOUSE_NAME, SUM(f.INVENTORY_VALUE_AMT) AS INVENTORY_VALUE
        FROM INVENTORY_DW_DEMO.GOLD.FACT_INVENTORY_DAILY_SNAPSHOT f
        JOIN INVENTORY_DW_DEMO.GOLD.DIM_WAREHOUSE w ON f.WAREHOUSE_KEY = w.WAREHOUSE_KEY
        WHERE f.SNAPSHOT_DATE_KEY = (SELECT MAX(SNAPSHOT_DATE_KEY) FROM INVENTORY_DW_DEMO.GOLD.FACT_INVENTORY_DAILY_SNAPSHOT)
        GROUP BY w.WAREHOUSE_NAME ORDER BY INVENTORY_VALUE DESC
        """
        return explanation, sql.strip()
 
    elif "inventory value by product category" in p or "by category" in p:
        explanation = "Aggregating inventory value by product category."
        sql = """
        SELECT p.CATEGORY_NAME, SUM(f.INVENTORY_VALUE_AMT) AS TOTAL_INVENTORY_VALUE
        FROM INVENTORY_DW_DEMO.GOLD.FACT_INVENTORY_DAILY_SNAPSHOT f
        JOIN INVENTORY_DW_DEMO.GOLD.DIM_PRODUCT p ON f.PRODUCT_KEY = p.PRODUCT_KEY
        WHERE f.SNAPSHOT_DATE_KEY = (SELECT MAX(SNAPSHOT_DATE_KEY) FROM INVENTORY_DW_DEMO.GOLD.FACT_INVENTORY_DAILY_SNAPSHOT)
        GROUP BY p.CATEGORY_NAME ORDER BY TOTAL_INVENTORY_VALUE DESC
        """
        return explanation, sql.strip()
 
    elif "subcategory" in p:
        explanation = "Aggregating inventory value by product subcategory."
        sql = """
        SELECT p.SUBCATEGORY_NAME, SUM(f.INVENTORY_VALUE_AMT) AS TOTAL_INVENTORY_VALUE
        FROM INVENTORY_DW_DEMO.GOLD.FACT_INVENTORY_DAILY_SNAPSHOT f
        JOIN INVENTORY_DW_DEMO.GOLD.DIM_PRODUCT p ON f.PRODUCT_KEY = p.PRODUCT_KEY
        WHERE f.SNAPSHOT_DATE_KEY = (SELECT MAX(SNAPSHOT_DATE_KEY) FROM INVENTORY_DW_DEMO.GOLD.FACT_INVENTORY_DAILY_SNAPSHOT)
        GROUP BY p.SUBCATEGORY_NAME ORDER BY TOTAL_INVENTORY_VALUE DESC
        """
        return explanation, sql.strip()
 
    elif "brand" in p:
        explanation = "Aggregating inventory value by product brand."
        sql = """
        SELECT p.BRAND_NAME, SUM(f.INVENTORY_VALUE_AMT) AS TOTAL_INVENTORY_VALUE
        FROM INVENTORY_DW_DEMO.GOLD.FACT_INVENTORY_DAILY_SNAPSHOT f
        JOIN INVENTORY_DW_DEMO.GOLD.DIM_PRODUCT p ON f.PRODUCT_KEY = p.PRODUCT_KEY
        WHERE f.SNAPSHOT_DATE_KEY = (SELECT MAX(SNAPSHOT_DATE_KEY) FROM INVENTORY_DW_DEMO.GOLD.FACT_INVENTORY_DAILY_SNAPSHOT)
        GROUP BY p.BRAND_NAME ORDER BY TOTAL_INVENTORY_VALUE DESC
        """
        return explanation, sql.strip()
 
    elif "stockout" in p or "out of stock" in p:
        if "warehouse" in p:
            explanation = "Calculating the number of stockouts organized by warehouse."
            sql = """
            SELECT w.WAREHOUSE_NAME, COUNT_IF(f.IS_STOCKOUT_FLAG) AS STOCKOUT_COUNT
            FROM INVENTORY_DW_DEMO.GOLD.FACT_INVENTORY_DAILY_SNAPSHOT f
            JOIN INVENTORY_DW_DEMO.GOLD.DIM_WAREHOUSE w ON f.WAREHOUSE_KEY = w.WAREHOUSE_KEY
            WHERE f.SNAPSHOT_DATE_KEY = (SELECT MAX(SNAPSHOT_DATE_KEY) FROM INVENTORY_DW_DEMO.GOLD.FACT_INVENTORY_DAILY_SNAPSHOT)
            GROUP BY w.WAREHOUSE_NAME ORDER BY STOCKOUT_COUNT DESC
            """
        else:
            explanation = "Counting how many products are completely out of stock."
            sql = """
            SELECT COUNT(*) AS STOCKOUT_COUNT
            FROM INVENTORY_DW_DEMO.GOLD.FACT_INVENTORY_DAILY_SNAPSHOT
            WHERE SNAPSHOT_DATE_KEY = (SELECT MAX(SNAPSHOT_DATE_KEY) FROM INVENTORY_DW_DEMO.GOLD.FACT_INVENTORY_DAILY_SNAPSHOT)
            AND IS_STOCKOUT_FLAG = TRUE
            """
        return explanation, sql.strip()
 
    elif "excess" in p and "warehouse" in p:
        explanation = "Aggregating the financial value of excess stock held above safety buffers by warehouse."
        sql = """
        SELECT w.WAREHOUSE_NAME, SUM(f.EXCESS_STOCK_VALUE_AMT) AS TOTAL_EXCESS_VALUE
        FROM INVENTORY_DW_DEMO.GOLD.FACT_INVENTORY_DAILY_SNAPSHOT f
        JOIN INVENTORY_DW_DEMO.GOLD.DIM_WAREHOUSE w ON f.WAREHOUSE_KEY = w.WAREHOUSE_KEY
        WHERE f.SNAPSHOT_DATE_KEY = (SELECT MAX(SNAPSHOT_DATE_KEY) FROM INVENTORY_DW_DEMO.GOLD.FACT_INVENTORY_DAILY_SNAPSHOT)
        GROUP BY w.WAREHOUSE_NAME ORDER BY TOTAL_EXCESS_VALUE DESC
        """
        return explanation, sql.strip()
 
    elif "top 10" in p and "inventory value" in p:
        explanation = "Ranking the top 10 products carrying the highest inventory value."
        sql = """
        SELECT p.PRODUCT_SKU, p.PRODUCT_NAME, SUM(f.INVENTORY_VALUE_AMT) AS INVENTORY_VALUE
        FROM INVENTORY_DW_DEMO.GOLD.FACT_INVENTORY_DAILY_SNAPSHOT f
        JOIN INVENTORY_DW_DEMO.GOLD.DIM_PRODUCT p ON f.PRODUCT_KEY = p.PRODUCT_KEY
        WHERE f.SNAPSHOT_DATE_KEY = (SELECT MAX(SNAPSHOT_DATE_KEY) FROM INVENTORY_DW_DEMO.GOLD.FACT_INVENTORY_DAILY_SNAPSHOT)
        GROUP BY p.PRODUCT_SKU, p.PRODUCT_NAME ORDER BY INVENTORY_VALUE DESC LIMIT 10
        """
        return explanation, sql.strip()
 
    elif "reorder" in p:
        explanation = "Counting products that have fallen below their reorder threshold."
        sql = """
        SELECT COUNT(*) AS REORDER_NEEDED_COUNT
        FROM INVENTORY_DW_DEMO.GOLD.FACT_INVENTORY_DAILY_SNAPSHOT
        WHERE SNAPSHOT_DATE_KEY = (SELECT MAX(SNAPSHOT_DATE_KEY) FROM INVENTORY_DW_DEMO.GOLD.FACT_INVENTORY_DAILY_SNAPSHOT)
        AND IS_REORDER_NEEDED_FLAG = TRUE
        """
        return explanation, sql.strip()
 
    elif "abc" in p:
        explanation = "Evaluating inventory value across ABC classification tiers."
        sql = """
        SELECT p.ABC_CLASSIFICATION, SUM(f.INVENTORY_VALUE_AMT) AS TOTAL_INVENTORY_VALUE
        FROM INVENTORY_DW_DEMO.GOLD.FACT_INVENTORY_DAILY_SNAPSHOT f
        JOIN INVENTORY_DW_DEMO.GOLD.DIM_PRODUCT p ON f.PRODUCT_KEY = p.PRODUCT_KEY
        WHERE f.SNAPSHOT_DATE_KEY = (SELECT MAX(SNAPSHOT_DATE_KEY) FROM INVENTORY_DW_DEMO.GOLD.FACT_INVENTORY_DAILY_SNAPSHOT)
        GROUP BY p.ABC_CLASSIFICATION ORDER BY p.ABC_CLASSIFICATION
        """
        return explanation, sql.strip()
 
    domain_keywords = [
        "inventory", "warehouse", "product", "stock", "stockout", "excess", "quarantine", "reorder", 
        "category", "subcategory", "brand", "abc", "hazardous", "perishable", "cold-chain", "sku", "supply", "quantity"
    ]
    if not any(word in p for word in domain_keywords):
        explanation = "I don't have a semantic mapping for that specific query yet. Please try asking exactly as listed in the Verified Questions!"
        return explanation, None
 
    else:
        explanation = "Displaying a recent snapshot overview of inventory by product and warehouse:"
        sql = """
        SELECT 
            d.FULL_DATE, p.PRODUCT_NAME, w.WAREHOUSE_NAME, f.ON_HAND_QTY, f.INVENTORY_VALUE_AMT, f.IS_STOCKOUT_FLAG
        FROM INVENTORY_DW_DEMO.GOLD.FACT_INVENTORY_DAILY_SNAPSHOT f
        JOIN INVENTORY_DW_DEMO.GOLD.DIM_DATE d ON f.SNAPSHOT_DATE_KEY = d.DATE_KEY
        JOIN INVENTORY_DW_DEMO.GOLD.DIM_PRODUCT p ON f.PRODUCT_KEY = p.PRODUCT_KEY
        JOIN INVENTORY_DW_DEMO.GOLD.DIM_WAREHOUSE w ON f.WAREHOUSE_KEY = w.WAREHOUSE_KEY
        WHERE f.SNAPSHOT_DATE_KEY = (SELECT MAX(SNAPSHOT_DATE_KEY) FROM INVENTORY_DW_DEMO.GOLD.FACT_INVENTORY_DAILY_SNAPSHOT)
        ORDER BY f.INVENTORY_VALUE_AMT DESC
        LIMIT 20
        """
        return explanation, sql.strip()

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
 
# ----------------- MAIN CHAT & ANALYTICS AREA -----------------
head_col1, head_col2 = st.columns([4.5, 1.2])
with head_col1:
    st.title("💬 Dilytics Enterprise AI")
    st.caption("Ask questions in natural language to explore Inventory and Sales.")
with head_col2:
    st.write("")
    if st.button("🔄 Reset Thread", use_container_width=True, help="Clear message history"):
        st.session_state.chat_sessions[current_id]["messages"] = []
        st.session_state.chat_sessions[current_id]["title"] = "New Conversation"
        st.rerun()
 
# ===================================================================
# 4. NEW DEDICATED TABS UI
# ===================================================================
quick_prompt = None

tab_inv, tab_sales = st.tabs(["📦 Inventory Intelligence", "💰 Sales Intelligence"])

with tab_inv:
    with st.expander("💡 What exact questions can I ask about Inventory?", expanded=False):
        st.markdown("""
        *(Powered by your `INVENTORY_ANALYST.yaml` and semantic rules)*
        * "What is the total available inventory value?"
        * "What is the total quantity of inventory currently on hand?"
        * "What is the inventory value by warehouse?"
        * "What is the inventory value by product category?"
        * "How many products are out of stock?"
        * "What is the total excess inventory value by warehouse?"
        """)
    
    st.markdown("##### 💡 Verified Inventory Questions:")
    q_col1, q_col2, q_col3, q_col4, q_col5 = st.columns(5)
    if q_col1.button("💰 Total Inv. Value", use_container_width=True, key='i1'): quick_prompt = "What is the total available inventory value?"
    if q_col2.button("🏭 Value by Warehouse", use_container_width=True, key='i2'): quick_prompt = "What is the inventory value by warehouse?"
    if q_col3.button("📦 Value by Category", use_container_width=True, key='i3'): quick_prompt = "What is the inventory value by product category?"
    if q_col4.button("📉 Stockout Count", use_container_width=True, key='i4'): quick_prompt = "How many products are out of stock?"
    if q_col5.button("⚠️ Excess Stock", use_container_width=True, key='i5'): quick_prompt = "What is the total excess inventory value by warehouse?"

with tab_sales:
    with st.expander("💡 What exact questions can I ask about Sales?", expanded=False):
        st.markdown("""
        *(Automatically powered by your `Sales Intelligence Model.yaml`)*
        * "What is the total sales amount?"
        * "What are the total sales by customer?"
        * "What are the top products by sales?"
        * "What are total sales by customer region?"
        * "What are total sales by month?"
        """)
        
    st.markdown("##### 💡 Verified Sales Questions:")
    s_col1, s_col2, s_col3, s_col4, s_col5 = st.columns(5)
    if s_col1.button("💵 Total Sales", use_container_width=True, key='s1'): quick_prompt = "What is the total sales amount?"
    if s_col2.button("🏆 Top Products", use_container_width=True, key='s2'): quick_prompt = "What are the top products by sales?"
    if s_col3.button("🌍 Sales by Region", use_container_width=True, key='s3'): quick_prompt = "What are total sales by customer region?"
    if s_col4.button("📅 Monthly Sales", use_container_width=True, key='s4'): quick_prompt = "What are total sales by month?"
    if s_col5.button("📊 Sales by Channel", use_container_width=True, key='s5'): quick_prompt = "What is total sales by order channel?"

st.markdown("---")

# ===================================================================
# 5. UNIFIED TRIPLE-LAYER CHAT EXECUTION
# ===================================================================
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
 
user_prompt = st.chat_input("Ask a question about inventory, warehouses, sales, or customers...") or quick_prompt
 
if user_prompt:
    if len(messages) == 0:
        st.session_state.chat_sessions[current_id]["title"] = user_prompt[:25] + ("..." if len(user_prompt) > 25 else "")
 
    messages.append({"role": "user", "content": user_prompt})
    with st.chat_message("user"):
        st.markdown(user_prompt)
 
    with st.chat_message("assistant"):
        
        explanation = None
        sql_query = None

        # LAYER 1: Check Sales YAML Model
        matched_q, sql_query = find_sql_for_prompt(user_prompt, st.session_state.sales_queries, "CORTEX_DEMO.MART.")
        if sql_query:
            explanation = f"**Sales Domain:** Querying data based on Semantic Rule: '{matched_q}'"
            
        # LAYER 2: Check Inventory YAML Model
        if not sql_query:
            matched_q, sql_query = find_sql_for_prompt(user_prompt, st.session_state.inventory_queries, "INVENTORY_DW_DEMO.GOLD.")
            if sql_query:
                explanation = f"**Inventory Domain:** Querying data based on Semantic Rule: '{matched_q}'"
                
        # LAYER 3: Fallback to original hardcoded rules
        if not sql_query:
            explanation, sql_query = generate_hardcoded_inventory_sql(user_prompt)
            
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
 
        messages.append({
            "role": "assistant",
            "content": explanation,
            "sql": sql_query,
            "data": df
        })
        st.rerun()
