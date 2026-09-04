import streamlit as st
import pandas as pd
from datetime import datetime
import snowflake.connector
from snowflake.snowpark import Session
import yaml
 
# Configuration
HOST = "WDSDGTL-XCC29288.snowflakecomputing.com" 
ACCOUNT = "WDSDGTL-XCC29288"
DATABASE = "INVENTORY_DW_DEMO"
SCHEMA = "GOLD"
WAREHOUSE = "COMPUTE_WH"

# Semantic Model Stage Paths
INVENTORY_YAML_STAGE_PATH = '@"INVENTORY_DW_DEMO"."INVENTORY_SCHEMA"."YAML"/INVENTORY_ANALYST.yaml'
SALES_YAML_STAGE_PATH = '@"CORTEX_DEMO"."CORTEX_SCHEMA"."YAML"/Sales Intelligence Model.yaml'
 
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
# 1. STREAMLIT CLOUD LOGIN SCREEN (Original)
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
# 2. YAML STAGE LOADER & SALES ENGINE (NEW)
# ===================================================================
session = st.session_state.snowpark_session

@st.cache_data(show_spinner=False)
def load_sales_queries_from_stage(_session):
    """Dynamically reads the Sales YAML file from the Snowflake Internal Stage"""
    try:
        # get_stream safely reads the file from the Snowflake stage without downloading it locally
        stream = _session.file.get_stream(SALES_YAML_STAGE_PATH)
        data = yaml.safe_load(stream)
        return data.get("verified_queries", [])
    except Exception as e:
        st.warning(f"Could not load Sales YAML from stage. Ensure the path is correct. Error: {e}")
        return []

sales_queries_list = load_sales_queries_from_stage(session)

def generate_sales_sql_from_prompt(prompt: str):
    """Parses the dynamically loaded Sales YAML to find a match"""
    if not sales_queries_list:
        return None, None
        
    p = prompt.lower().strip().replace('?', '')
    
    for q in sales_queries_list:
        yaml_q = q.get('question', '').lower().replace('?', '').strip()
        
        # If the user's prompt matches a YAML verified question
        if p == yaml_q or p in yaml_q or yaml_q in p:
            sql = q.get('sql', '')
            # Dynamically replace Semantic table prefixes (__) with your actual database schema
            sql = sql.replace('__fact_sales_item', 'CORTEX_DEMO.CORTEX_SCHEMA.FACT_SALES_ITEM')
            sql = sql.replace('__fact_sales', 'CORTEX_DEMO.CORTEX_SCHEMA.FACT_SALES')
            sql = sql.replace('__dim_customer', 'CORTEX_DEMO.CORTEX_SCHEMA.DIM_CUSTOMER')
            sql = sql.replace('__dim_product', 'CORTEX_DEMO.CORTEX_SCHEMA.DIM_PRODUCT')
            sql = sql.replace('__dim_date', 'CORTEX_DEMO.CORTEX_SCHEMA.DIM_DATE')
            sql = sql.replace('__dim_sales_rep', 'CORTEX_DEMO.CORTEX_SCHEMA.DIM_SALES_REP')
            
            explanation = f"**Sales AI:** Querying data based on the Sales Semantic Model rule: '{q.get('question')}'"
            return explanation, sql
            
    return None, None


# ===================================================================
# 3. ORIGINAL APP LOGIC & INVENTORY ENGINE (Untouched)
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
    y_key = f"{key_prefix}_y" if key_prefix else
