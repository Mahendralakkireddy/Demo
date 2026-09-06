import streamlit as st
import pandas as pd
from datetime import datetime
import snowflake.connector
from snowflake.snowpark import Session
import requests
from typing import Any, Dict, List, Optional

# ===================================================================
# Configuration
# ===================================================================
HOST = "WDSDGTL-XCC29288.snowflakecomputing.com"
ACCOUNT = "WDSDGTL-XCC29288"
DATABASE = "INVENTORY_DW_DEMO"
SCHEMA = "GOLD"
WAREHOUSE = "COMPUTE_WH"
ROLE = "ACCOUNTADMIN"

# FULL semantic-model YAML files on Snowflake stages.
INVENTORY_YAML_STAGE_PATH = (
    '@"INVENTORY_DW_DEMO"."INVENTORY_SCHEMA"."YAML"/INV_ANALYST_DEMO_90_VERIFIED.yaml'
)
SALES_YAML_STAGE_PATH = (
    '@"CORTEX_DEMO"."CORTEX_SCHEMA"."YAML"/sales_intelligence_model_80_queries_fixed_FINAL.yaml'
)

ANALYST_ENDPOINT = f"https://{HOST}/api/v2/cortex/analyst/message"

st.set_page_config(
    page_title="Dilytics Enterprise AI",
    page_icon="📦",
    layout="wide",
)

st.markdown("""
<style>
.status-pill {
    display:inline-flex; align-items:center; gap:6px;
    background-color:#ecfdf5; color:#065f46;
    border:1px solid #a7f3d0; border-radius:20px;
    padding:2px 10px; font-size:.75rem; font-weight:600;
}
div[data-testid="stButton"] > button {
    border-radius:8px; font-weight:500;
    transition:all .2s ease-in-out;
}
</style>
""", unsafe_allow_html=True)


# ===================================================================
# 1. LOGIN / SNOWFLAKE SESSION
# ===================================================================
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False
    st.session_state.username = "PBCS"
    st.session_state.password = ""
    st.session_state.snowpark_session = None
    st.session_state.snowflake_conn = None

if not st.session_state.authenticated:
    st.title("Welcome to Dilytics Enterprise AI")
    st.markdown("Please login to connect to your Snowflake Data Warehouse.")

    st.session_state.username = st.text_input(
        "Enter Snowflake Username:", value=st.session_state.username
    )
    st.session_state.password = st.text_input(
        "Enter Password:", type="password"
    )

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
                    role=ROLE,
                    database=DATABASE,
                    schema=SCHEMA,
                )
                st.session_state.snowflake_conn = conn
                st.session_state.snowpark_session = (
                    Session.builder.configs({"connection": conn}).create()
                )
                st.session_state.authenticated = True
                st.rerun()
        except Exception as e:
            st.error(f"Authentication failed: {e}")
    st.stop()

session = st.session_state.snowpark_session
conn = st.session_state.snowflake_conn


# ===================================================================
# 2. CORTEX ANALYST
#
# No Python question -> SQL mapping.
# Cortex Analyst receives the complete YAML semantic model(s),
# understands the user's natural-language question, and generates SQL.
# ===================================================================
def get_analyst_headers() -> Dict[str, str]:
    token = conn.rest.token
    return {
        "Authorization": f'Snowflake Token="{token}"',
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def call_cortex_analyst(prompt: str) -> Dict[str, Any]:
    request_body = {
        "messages": [{
            "role": "user",
            "content": [{"type": "text", "text": prompt}],
        }],
        "semantic_models": [
            {"semantic_model_file": INVENTORY_YAML_STAGE_PATH},
            {"semantic_model_file": SALES_YAML_STAGE_PATH},
        ],
        "stream": False,
    }

    response = requests.post(
        ANALYST_ENDPOINT,
        headers=get_analyst_headers(),
        json=request_body,
        timeout=120,
    )

    if response.status_code >= 400:
        try:
            details = response.json()
        except Exception:
            details = response.text
        raise RuntimeError(
            f"Cortex Analyst API error ({response.status_code}): {details}"
        )

    return response.json()


def extract_analyst_response(data: Dict[str, Any]) -> Dict[str, Any]:
    result = {
        "text": "",
        "sql": None,
        "warnings": data.get("warnings", []) or [],
        "semantic_model_selection": data.get("semantic_model_selection"),
        "verified_query_used": None,
        "request_id": data.get("request_id"),
    }

    message = data.get("message", {})
    content = message.get("content", [])
    if isinstance(content, dict):
        content = [content]

    text_parts = []

    for block in content:
        block_type = block.get("type")

        if block_type == "text":
            if block.get("text"):
                text_parts.append(block["text"])

        elif block_type == "sql":
            result["sql"] = (
                block.get("statement")
                or block.get("sql")
                or block.get("query")
            )
            confidence = block.get("confidence", {})
            if isinstance(confidence, dict):
                result["verified_query_used"] = confidence.get(
                    "verified_query_used"
                )

        elif block_type == "suggestions":
            suggestions = block.get("suggestions", [])
            if isinstance(suggestions, list):
                text_parts.append(
                    "I could not generate SQL for this question. "
                    "Try one of these questions:\n\n"
                    + "\n".join(f"- {x}" for x in suggestions)
                )
            elif suggestions:
                text_parts.append(str(suggestions))

    result["text"] = "\n\n".join(text_parts).strip()

    if not result["sql"]:
        result["sql"] = message.get("statement")

    return result


# ===================================================================
# 3. CHAT SESSION STATE
# ===================================================================
if "chat_sessions" not in st.session_state:
    st.session_state.chat_sessions = {}

if "current_session_id" not in st.session_state:
    init_id = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    st.session_state.current_session_id = init_id
    st.session_state.chat_sessions[init_id] = {
        "title": "New Conversation",
        "messages": [],
    }

current_id = st.session_state.current_session_id
messages = st.session_state.chat_sessions[current_id]["messages"]


# ===================================================================
# 4. CHART DISPLAY
# ===================================================================
def display_chart_tab(df: pd.DataFrame, key_prefix: str = ""):
    if df is None or df.empty:
        st.info("No data available for charting.")
        return

    if len(df.columns) < 2:
        st.info("Need at least 2 columns to render a chart.")
        return

    all_cols = list(df.columns)
    col1, col2, col3 = st.columns(3)

    x_col = col1.selectbox(
        "Dimension (X-axis)", all_cols, index=0,
        key=f"{key_prefix}_x"
    )

    remaining_cols = [c for c in all_cols if c != x_col]
    if not remaining_cols:
        return

    y_col = col2.selectbox(
        "Metric (Y-axis)", remaining_cols, index=0,
        key=f"{key_prefix}_y"
    )

    chart_type = col3.selectbox(
        "Chart Type",
        ["Bar Chart", "Line Chart", "Area Chart", "Scatter Plot"],
        key=f"{key_prefix}_type",
    )

    chart_df = df.copy()

    if any(k in x_col.lower()
           for k in ["year", "quarter", "month", "day", "date"]):
        chart_df[x_col] = chart_df[x_col].apply(
            lambda x: (
                str(int(x))
                if pd.notnull(x) and isinstance(x, (int, float))
                else str(x)
            )
        )

    try:
        if chart_type == "Bar Chart":
            st.bar_chart(chart_df.set_index(x_col)[y_col])
        elif chart_type == "Line Chart":
            st.line_chart(chart_df.set_index(x_col)[y_col])
        elif chart_type == "Area Chart":
            st.area_chart(chart_df.set_index(x_col)[y_col])
        else:
            st.scatter_chart(chart_df, x=x_col, y=y_col)
    except Exception as e:
        st.info(f"Chart could not be rendered: {e}")


# ===================================================================
# 5. SIDEBAR
# ===================================================================
with st.sidebar:
    st.markdown("### ⚡ Dilytics AI")
    st.markdown(
        '<span class="status-pill">● Cortex Analyst Live</span>',
        unsafe_allow_html=True,
    )
    st.write("")

    if st.button("➕ New Chat", use_container_width=True, type="primary"):
        new_id = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        st.session_state.current_session_id = new_id
        st.session_state.chat_sessions[new_id] = {
            "title": f"Chat {len(st.session_state.chat_sessions) + 1}",
            "messages": [],
        }
        st.rerun()

    st.markdown("---")
    st.markdown("##### 🕒 Recent Conversations")

    for s_id, s_data in reversed(list(st.session_state.chat_sessions.items())):
        is_active = s_id == st.session_state.current_session_id
        label = s_data["title"]
        if len(label) > 20:
            label = label[:18] + "..."

        if st.button(
            f"{'👉 ' if is_active else '🗨️ '}{label}",
            key=f"sess_{s_id}",
            use_container_width=True,
        ):
            st.session_state.current_session_id = s_id
            st.rerun()

    st.markdown("---")

    if st.button("🗑️ Clear All Sessions", use_container_width=True):
        st.session_state.chat_sessions = {}
        init_id = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        st.session_state.current_session_id = init_id
        st.session_state.chat_sessions[init_id] = {
            "title": "New Conversation",
            "messages": [],
        }
        st.rerun()


# ===================================================================
# 6. MAIN HEADER
# ===================================================================
head_col1, head_col2 = st.columns([4.5, 1.2])

with head_col1:
    st.title("💬 Dilytics Enterprise AI")
    st.caption(
        "Ask natural-language questions to explore Inventory and Sales."
    )

with head_col2:
    st.write("")
    if st.button(
        "🔄 Reset Thread",
        use_container_width=True,
        help="Clear message history",
    ):
        st.session_state.chat_sessions[current_id]["messages"] = []
        st.session_state.chat_sessions[current_id]["title"] = "New Conversation"
        st.rerun()


# ===================================================================
# 7. EXAMPLE QUESTIONS
# These buttons are only examples. They do NOT contain SQL.
# ===================================================================
quick_prompt = None
tab_inv, tab_sales = st.tabs(
    ["📦 Inventory Intelligence", "💰 Sales Intelligence"]
)

with tab_inv:
    with st.expander("💡 What can I ask about Inventory?", expanded=False):
        st.markdown("""
        Questions are answered by Cortex Analyst using
        `INV_ANALYST_DEMO_90_VERIFIED.yaml`.

        * How many products are out of stock?
        * What is the inventory value by warehouse?
        * Which products have the highest inventory value?
        * Which warehouses have the highest outbound quantity?
        * Which products need to be reordered?
        * What is the inventory value by product category?
        """)

    st.markdown("##### 💡 Example Inventory Questions")
    q1, q2, q3, q4, q5 = st.columns(5)

    if q1.button("💰 Inventory Value", use_container_width=True, key="i1"):
        quick_prompt = "What is the total inventory value?"
    if q2.button("🏭 Value by Warehouse", use_container_width=True, key="i2"):
        quick_prompt = "What is the inventory value by warehouse?"
    if q3.button("📦 Value by Category", use_container_width=True, key="i3"):
        quick_prompt = "What is the inventory value by product category?"
    if q4.button("📉 Stockout Count", use_container_width=True, key="i4"):
        quick_prompt = "How many products are out of stock?"
    if q5.button("⚠️ Excess Stock", use_container_width=True, key="i5"):
        quick_prompt = "What is the total excess inventory value by warehouse?"

with tab_sales:
    with st.expander("💡 What can I ask about Sales?", expanded=False):
        st.markdown("""
        Questions are answered by Cortex Analyst using
        `sales_intelligence_model_80_queries_fixed.yaml`.

        * What is the total sales amount?
        * What are the top products by sales?
        * What are total sales by customer region?
        * What are total sales by month?
        * What is total sales by order channel?
        """)

    st.markdown("##### 💡 Example Sales Questions")
    s1, s2, s3, s4, s5 = st.columns(5)

    if s1.button("💵 Total Sales", use_container_width=True, key="s1"):
        quick_prompt = "What is the total sales amount?"
    if s2.button("🏆 Top Products", use_container_width=True, key="s2"):
        quick_prompt = "What are the top products by sales?"
    if s3.button("🌍 Sales by Region", use_container_width=True, key="s3"):
        quick_prompt = "What are total sales by customer region?"
    if s4.button("📅 Monthly Sales", use_container_width=True, key="s4"):
        quick_prompt = "What are total sales by month?"
    if s5.button("📊 Sales by Channel", use_container_width=True, key="s5"):
        quick_prompt = "What is total sales by order channel?"

st.markdown("---")


# ===================================================================
# 8. DISPLAY CHAT HISTORY
# ===================================================================
for idx, msg in enumerate(messages):
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

        if msg.get("sql"):
            with st.expander("Generated SQL", expanded=False):
                st.code(msg["sql"], language="sql")

        if msg.get("semantic_model"):
            st.caption(
                f"Semantic model selected: `{msg['semantic_model']}`"
            )

        if msg.get("verified_query"):
            name = msg["verified_query"].get("name")
            if name:
                st.caption(f"Verified Query Used: `{name}`")

        if msg.get("data") is not None:
            tab_data, tab_chart = st.tabs(["Data 📄", "Chart 📈"])
            with tab_data:
                st.dataframe(msg["data"], use_container_width=True)
            with tab_chart:
                display_chart_tab(
                    msg["data"],
                    key_prefix=f"hist_{current_id}_{idx}",
                )


# ===================================================================
# 9. CHAT INPUT
# ===================================================================
user_prompt = (
    st.chat_input(
        "Ask a question about inventory, warehouses, products, sales, customers..."
    )
    or quick_prompt
)


# ===================================================================
# 10. CORTEX ANALYST EXECUTION
# ===================================================================
if user_prompt:
    if len(messages) == 0:
        st.session_state.chat_sessions[current_id]["title"] = (
            user_prompt[:25] + ("..." if len(user_prompt) > 25 else "")
        )

    messages.append({"role": "user", "content": user_prompt})

    with st.chat_message("user"):
        st.markdown(user_prompt)

    with st.chat_message("assistant"):
        df = None
        sql_query = None
        explanation = ""
        semantic_model = None
        verified_query = None

        try:
            with st.spinner("Cortex Analyst is interpreting your question..."):
                analyst_json = call_cortex_analyst(user_prompt)
                result = extract_analyst_response(analyst_json)

            explanation = result["text"]
            sql_query = result["sql"]
            semantic_model = result["semantic_model_selection"]
            verified_query = result["verified_query_used"]

            for warning in result["warnings"]:
                warning_text = (
                    warning.get("message", str(warning))
                    if isinstance(warning, dict)
                    else str(warning)
                )
                st.warning(warning_text)

            if not sql_query:
                if not explanation:
                    explanation = (
                        "Cortex Analyst could not generate SQL for this "
                        "question from the configured semantic models."
                    )
                st.markdown(explanation)

            else:
                if not explanation:
                    explanation = (
                        "I generated this answer using the Snowflake "
                        "semantic model."
                    )

                st.markdown(explanation)

                if semantic_model:
                    st.caption(
                        f"Semantic model selected: `{semantic_model}`"
                    )

                if verified_query:
                    name = verified_query.get("name")
                    if name:
                        st.caption(f"Verified Query Used: `{name}`")

                with st.expander("Generated SQL", expanded=False):
                    st.code(sql_query, language="sql")

                with st.spinner("Executing generated SQL in Snowflake..."):
                    df = session.sql(sql_query).to_pandas()

                tab_data, tab_chart = st.tabs(["Data 📄", "Chart 📈"])

                with tab_data:
                    st.dataframe(df, use_container_width=True)

                with tab_chart:
                    display_chart_tab(
                        df,
                        key_prefix=f"live_{current_id}_{len(messages)}",
                    )

        except requests.exceptions.Timeout:
            explanation = (
                "Cortex Analyst took too long to respond. Please try again."
            )
            st.error(explanation)

        except requests.exceptions.RequestException as e:
            explanation = f"Could not connect to Cortex Analyst: {e}"
            st.error(explanation)

        except Exception as e:
            explanation = f"Unable to process the question: {e}"
            st.error(explanation)

        messages.append({
            "role": "assistant",
            "content": explanation,
            "sql": sql_query,
            "data": df,
            "semantic_model": semantic_model,
            "verified_query": verified_query,
        })

    st.rerun()
