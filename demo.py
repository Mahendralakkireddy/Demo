import io
import streamlit as st
import pandas as pd
from datetime import datetime
import snowflake.connector
from snowflake.snowpark import Session
import requests
from typing import Any, Dict, List, Optional
import re
import yaml

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
    '@"INVENTORY_DW_DEMO"."INVENTORY_SCHEMA"."YAML"/INV_ANALYST_DEMO_90_VERIFIED_FIXED_1.yaml'
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


def call_cortex_analyst_with_semantic_model(
    prompt: str,
    semantic_model_yaml: str,
) -> Dict[str, Any]:
    """Call Cortex Analyst with an inline, dynamically generated YAML model."""
    request_body = {
        "messages": [{
            "role": "user",
            "content": [{"type": "text", "text": prompt}],
        }],
        "semantic_model": semantic_model_yaml,
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

    data = response.json()
    if isinstance(data, dict) and data.get("error_code"):
        raise RuntimeError(
            f"Cortex Analyst returned error {data.get('error_code')}: "
            f"{data.get('message', data)}"
        )
    return data


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
# 2A. UPLOADED DOCUMENT ANALYSIS (ADDED - ORIGINAL CORTEX ANALYST
#     INVENTORY/SALES CODE IS PRESERVED)
# ===================================================================
def extract_df_from_xlsx(file_bytes: bytes) -> pd.DataFrame:
    """Multi-stage robust spreadsheet extraction supporting XLSX, XLS, HTML, and CSV fallbacks."""
    # 1. Standard openpyxl engine
    try:
        df = pd.read_excel(io.BytesIO(file_bytes), engine="openpyxl")
        if df is not None and not df.empty:
            return df
    except Exception:
        pass

    # 2. Try xlrd for older binary .xls files
    try:
        df = pd.read_excel(io.BytesIO(file_bytes), engine="xlrd")
        if df is not None and not df.empty:
            return df
    except Exception:
        pass

    # 3. Try default auto engine
    try:
        df = pd.read_excel(io.BytesIO(file_bytes))
        if df is not None and not df.empty:
            return df
    except Exception:
        pass

    # 4. Deep ZIP-XML parsing for modern .xlsx
    try:
        with zipfile.ZipFile(io.BytesIO(file_bytes)) as z:
            shared_strings = []
            if 'xl/sharedStrings.xml' in z.namelist():
                ss_tree = ET.fromstring(z.read('xl/sharedStrings.xml'))
                for si in ss_tree.iterfind('{http://schemas.openxmlformats.org/spreadsheetml/2006/main}si'):
                    t_nodes = si.iterfind('.//{http://schemas.openxmlformats.org/spreadsheetml/2006/main}t')
                    shared_strings.append("".join([n.text or "" for n in t_nodes]))

            sheet_files = [n for n in z.namelist() if n.startswith('xl/worksheets/sheet')]
            if sheet_files:
                sheet_tree = ET.fromstring(z.read(sheet_files[0]))
                rows_data = []
                for row in sheet_tree.iterfind('.//{http://schemas.openxmlformats.org/spreadsheetml/2006/main}row'):
                    row_cells = []
                    for c in row.iterfind('{http://schemas.openxmlformats.org/spreadsheetml/2006/main}c'):
                        val_node = c.find('{http://schemas.openxmlformats.org/spreadsheetml/2006/main}v')
                        cell_val = val_node.text if val_node is not None else ""
                        if c.attrib.get('t') == 's' and cell_val.isdigit():
                            idx = int(cell_val)
                            cell_val = shared_strings[idx] if idx < len(shared_strings) else cell_val
                        row_cells.append(cell_val)
                    if any(str(cell).strip() for cell in row_cells):
                        rows_data.append(row_cells)

                if rows_data:
                    headers = [str(h).strip() if str(h).strip() else f"Col_{i+1}" for i, h in enumerate(rows_data[0])]
                    df = pd.DataFrame(rows_data[1:], columns=headers)
                    for col in df.columns:
                        try:
                            df[col] = pd.to_numeric(df[col])
                        except (ValueError, TypeError):
                            pass
                    return df
    except Exception:
        pass

    # 5. Check if the spreadsheet is actually an HTML table export
    try:
        tables = pd.read_html(io.BytesIO(file_bytes))
        if tables:
            return tables[0]
    except Exception:
        pass

    # 6. Check if it is a renamed CSV file across standard delimiters and encodings
    for enc in ['utf-8', 'latin1', 'cp1252']:
        for sep in [',', '\t', ';', '|']:
            try:
                df = pd.read_csv(io.BytesIO(file_bytes), sep=sep, encoding=enc)
                if df is not None and len(df.columns) > 1 and len(df) > 0:
                    return df
            except Exception:
                pass

    raise ValueError("Unable to read this spreadsheet. Please ensure it is a valid .xlsx, .xls, or .csv file.")

def extract_text_from_pdf(file_bytes: bytes) -> str:
    if pypdf is None:
        return "PDF text extraction requires the pypdf library."
    try:
        reader = pypdf.PdfReader(io.BytesIO(file_bytes))
        return "".join([page.extract_text() or "" for page in reader.pages]).strip()
    except Exception as exc:
        return f"Error extracting PDF: {str(exc)}"

def extract_text_from_docx(file_bytes: bytes) -> str:
    if not file_bytes:
        return ""
    try:
        with zipfile.ZipFile(io.BytesIO(file_bytes)) as z:
            xml_content = z.read('word/document.xml')
            tree = ET.fromstring(xml_content)
            text_pieces = []
            for node in tree.iter():
                if node.tag.split('}')[-1] == 't' and node.text:
                    text_pieces.append(node.text)
                elif node.tag.split('}')[-1] in ('p', 'tr'):
                    text_pieces.append("\n")
            return re.sub(r'\n\s*\n+', '\n\n', "".join(text_pieces)).strip()
    except Exception as exc:
        return f"Error extracting Word document: {str(exc)}"

def answer_user_question_on_document(question: str, doc_context: str, filename: str, df: Optional[pd.DataFrame] = None) -> str:
    q_lower = question.lower().strip()

    if df is not None and not df.empty:
        col_map = {str(col).lower().strip(): col for col in df.columns}
        matched_target_col = None
        for c_lower, c_orig in col_map.items():
            stem = c_lower.rstrip('s')
            if stem in q_lower or (stem.endswith('y') and stem[:-1] + 'ies' in q_lower):
                matched_target_col = c_orig
                break

        is_count_query = any(k in q_lower for k in ["how many", "count", "number of", "total", "distinct", "unique"])
        is_list_query = any(k in q_lower for k in ["list", "what are", "show", "names of", "give me"])

        if matched_target_col and is_count_query:
            valid_entries = df[matched_target_col].dropna()
            valid_entries = valid_entries[valid_entries.astype(str).str.strip().str.lower() != 'none']
            total_rows = len(valid_entries)
            unique_count = valid_entries.nunique()
            unique_vals = list(valid_entries.unique())

            sample_str = ", ".join([f"`{str(v)}`" for v in unique_vals[:8]])
            if len(unique_vals) > 8:
                sample_str += f" and {len(unique_vals) - 8} more..."

            return (
                f"In **`{filename}`**, there are **{unique_count} unique {matched_target_col}s** "
                f"(across **{total_rows}** total populated records).\n\n"
                f"**Entries:** {sample_str}"
            )

        if matched_target_col and is_list_query:
            valid_entries = df[matched_target_col].dropna()
            valid_entries = valid_entries[valid_entries.astype(str).str.strip().str.lower() != 'none']
            unique_vals = list(valid_entries.unique())
            val_bullets = "\n".join([f"• {str(v)}" for v in unique_vals])
            return f"**List of {matched_target_col}s in `{filename}` ({len(unique_vals)} unique):**\n\n{val_bullets}"

        words = [w for w in re.findall(r'\b[a-zA-Z0-9_]+\b', q_lower) if len(w) > 2 and w not in [
            "what", "is", "the", "are", "sales", "for", "total", "average", "avg", 
            "count", "show", "give", "list", "of", "in", "by", "all", "me", "find",
            "county", "state", "city", "district", "value", "how", "many"
        ]]
        
        mask = pd.Series(False, index=df.index)
        for col in df.columns:
            for word in words:
                mask = mask | df[col].astype(str).str.lower().str.contains(r'\b' + re.escape(word) + r'\b', na=False)
        
        matched_df = df[mask]
        
        if not matched_df.empty:
            numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
            sales_cols = [c for c in numeric_cols if any(k in c.lower() for k in ["sale", "amount", "revenue", "total", "val"])]
            target_metric_col = sales_cols[0] if sales_cols else (numeric_cols[0] if numeric_cols else None)

            if target_metric_col:
                total_val = matched_df[target_metric_col].sum()
                avg_val = matched_df[target_metric_col].mean()
                count_val = len(matched_df)
                matched_entity = ' '.join(words).title() if words else "the requested entity"

                if any(k in q_lower for k in ["average", "avg", "mean"]):
                    return f"In **`{filename}`**, the average **{target_metric_col}** for **{matched_entity}** is **{avg_val:,.2f}** ({count_val} matching records found)."
                else:
                    return f"In **`{filename}`**, the total **{target_metric_col}** for **{matched_entity}** is **{total_val:,.2f}** ({count_val} matching records found)."
            else:
                preview = matched_df.dropna(how='all', axis=1).head(15)
                return f"Found **{len(matched_df)}** matching record(s) in **`{filename}`**:\n\n" + preview.to_markdown(index=False)

    clean_doc = doc_context[:10000].replace("'", "''")
    clean_q = question.replace("'", "''")
    prompt = f"Answer factually using only this data from {filename}:\n\n{clean_doc}\n\nQuestion: {clean_q}"
    
    for model in ['llama3.1-8b', 'mistral-7b']:
        try:
            res = session.sql(f"SELECT SNOWFLAKE.CORTEX.COMPLETE('{model}', '{prompt}') AS answer").collect()
            ans = res[0]["ANSWER"].strip()
            if ans and len(ans) > 2:
                return ans
        except Exception:
            continue

    return f"The uploaded document (`{filename}`) does not contain information to answer this query."

def process_uploaded_document(uploaded_file) -> Tuple[str, Optional[pd.DataFrame], Optional[str]]:
    uploaded_file.seek(0)
    filename = uploaded_file.name
    file_bytes = uploaded_file.read()
    if not file_bytes:
        return f"The uploaded file `{filename}` is empty.", None, None

    fname_lower = filename.lower()
    try:
        if fname_lower.endswith((".csv", ".xlsx", ".xls")):
            if fname_lower.endswith(".csv"):
                try:
                    df = pd.read_csv(io.BytesIO(file_bytes))
                except Exception:
                    df = pd.read_csv(io.BytesIO(file_bytes), encoding='latin1')
            else:
                df = extract_df_from_xlsx(file_bytes)
                
            clean_df = df.dropna(how='all')
            context_str = f"File: {filename}\nTotal Rows: {len(clean_df)}\nColumns: {', '.join([str(c) for c in clean_df.columns])}\n\nDATA PREVIEW AND RECORDS:\n"
            context_str += clean_df.to_string(max_rows=150)
            
            summary = (
                f"Successfully processed **`{filename}`** with **{len(clean_df):,} rows** and **{len(clean_df.columns)} columns**.\n\n"
                f"**Columns:** {', '.join([f'`{col}`' for col in clean_df.columns])}\n\n"
                f"You can now ask questions about the records, values, or metrics in this file."
            )
            return summary, clean_df, context_str
            
        elif fname_lower.endswith(".pdf"):
            txt = extract_text_from_pdf(file_bytes)
            summary = f"Uploaded PDF **`{filename}`** (~{len(txt.split()):,} words). Ready for your questions."
            return summary, None, txt
            
        elif fname_lower.endswith((".docx", ".doc")):
            txt = extract_text_from_docx(file_bytes)
            summary = f"Uploaded Word Document **`{filename}`** (~{len(txt.split()):,} words). Ready for your questions."
            return summary, None, txt
            
    except Exception as err:
        return f"⚠️ Could not parse `{filename}`: {str(err)}", None, None

    return f"Unsupported format for `{filename}`.", None, None



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
    st.markdown("---")
    st.markdown("##### 📄 Analyze an Uploaded Document")

    uploaded_doc = st.file_uploader(
        "Upload CSV, Excel, PDF or Word",
        type=["csv", "xlsx", "xls", "pdf", "docx"],
        key="document_uploader",
        help="Upload a document, click Analyze, then choose Uploaded Document in the chat.",
    )

    if st.button(
        "🔍 Analyze Document",
        use_container_width=True,
        disabled=uploaded_doc is None,
        key="analyze_uploaded_document",
    ):
        try:
            with st.spinner("Reading and analyzing document..."):
                doc_type, doc_df, doc_text, doc_message = process_uploaded_document(
                    uploaded_doc
                )

                _drop_uploaded_table()
                st.session_state.uploaded_document_name = uploaded_doc.name
                st.session_state.uploaded_document_type = doc_type
                st.session_state.uploaded_document_df = doc_df
                st.session_state.uploaded_document_text = doc_text
                st.session_state.uploaded_document = uploaded_doc.name
                st.session_state.uploaded_document_table = None
                st.session_state.uploaded_document_semantic_model = None

                if doc_type == "table":
                    prepare_uploaded_table(doc_df)
                elif doc_type == "text":
                    # Word (.docx) uses the trial-safe local document Q&A path below.
                    # Keep PDF on the existing AI_COMPLETE path. The working
                    # Excel/CSV Cortex Analyst functionality is untouched.
                    if uploaded_doc.name.lower().endswith(".pdf"):
                        _upload_document_to_stage(uploaded_doc)

            st.success(doc_message)
            st.rerun()
        except Exception as e:
            st.error(f"Document analysis failed: {e}")

    if st.session_state.uploaded_document_name:
        st.caption(
            f"Loaded: `{st.session_state.uploaded_document_name}`"
        )
        if st.button(
            "✖ Remove Uploaded Document",
            use_container_width=True,
            key="remove_uploaded_document",
        ):
            _drop_uploaded_table()
            st.session_state.uploaded_document = None
            st.session_state.uploaded_document_name = None
            st.session_state.uploaded_document_df = None
            st.session_state.uploaded_document_text = ""
            st.session_state.uploaded_document_type = None
            st.session_state.uploaded_document_table = None
            st.session_state.uploaded_document_semantic_model = None
            st.session_state.uploaded_document_stage = None
            st.session_state.uploaded_document_stage_file = None
            st.rerun()
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
# ===================================================================
# 7A. UPLOADED DOCUMENT PREVIEW (ADDED)
# ===================================================================
render_uploaded_document_preview()
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
# ===================================================================
# 8A. ANSWER SOURCE (ADDED)
# ===================================================================
if st.session_state.uploaded_document_name:
    answer_source = st.radio(
        "Answer from:",
        ["Snowflake Data", "Uploaded Document"],
        horizontal=True,
        key="answer_source",
        help="Choose whether your question should use the existing Inventory/Sales semantic models or the uploaded document.",
    )
else:
    answer_source = "Snowflake Data"
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
    # ===================================================================
    # UPLOADED DOCUMENT QUESTION PATH (ADDED)
    # This branch is intentionally placed before the original Cortex
    # Analyst block. The original Inventory/Sales path below is unchanged.
    # ===================================================================
    if answer_source == "Uploaded Document":
        if len(messages) == 0:
            st.session_state.chat_sessions[current_id]["title"] = (
                user_prompt[:25] + ("..." if len(user_prompt) > 25 else "")
            )

        messages.append({"role": "user", "content": user_prompt})
        with st.chat_message("user"):
            st.markdown(user_prompt)

        with st.chat_message("assistant"):
            doc_df_result = None
            doc_sql_result = None
            doc_answer = ""

            try:
                with st.spinner("Analyzing your uploaded document..."):
                    if st.session_state.uploaded_document_type == "table":
                        doc_df_result, doc_sql_result, doc_analyst_result = answer_uploaded_table_question(
                            user_prompt,
                            st.session_state.uploaded_document_df,
                        )
                        doc_answer = (
                            "I answered your question using the complete uploaded "
                            "dataset through Cortex Analyst. The SQL below was "
                            "generated dynamically from the uploaded document schema."
                        )
                        if doc_analyst_result.get("semantic_model_selection"):
                            st.caption(
                                "Semantic model selected: "
                                + str(doc_analyst_result["semantic_model_selection"])
                            )
                    else:
                        doc_answer = answer_uploaded_text_question(
                            user_prompt,
                            st.session_state.uploaded_document_text,
                        )

                st.markdown(doc_answer)

                if doc_sql_result:
                    with st.expander("Generated SQL for Uploaded Document", expanded=False):
                        st.code(doc_sql_result, language="sql")

                if doc_df_result is not None:
                    tab_data, tab_chart = st.tabs(["Data 📄", "Chart 📈"])
                    with tab_data:
                        st.dataframe(doc_df_result, use_container_width=True)
                    with tab_chart:
                        display_chart_tab(
                            doc_df_result,
                            key_prefix=f"document_{current_id}_{len(messages)}",
                        )

                messages.append({
                    "role": "assistant",
                    "content": doc_answer,
                    "sql": doc_sql_result,
                    "data": doc_df_result,
                    "semantic_model": "Uploaded Document",
                    "verified_query": None,
                })

            except Exception as e:
                doc_answer = f"Unable to analyze the uploaded document: {e}"
                st.error(doc_answer)
                messages.append({
                    "role": "assistant",
                    "content": doc_answer,
                    "sql": None,
                    "data": None,
                    "semantic_model": "Uploaded Document",
                    "verified_query": None,
                })

        st.rerun()
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
