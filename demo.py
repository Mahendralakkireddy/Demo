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
DOCUMENT_CORTEX_MODEL = "claude-3-5-sonnet"  # retained only for legacy PDF/DOCX path

if "uploaded_document" not in st.session_state:
    st.session_state.uploaded_document = None
if "uploaded_document_name" not in st.session_state:
    st.session_state.uploaded_document_name = None
if "uploaded_document_df" not in st.session_state:
    st.session_state.uploaded_document_df = None
if "uploaded_document_text" not in st.session_state:
    st.session_state.uploaded_document_text = ""
if "uploaded_document_type" not in st.session_state:
    st.session_state.uploaded_document_type = None
if "uploaded_document_table" not in st.session_state:
    st.session_state.uploaded_document_table = None
if "uploaded_document_semantic_model" not in st.session_state:
    st.session_state.uploaded_document_semantic_model = None


def _snowflake_sql_literal(value: str) -> str:
    """Safely convert a Python string into a Snowflake SQL string literal."""
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def cortex_complete(prompt: str) -> str:
    """Legacy PDF/DOCX helper.

    Excel/CSV analysis does NOT use COMPLETE.  It uses Cortex Analyst below,
    which is available in the user's current trial setup.
    """
    model_literal = _snowflake_sql_literal(DOCUMENT_CORTEX_MODEL)
    prompt_literal = _snowflake_sql_literal(prompt)
    sql = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            {model_literal},
            {prompt_literal}
        ) AS RESPONSE
    """
    rows = session.sql(sql).collect()
    if not rows:
        raise RuntimeError("Cortex did not return a response.")
    row = rows[0]
    try:
        response = row["RESPONSE"]
    except Exception:
        response = row[0]
    if response is None:
        raise RuntimeError("Cortex returned an empty response.")
    return str(response)


def _clean_generated_sql(text_value: str) -> str:
    """Extract and validate a read-only SELECT/WITH SQL statement."""
    sql_text = str(text_value or "").strip()

    if "```" in sql_text:
        blocks = re.findall(
            r"```(?:sql|SQL)?\s*(.*?)```", sql_text, flags=re.DOTALL
        )
        if blocks:
            sql_text = blocks[0].strip()

    sql_text = re.sub(
        r"^\s*(SQL\s*:|Query\s*:)\s*", "", sql_text, flags=re.I
    ).strip().rstrip(";").strip()

    if not re.match(r"^(SELECT|WITH)\b", sql_text, flags=re.I):
        raise RuntimeError(
            "Cortex Analyst did not return a valid SELECT/WITH statement."
        )

    forbidden = re.search(
        r"\b(INSERT|UPDATE|DELETE|MERGE|DROP|ALTER|TRUNCATE|CREATE|GRANT|REVOKE|COPY|PUT|REMOVE|CALL)\b",
        sql_text,
        flags=re.I,
    )
    if forbidden:
        raise RuntimeError(
            f"Generated document SQL contains a non-read-only command: {forbidden.group(1)}"
        )

    return sql_text


def _normalize_uploaded_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Make mixed Excel/CSV columns safe for Streamlit and Snowflake.

    Numeric/date/bool columns stay typed. Object columns are normalized to text
    because Excel frequently mixes integers, strings such as 'Grand Total', and
    blanks in the same column.
    """
    if df is None:
        return df

    work_df = df.copy()
    for col in work_df.columns:
        series = work_df[col]
        if pd.api.types.is_object_dtype(series.dtype):
            work_df[col] = series.map(
                lambda value: None if pd.isna(value) else str(value)
            )
    return work_df


def _safe_column_names(df: pd.DataFrame):
    """Create SQL-friendly, unique Snowflake column names."""
    mapping = {}
    used = set()

    for original in df.columns:
        base = re.sub(
            r"[^A-Za-z0-9_]+", "_", str(original)
        ).strip("_").upper()
        if not base:
            base = "COLUMN"
        if base[0].isdigit():
            base = "_" + base

        candidate = base
        n = 2
        while candidate in used:
            candidate = f"{base}_{n}"
            n += 1

        used.add(candidate)
        mapping[str(original)] = candidate

    return mapping


def _snowflake_type_for_pandas(dtype) -> str:
    if pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    if pd.api.types.is_integer_dtype(dtype):
        return "NUMBER"
    if pd.api.types.is_float_dtype(dtype):
        return "NUMBER"
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP_NTZ"
    return "TEXT"


def _column_synonyms(original_name: str):
    """Create conservative synonyms from the actual uploaded header."""
    text = re.sub(r"[_\-]+", " ", str(original_name)).strip()
    words = text.split()
    synonyms = [text.lower()]

    if text.lower().endswith(" id"):
        synonyms.append(text[:-3].strip().lower() + " identifier")
    if "commercial project" in text.lower() and "id" in text.lower():
        synonyms.extend(["project id", "commercial project"])
    if "jurisdiction" in text.lower():
        synonyms.extend(["jurisdiction", "local jurisdiction"])
    if "contractor" in text.lower():
        synonyms.extend(["contractor", "vendor"])
    if "business name" in text.lower():
        synonyms.extend(["business", "project business"])
    if "close out" in text.lower() or "closeout" in text.lower():
        synonyms.extend([
            "closeout date",
            "close out date",
            "completion date",
            "completed date",
        ])

    # Preserve order and uniqueness.
    result = []
    seen = set()
    for item in synonyms:
        item = item.strip()
        if item and item not in seen:
            result.append(item)
            seen.add(item)
    return result[:8]


def _sample_values(df: pd.DataFrame, original: str, limit: int = 5):
    values = []
    for value in df[original].dropna().head(limit).tolist():
        text = str(value)
        if len(text) > 100:
            text = text[:97] + "..."
        values.append(text)
    return values


def build_uploaded_semantic_model(df: pd.DataFrame, table_name: str) -> str:
    """Build a semantic model directly from the uploaded spreadsheet schema.

    The model is sent inline to the Cortex Analyst REST API. No COMPLETE call
    and no hard-coded question-to-SQL mapping are used.
    """
    mapping = _safe_column_names(df)

    dimensions = []
    time_dimensions = []
    facts = []

    for original, safe in mapping.items():
        dtype = df[original].dtype
        sf_type = _snowflake_type_for_pandas(dtype)
        synonyms = _column_synonyms(original)
        samples = _sample_values(df, original)
        desc = f"Uploaded spreadsheet column '{original}'."

        # Close-out date is commonly the strongest completion indicator in
        # project workbooks. Only add this interpretation when that real column exists.
        original_lower = original.lower()
        if "close out" in original_lower or "closeout" in original_lower:
            desc = (
                f"Uploaded spreadsheet column '{original}'. A non-null value indicates "
                "that the project received close-out approval and can be used as a "
                "completion indicator."
            )

        entry = {
            "name": safe,
            "description": desc,
            "expr": safe,
            "data_type": sf_type,
            "unique": False,
        }
        if synonyms:
            entry["synonyms"] = synonyms
        if samples:
            entry["sample_values"] = samples

        if pd.api.types.is_datetime64_any_dtype(dtype):
            time_dimensions.append(entry)
        else:
            dimensions.append(entry)

        if pd.api.types.is_numeric_dtype(dtype):
            facts.append({
                "name": safe,
                "description": f"Numeric value from uploaded column '{original}'.",
                "expr": safe,
                "data_type": "NUMBER",
                "sample_values": samples,
            })

    # A row indicator gives Analyst an explicit way to calculate row/project
    # counts without requiring any hard-coded question mapping.
    facts.append({
        "name": "ROW_INDICATOR",
        "description": "One numeric indicator per uploaded spreadsheet row. SUM this fact to count rows/projects.",
        "expr": "1",
        "data_type": "NUMBER",
    })

    # Add a semantic completion flag only when a real close-out column exists.
    closeout_safe = None
    for original, safe in mapping.items():
        low = original.lower()
        if "close out" in low or "closeout" in low:
            closeout_safe = safe
            break

    if closeout_safe:
        dimensions.append({
            "name": "IS_COMPLETED",
            "description": "True when the close-out approval date is not null; this represents a completed project in this uploaded workbook.",
            "expr": f"{closeout_safe} IS NOT NULL",
            "data_type": "BOOLEAN",
            "unique": False,
            "synonyms": ["completed", "project completed", "completion status"],
            "sample_values": ["TRUE", "FALSE"],
        })

    table_definition = {
        "name": "UPLOADED_DATA",
        "description": "One logical table containing the complete uploaded spreadsheet.",
        "base_table": {
            "database": DATABASE,
            "schema": SCHEMA,
            "table": table_name,
        },
        "dimensions": dimensions,
        "facts": facts,
    }
    if time_dimensions:
        table_definition["time_dimensions"] = time_dimensions

    model = {
        "name": "UPLOADED_DOCUMENT_ANALYSIS",
        "description": "Semantic model generated dynamically from one uploaded spreadsheet. Use only this uploaded dataset.",
        "tables": [table_definition],
        "module_custom_instructions": {
            "sql_generation": (
                "Use only the uploaded_data logical table. Query the complete underlying table. "
                "Use ROW_INDICATOR for row/project counts when appropriate. "
                "If IS_COMPLETED exists, use it when the user asks about completed projects. "
                "Do not invent columns or business definitions."
            ),
            "question_categorization": (
                "Classify questions only from the uploaded table's actual columns and values. "
                "Do not use the Inventory or Sales semantic models for this document question."
            ),
        },
    }

    return yaml.safe_dump(
        model,
        sort_keys=False,
        allow_unicode=True,
        default_flow_style=False,
    )


def _drop_uploaded_table():
    table_name = st.session_state.get("uploaded_document_table")
    if not table_name:
        return
    try:
        # Generated names contain only A-Z, 0-9 and underscore.
        if re.fullmatch(r"UPLOADED_DOCUMENT_[A-Z0-9_]+", str(table_name)):
            session.sql(f'DROP TABLE IF EXISTS "{table_name}"').collect()
    except Exception:
        pass
    st.session_state.uploaded_document_table = None
    st.session_state.uploaded_document_semantic_model = None


def process_uploaded_document(uploaded_file):
    """Read CSV/XLSX/XLS/PDF/DOCX and return display data/text."""
    name = uploaded_file.name
    extension = name.rsplit(".", 1)[-1].lower()

    if extension == "csv":
        uploaded_file.seek(0)
        df = pd.read_csv(uploaded_file)
        df = _normalize_uploaded_dataframe(df)
        return "table", df, "", f"CSV file loaded with {len(df):,} rows."

    if extension in {"xlsx", "xls"}:
        uploaded_file.seek(0)
        excel_file = pd.ExcelFile(uploaded_file)
        sheet_name = excel_file.sheet_names[0]
        df = pd.read_excel(excel_file, sheet_name=sheet_name)
        df = _normalize_uploaded_dataframe(df)
        return (
            "table",
            df,
            "",
            f"Excel file loaded from sheet '{sheet_name}' with {len(df):,} rows.",
        )

    if extension == "pdf":
        try:
            from pypdf import PdfReader
        except ImportError:
            from PyPDF2 import PdfReader

        uploaded_file.seek(0)
        reader = PdfReader(uploaded_file)
        pages = []
        for page in reader.pages:
            pages.append(page.extract_text() or "")
        full_text = "\n\n".join(pages).strip()
        return "text", None, full_text, f"PDF analyzed successfully ({len(reader.pages)} pages)."

    if extension == "docx":
        from docx import Document

        uploaded_file.seek(0)
        document = Document(uploaded_file)
        paragraphs = [p.text for p in document.paragraphs if p.text.strip()]
        table_parts = []
        for table in document.tables:
            for row in table.rows:
                table_parts.append(" | ".join(cell.text.strip() for cell in row.cells))
        full_text = "\n".join(paragraphs + table_parts).strip()
        return "text", None, full_text, "DOCX document analyzed successfully."

    raise ValueError("Unsupported document type.")


def prepare_uploaded_table(df: pd.DataFrame) -> str:
    """Create a transient table so Cortex Analyst's REST session can see it."""
    if df is None or df.empty:
        raise ValueError("The uploaded spreadsheet contains no rows.")

    _drop_uploaded_table()

    work_df = _normalize_uploaded_dataframe(df)
    mapping = _safe_column_names(work_df)
    work_df.columns = [mapping[str(c)] for c in work_df.columns]

    table_name = (
        "UPLOADED_DOCUMENT_"
        + datetime.now().strftime("%Y%m%d_%H%M%S_%f").upper()
    )

    # IMPORTANT: do NOT use a TEMPORARY table here. Cortex Analyst REST runs
    # in a separate Snowflake session and cannot see session-scoped temp tables.
    # A TRANSIENT table is visible to the Analyst request and is dropped when
    # the user uploads another document or removes the current document.
    session.write_pandas(
        work_df,
        table_name,
        auto_create_table=True,
        overwrite=True,
        table_type="transient",
    )

    try:
        session.sql(
            f'ALTER TABLE "{table_name}" SET DATA_RETENTION_TIME_IN_DAYS = 0'
        ).collect()
    except Exception:
        pass

    semantic_model = build_uploaded_semantic_model(df, table_name)

    st.session_state.uploaded_document_table = table_name
    st.session_state.uploaded_document_semantic_model = semantic_model

    return semantic_model


def answer_uploaded_table_question(question: str, df: pd.DataFrame):
    """Use Cortex Analyst to generate SQL against the complete uploaded table."""
    if df is None or df.empty:
        raise ValueError("The uploaded spreadsheet has no usable rows.")

    if not st.session_state.uploaded_document_table:
        prepare_uploaded_table(df)

    table_name = st.session_state.uploaded_document_table
    semantic_model = st.session_state.uploaded_document_semantic_model

    if not table_name or not semantic_model:
        raise RuntimeError("The uploaded document semantic model was not created.")

    analyst_json = call_cortex_analyst_with_semantic_model(
        question,
        semantic_model,
    )
    result = extract_analyst_response(analyst_json)

    if result.get("warnings"):
        warning_text = " ".join(
            str(w.get("message", w)) if isinstance(w, dict) else str(w)
            for w in result["warnings"]
        )
        if warning_text:
            st.warning(warning_text)

    if not result.get("sql"):
        raise RuntimeError(
            result.get("text")
            or "Cortex Analyst could not generate SQL for the uploaded document question."
        )

    sql_query = _clean_generated_sql(result["sql"])
    result_df = session.sql(sql_query).to_pandas()

    return result_df, sql_query, result


def answer_uploaded_text_question(question: str, document_text: str):
    """Legacy PDF/DOCX path. Trial accounts without COMPLETE cannot use it."""
    if not document_text.strip():
        raise ValueError("No readable text was extracted from the uploaded document.")
    raise RuntimeError(
        "PDF/DOCX question answering currently requires a Cortex text-generation "
        "function that is not enabled for this Snowflake trial account. Excel/CSV "
        "analysis uses Cortex Analyst and does not require COMPLETE."
    )


def render_uploaded_document_preview():
    """Display the analyzed document without interfering with the original UI."""
    doc_type = st.session_state.uploaded_document_type
    doc_name = st.session_state.uploaded_document_name

    if not doc_name:
        return

    st.markdown("---")
    st.markdown(f"### 📄 Uploaded Document: `{doc_name}`")

    if doc_type == "table":
        df = st.session_state.uploaded_document_df
        if df is not None:
            st.dataframe(_normalize_uploaded_dataframe(df), use_container_width=True)
    elif doc_type == "text":
        with st.expander("📖 Extracted Document Content", expanded=False):
            st.text_area(
                "Document text",
                st.session_state.uploaded_document_text,
                height=350,
                disabled=True,
                label_visibility="collapsed",
            )


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
